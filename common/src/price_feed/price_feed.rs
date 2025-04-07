use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, Mutex};
use std::time::{Duration, Instant};
use reqwest::Client;
use log::{debug, info, warn, trace};
use std::collections::HashMap;
use crate::error::PulserError;
use crate::types::PriceInfo;
use crate::utils;
use crate::websocket::WebSocketManager;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use futures::SinkExt;
use futures::StreamExt;
use tokio::time::sleep;
use std::fmt;


use super::{PriceHistory, DEFAULT_CACHE_DURATION_SECS, PRICE_CACHE, WS_PING_INTERVAL_SECS, PRICE_UPDATE_INTERVAL_MS};
use super::sources::{SourceManager, BinanceProvider, BitfinexProvider, KrakenProvider, DeribitProvider};
use super::aggregator::PriceAggregator;
use super::http_sources;
use super::cache::save_price_history;

#[derive(Clone)]
pub struct PriceFeed {
    source_manager: Arc<RwLock<SourceManager>>,
    aggregator: Arc<PriceAggregator>,
    latest_deribit_price: Arc<RwLock<f64>>,
    latest_kraken_futures_price: Arc<RwLock<f64>>, // Added for Kraken Futures
        latest_binance_futures_price: Arc<RwLock<f64>>, // New field
    last_deribit_update: Arc<RwLock<i64>>,
    client: Client,
    ws_manager: Arc<WebSocketManager>, // Added for WebSocket management
}

impl fmt::Debug for PriceFeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PriceFeed")
            .field("latest_deribit_price", &self.latest_deribit_price)
            .field("latest_kraken_futures_price", &self.latest_kraken_futures_price)
            .field("last_deribit_update", &self.last_deribit_update)
            // Skip fields that don't implement Debug
            .finish_non_exhaustive()
    }
}

impl PriceFeed {
pub fn new() -> Self {
    let client = Client::builder()
        .timeout(Duration::from_secs(super::DEFAULT_TIMEOUT_SECS))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_else(|_| Client::new());

    let mut source_manager = SourceManager::new(client.clone());
    source_manager.register(KrakenProvider::new().with_weight(1.2));
    source_manager.register(BitfinexProvider::new().with_weight(1.1));
    source_manager.register(BinanceProvider::new().with_weight(1.0));
    source_manager.register(DeribitProvider::new().with_weight(0.8));

    let ws_config = crate::websocket::WebSocketConfig {
        ping_interval_secs: WS_PING_INTERVAL_SECS,
        timeout_secs: 30,
        max_reconnect_attempts: 10,
        reconnect_base_delay_secs: 1,
    };
    let ws_manager = Arc::new(WebSocketManager::new(ws_config));

    Self {
        source_manager: Arc::new(RwLock::new(source_manager)),
        aggregator: Arc::new(PriceAggregator::new().with_min_sources(2)),
        latest_deribit_price: Arc::new(RwLock::new(0.0)),
        latest_kraken_futures_price: Arc::new(RwLock::new(0.0)),
        latest_binance_futures_price: Arc::new(RwLock::new(0.0)), // Initialize the new field
        last_deribit_update: Arc::new(RwLock::new(0)),
        client,
        ws_manager,
    }
}
    
    pub fn get_websocket_manager(&self) -> Arc<WebSocketManager> {
        self.ws_manager.clone()
    }

    pub async fn get_price(&self) -> Result<PriceInfo, PulserError> {
let cache_ttl = Duration::from_secs(DEFAULT_CACHE_DURATION_SECS);
let now = utils::now_timestamp();
// Fix - store the read lock in a variable
let cache = PRICE_CACHE.read().await;
let cached_price = if cache.0 > 0.0 && (now - cache.1) < cache_ttl.as_secs() as i64 {
    Some((cache.0, cache.1))
} else {
    None
};
drop(cache); // Now cache is in scope

if let Some((price, timestamp)) = cached_price {
    let mut feeds = HashMap::new();
    feeds.insert("cached".to_string(), price);
    return Ok(PriceInfo {
        raw_btc_usd: price,
        timestamp,
        price_feeds: feeds,
    });
}


        debug!("Fetching fresh prices from configured sources");
        let sources = tokio::time::timeout(Duration::from_secs(5), self.source_manager.read()).await
            .map_err(|_| PulserError::InternalError("Timeout acquiring source manager lock".to_string()))?;
        let results = sources.fetch_all_prices().await;

        debug!("Received {}/{} successful price responses", 
            results.values().filter(|r| r.is_ok()).count(), 
            results.len());
        for (name, result) in &results {
            match result {
                Ok(source) => trace!("Source {}: ${:.2} (weight: {:.2})", name, source.price, source.weight),
                Err(e) => debug!("Source {} error: {}", name, e),
            }
        }

let price_info = self.aggregator.as_ref().calculate_quality_adjusted_vwap(&results)?;
        let mut cache = PRICE_CACHE.write().await;
        *cache = (price_info.raw_btc_usd, price_info.timestamp);
        info!("Calculated VWAP price: ${:.2} from {} sources", 
            price_info.raw_btc_usd, price_info.price_feeds.len());

        if let Some(p) = price_info.price_feeds.get("Deribit") {
            let mut deribit_price = self.latest_deribit_price.write().await;
            *deribit_price = *p;
            let mut timestamp = self.last_deribit_update.write().await;
            *timestamp = now;
        }

        tokio::spawn(async move {
            let history = vec![PriceHistory {
                timestamp: now as u64,
                btc_usd: price_info.raw_btc_usd,
                source: "VWAP".to_string(),
            }];
            if let Err(e) = save_price_history(history).await {
                warn!("Failed to save price history: {}", e);
            }
        });

        Ok(price_info)
    }

pub async fn start_futures_feed(&self, shutdown_rx: &mut broadcast::Receiver<()>) -> Result<(), PulserError> {
    // Connect to Deribit via the WebSocketManager
    let max_reconnect_attempts = 5;
    let base_delay_secs = 2;
    let mut attempt = 0;

    // Create the subscription message
    let subscription = json!({
        "method": "public/subscribe",
        "params": {
            "channels": ["ticker.BTC-PERPETUAL.raw"]
        },
        "id": 1
    }).to_string();
    
    // Get the endpoint URL
    let endpoint = "wss://test.deribit.com/ws/api/v2";
    
    // Try to connect with backoff
    loop {
        match self.ws_manager.subscribe(endpoint, &subscription, PRICE_UPDATE_INTERVAL_MS).await {
            Ok(_) => {
                info!("Successfully connected to Deribit WebSocket");
                break;
            }
            Err(e) => {
                attempt += 1;
                if attempt >= max_reconnect_attempts {
                    return Err(PulserError::NetworkError(
                        format!("Failed to connect to Deribit after {} attempts: {}", 
                        max_reconnect_attempts, e)
                    ));
                }
                
                // Exponential backoff
                let delay = base_delay_secs * (1 << attempt);
                warn!("Deribit connection failed (attempt {}/{}): {}. Retrying in {}s", 
                     attempt, max_reconnect_attempts, e, delay);
                sleep(Duration::from_secs(delay)).await;
            }
        }
    }
    
    // Set up price handling
    let price_clone = self.latest_deribit_price.clone();
    let update_clone = self.last_deribit_update.clone();
    let price_buffer = Arc::new(Mutex::new(Vec::<PriceHistory>::new()));
    let price_buffer_clone = price_buffer.clone();
    
    self.ws_manager.process_messages(endpoint, move |json| {
        let price_clone = price_clone.clone();
        let update_clone = update_clone.clone();
        let price_buffer = price_buffer.clone();
        
        async move {
            if let Some(price) = json.get("params")
                .and_then(|p| p.get("data"))
                .and_then(|d| d.get("last_price"))
                .and_then(|p| p.as_f64())
            {
                let mut price_guard = price_clone.write().await;
                *price_guard = price;
                
                let mut time_guard = update_clone.write().await;
                *time_guard = utils::now_timestamp();
                
                let mut buffer = price_buffer.lock().await;
                buffer.push(PriceHistory {
                    timestamp: utils::now_timestamp() as u64,
                    btc_usd: price,
                    source: "Deribit".to_string(),
                });
                
                trace!("Deribit BTC/USD: ${:.2}", price);
            }
            Ok(())
        }
    }).await?;
    
    // Buffer flush task
    tokio::spawn({
        let buffer = price_buffer_clone;
        let mut flush_interval = tokio::time::interval(Duration::from_secs(30));
        async move {
            loop {
                flush_interval.tick().await;
                let entries = {
                    let mut buf = buffer.lock().await;
                    if buf.is_empty() { continue; }
                    std::mem::take(&mut *buf)
                };
                trace!("Saved {} batched Deribit prices", entries.len());
                if let Err(e) = save_price_history(entries).await {
                    warn!("Failed to save batched price history: {}", e);
                }
            }
        }
    });
    
    // Clone the WS manager to avoid the 'self' lifetime issue
    let ws_manager_clone = self.ws_manager.clone();
    
    // Start monitoring WebSocket connection
    let shutdown_clone = shutdown_rx.resubscribe();
    tokio::spawn(async move {
        ws_manager_clone.start_monitoring(shutdown_clone).await
    });
    
    // Add Kraken Futures connection
    let kraken_endpoint = "wss://futures.kraken.com/ws/v1";
    let kraken_price = self.latest_kraken_futures_price.clone();
    let kraken_sub = json!({"event": "subscribe", "feed": "ticker", "product_ids": ["PI_XBTUSD"]}).to_string();
    
    match self.ws_manager.subscribe(kraken_endpoint, &kraken_sub, PRICE_UPDATE_INTERVAL_MS).await {
        Ok(_) => {
            info!("Successfully connected to Kraken Futures WebSocket");
            
            self.ws_manager.process_messages(kraken_endpoint, move |json| {
                let kraken_price = kraken_price.clone();
                
                async move {
                    if let Some(price) = json.get("last").and_then(|p| p.as_f64()) {
                        let mut price_guard = kraken_price.write().await;
                        *price_guard = price;
                        trace!("Kraken futures: ${:.2}", price);
                    }
                    Ok(())
                }
            }).await?;
        },
        Err(e) => {
            warn!("Failed to connect to Kraken Futures: {}", e);
            // Continue anyway, as Deribit is our primary source
        }
    }
    
    // Add Binance Futures connection
    let binance_endpoint = "wss://fstream.binance.com/ws/btcusdt@ticker";
    let binance_price = self.latest_binance_futures_price.clone();

    match self.ws_manager.subscribe(binance_endpoint, "", PRICE_UPDATE_INTERVAL_MS).await {
        Ok(_) => {
            info!("Successfully connected to Binance Futures WebSocket");
            
            self.ws_manager.process_messages(binance_endpoint, move |json| {
                let binance_price = binance_price.clone();
                
                async move {
                    if let Some(price) = json.get("c") // "c" is the last price in Binance's ticker stream
                        .and_then(|p| p.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                    {
                        let mut price_guard = binance_price.write().await;
                        *price_guard = price;
                        trace!("Binance Futures BTC/USD: ${:.2}", price);
                    }
                    Ok(())
                }
            }).await?;
        },
        Err(e) => {
            warn!("Failed to connect to Binance Futures: {}", e);
            // Continue anyway, treating Binance as optional
        }
    }

    Ok(())
}

pub async fn get_best_futures_price(&self) -> Result<(String, f64), PulserError> {
    let deribit_price = *self.latest_deribit_price.read().await;
    let kraken_price = *self.latest_kraken_futures_price.read().await;
    let binance_price = *self.latest_binance_futures_price.read().await;
    let last_update = *self.last_deribit_update.read().await;
    let now = utils::now_timestamp();

    if now - last_update > 60 {
        warn!("Futures prices stale, last update: {}", last_update);
        return Err(PulserError::PriceFeedError("Futures prices outdated".to_string()));
    }

    // Collect all valid prices
    let mut valid_prices = Vec::new();
    if deribit_price > 0.0 {
        valid_prices.push(("Deribit".to_string(), deribit_price));
    }
    if kraken_price > 0.0 {
        valid_prices.push(("Kraken".to_string(), kraken_price));
    }
    if binance_price > 0.0 {
        valid_prices.push(("Binance".to_string(), binance_price));
    }

    if valid_prices.is_empty() {
        return Err(PulserError::PriceFeedError("No valid futures prices".to_string()));
    }

    // For hedging shorts, find the lowest price
    valid_prices.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    Ok(valid_prices[0].clone())
}

pub async fn get_all_futures_prices(&self) -> HashMap<String, f64> {
    let mut prices = HashMap::new();
    let deribit_price = *self.latest_deribit_price.read().await;
    if deribit_price > 0.0 {
        prices.insert("Deribit".to_string(), deribit_price);
    }
    
    let kraken_price = *self.latest_kraken_futures_price.read().await;
    if kraken_price > 0.0 {
        prices.insert("Kraken".to_string(), kraken_price);
    }
    
    let binance_price = *self.latest_binance_futures_price.read().await;
    if binance_price > 0.0 {
        prices.insert("Binance".to_string(), binance_price);
    }
    
    prices
}

    pub async fn get_deribit_price(&self) -> Result<f64, PulserError> {
        let deribit_price = *self.latest_deribit_price.read().await;
        let last_update = *self.last_deribit_update.read().await;
        let now = utils::now_timestamp();

        if deribit_price > 0.0 && (now - last_update) < 60 {
            trace!("Using cached Deribit price: ${:.2}", deribit_price);
            Ok(deribit_price)
        } else {
            self.fetch_deribit_price().await
        }
    }

    async fn fetch_deribit_price(&self) -> Result<f64, PulserError> {
        let url = "https://test.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL";
        let response = tokio::time::timeout(Duration::from_secs(super::DEFAULT_TIMEOUT_SECS), self.client.get(url).send()).await??;
        if !response.status().is_success() {
            return Err(PulserError::ApiError(format!("Deribit API error: {}", response.status())));
        }
        let json: Value = response.json().await?;
        let price = json["result"]["last_price"].as_f64()
            .ok_or_else(|| PulserError::PriceFeedError("Missing price in Deribit response".to_string()))?;
        
        let mut price_guard = self.latest_deribit_price.write().await;
        *price_guard = price;
        let mut time_guard = self.last_deribit_update.write().await;
        *time_guard = utils::now_timestamp();
        debug!("Updated Deribit price via HTTP: ${:.2}", price);
        Ok(price)
    }

    pub async fn get_option_price(&self, strike: f64, option_type: &str) -> Result<f64, PulserError> {
        crate::price_feed::hedging::options::get_option_price(&self.client, strike, option_type).await
    }

    pub async fn get_combo_price(&self, leg1: &str, leg2: &str) -> Result<f64, PulserError> {
        crate::price_feed::hedging::futures::get_combo_price(&self.client, leg1, leg2).await
    }

    pub async fn get_option_greeks(&self, strike: f64, option_type: &str) -> Result<(f64, f64, f64, f64), PulserError> {
        crate::price_feed::hedging::options::get_option_greeks(&self.client, strike, option_type).await
    }

    pub async fn get_funding_rate(&self, instrument: &str) -> Result<f64, PulserError> {
        crate::price_feed::hedging::futures::get_funding_rate(&self.client, instrument).await
    }
    
pub async fn is_websocket_connected(&self) -> bool {
    // We consider it connected if at least Deribit (our primary source) is connected
    self.ws_manager.is_connected("wss://test.deribit.com/ws/api/v2").await
    
    // Alternative: check if ANY of the connections are active
    // let deribit = self.ws_manager.is_connected("wss://test.deribit.com/ws/api/v2").await;
    // let kraken = self.ws_manager.is_connected("wss://futures.kraken.com/ws/v1").await;
    // let binance = self.ws_manager.is_connected("wss://fstream.binance.com/ws/btcusdt@ticker").await;
    // deribit || kraken || binance
}

// In common/src/price_feed/price_feed.rs - Improve the shutdown_websocket method
pub async fn shutdown_websocket(&self) -> Option<Result<(), PulserError>> {
    info!("Closing all WebSocket connections...");
    
    let futures = vec![
        self.send_unsubscribe_message("wss://test.deribit.com/ws/api/v2"),
        self.send_unsubscribe_message("wss://futures.kraken.com/ws/v1"),
        self.send_unsubscribe_message("wss://fstream.binance.com/ws/btcusdt@ticker"),
    ];
    
    // Send all unsubscribe messages in parallel with a short timeout
    let _ = tokio::time::timeout(Duration::from_millis(500), futures::future::join_all(futures)).await;
    
    // Short pause for messages to be processed
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Now close connections with a short timeout for each
    self.ws_manager.force_close_all_with_timeout(Duration::from_secs(2)).await;
    
    Some(Ok(()))
}

// Add this new helper method to price_feed.rs
async fn send_unsubscribe_message(&self, endpoint: &str) -> Result<(), PulserError> {
    if self.ws_manager.is_connected(endpoint).await {
        let unsubscribe_msg = match endpoint {
            "wss://test.deribit.com/ws/api/v2" => r#"{"method":"public/unsubscribe_all","params":{},"id":1}"#,
            "wss://futures.kraken.com/ws/v1" => r#"{"event":"unsubscribe","feed":"ticker"}"#,
            "wss://fstream.binance.com/ws/btcusdt@ticker" => "", // Binance doesn't need explicit unsubscribe
            _ => ""
        };
        
        if !unsubscribe_msg.is_empty() {
            match tokio::time::timeout(
                Duration::from_millis(300),
                self.ws_manager.send_message(endpoint, unsubscribe_msg)
            ).await {
                Ok(Ok(_)) => debug!("Sent unsubscribe to {}", endpoint),
                _ => debug!("Failed to send unsubscribe to {}", endpoint),
            }
        }
    }
    
    Ok(())
}

}

