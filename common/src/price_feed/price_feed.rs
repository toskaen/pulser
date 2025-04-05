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
    last_deribit_update: Arc<RwLock<i64>>,
    client: Client,
    ws_manager: Arc<WebSocketManager>, // Added for WebSocket management
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
            last_deribit_update: Arc::new(RwLock::new(0)),
            client,
            ws_manager,
        }
    }

    pub async fn get_price(&self) -> Result<PriceInfo, PulserError> {
        let now = utils::now_timestamp();
        let cache = PRICE_CACHE.read().await;
        if cache.0 > 0.0 && (now - cache.1) < DEFAULT_CACHE_DURATION_SECS as i64 {
            trace!("Using cached price: ${:.2}", cache.0);
            let mut price_feeds = HashMap::new();
            price_feeds.insert("cache".to_string(), cache.0);
            return Ok(PriceInfo {
                raw_btc_usd: cache.0,
                timestamp: cache.1,
                price_feeds,
            });
        }
        drop(cache);

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

        let price_info = self.aggregator.calculate_vwap(&results)?;
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

    pub async fn start_deribit_feed(&self, shutdown_rx: &mut broadcast::Receiver<()>) -> Result<(), PulserError> {
        let ws_manager = self.ws_manager.clone();
        let price_clone = self.latest_deribit_price.clone();
        let update_clone = self.last_deribit_update.clone();
        let price_buffer = Arc::new(Mutex::new(Vec::<PriceHistory>::new()));
        let price_buffer_clone = price_buffer.clone();

        let subscription = json!({"method": "public/subscribe", "params": {"channels": ["ticker.BTC-PERPETUAL.raw"]}}).to_string();
        ws_manager.subscribe("wss://test.deribit.com/ws/api/v2", &subscription, PRICE_UPDATE_INTERVAL_MS).await?;
ws_manager.process_messages("wss://test.deribit.com/ws/api/v2", move |json| async move {

            if let Some(price) = json.get("params").and_then(|p| p.get("data")).and_then(|d| d.get("last_price")).and_then(|p| p.as_f64()) {
                let mut price_guard = price_clone.blocking_write();
                *price_guard = price;
                let mut time_guard = update_clone.blocking_write();
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
        }).await?;

        // Kraken Futures
        let kraken_price = self.latest_kraken_futures_price.clone();
        let kraken_sub = json!({"event": "subscribe", "feed": "ticker", "product_ids": ["PI_XBTUSD"]}).to_string();
        ws_manager.subscribe("wss://futures.kraken.com/ws/v1", &kraken_sub, PRICE_UPDATE_INTERVAL_MS).await?;
        ws_manager.process_messages("wss://futures.kraken.com/ws/v1", move |json| {
            if let Some(price) = json.get("last").and_then(|p| p.as_f64()) {
                let mut price_guard = kraken_price.blocking_write();
                *price_guard = price;
                trace!("Kraken futures: ${:.2}", price);
            }
            Ok(())
        }).await?;

        // Buffer flush task (kept from original)
        tokio::spawn({
            let buffer = price_buffer_clone;
            let mut flush_interval = tokio::time::interval(Duration::from_secs(30));
            async move {
                loop {
                    flush_interval.tick().await;
                    let entries = {
let mut buf = buffer.lock().await;           // Line 168
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

        tokio::spawn(async move {
ws_manager.start_monitoring(shutdown_rx.resubscribe()).await
        });
        Ok(())
    }

    pub async fn get_best_futures_price(&self) -> Result<(String, f64), PulserError> {
        let deribit_price = *self.latest_deribit_price.read().await;
        let kraken_price = *self.latest_kraken_futures_price.read().await;
        let last_update = *self.last_deribit_update.read().await;
        let now = utils::now_timestamp();

        if now - last_update > 60 {
            warn!("Futures prices stale, last update: {}", last_update);
            return Err(PulserError::PriceFeedError("Futures prices outdated".to_string()));
        }

        match (deribit_price > 0.0, kraken_price > 0.0) {
            (true, true) => Ok(if deribit_price < kraken_price {
                ("Deribit".to_string(), deribit_price)
            } else {
                ("Kraken".to_string(), kraken_price)
            }),
            (true, false) => Ok(("Deribit".to_string(), deribit_price)),
            (false, true) => Ok(("Kraken".to_string(), kraken_price)),
            (false, false) => Err(PulserError::PriceFeedError("No valid futures prices".to_string())),
        }
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
    self.ws_manager.is_connected("wss://test.deribit.com/ws/api/v2").await
}

pub async fn shutdown_websocket(&self) -> Option<Result<(), PulserError>> {
    self.ws_manager.shutdown_connection("wss://test.deribit.com/ws/api/v2").await
}
}
