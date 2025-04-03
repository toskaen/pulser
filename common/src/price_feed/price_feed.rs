// Replace the existing deribit.rs PriceFeed struct with this implementation
// or integrate it into your main price_feed/mod.rs

use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, Mutex};
use std::time::{Duration, Instant};
use reqwest::Client;
use log::{debug, info, warn, trace, error};
use std::collections::HashMap;
use crate::error::PulserError;
use crate::types::PriceInfo;
use crate::utils;

use super::{PriceHistory, DEFAULT_CACHE_DURATION_SECS, PRICE_CACHE};
use super::sources::{SourceManager, BinanceProvider, BitfinexProvider, KrakenProvider, CoinbaseProvider, DeribitProvider};
use super::aggregator::PriceAggregator;
use super::http_sources;
use super::cache::save_price_history;

#[derive(Clone)]
pub struct PriceFeed {
    // Source management
    source_manager: Arc<RwLock<SourceManager>>,
    aggregator: Arc<PriceAggregator>,
    
    // Deribit-specific state (for backward compatibility)
    pub latest_deribit_price: Arc<RwLock<f64>>,
    pub last_deribit_update: Arc<RwLock<i64>>,
    pub last_ws_activity: Arc<RwLock<Instant>>,
    pub connected: Arc<RwLock<bool>>,
    
    // HTTP client
    client: Client,
}

impl PriceFeed {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(super::DEFAULT_TIMEOUT_SECS))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
            
        let mut source_manager = SourceManager::new(client.clone());
        
        // Register price sources with weights reflecting our preference order
        source_manager.register(KrakenProvider::new().with_weight(1.2));     // Highest priority
        source_manager.register(BitfinexProvider::new().with_weight(1.1));   // Second priority
        source_manager.register(BinanceProvider::new().with_weight(1.0));    // Third priority
        source_manager.register(DeribitProvider::new().with_weight(0.8));    // Futures reference only
        
        Self {
            source_manager: Arc::new(RwLock::new(source_manager)),
            aggregator: Arc::new(PriceAggregator::new().with_min_sources(2)),
            latest_deribit_price: Arc::new(RwLock::new(0.0)),
            last_deribit_update: Arc::new(RwLock::new(0)),
            last_ws_activity: Arc::new(RwLock::new(Instant::now())),
            connected: Arc::new(RwLock::new(false)),
            client,
        }
    }
    
    // Main method to get the current price (primarily using VWAP)
    pub async fn get_price(&self) -> Result<PriceInfo, PulserError> {
        // Try to get cached price first to avoid unnecessary API calls
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
        drop(cache); // Release read lock before proceeding
        
        // Log which price sources we're about to query
        debug!("Fetching fresh prices from configured sources");
        
        // Get fresh prices from all sources
        let sources = match tokio::time::timeout(
            Duration::from_secs(5), 
            self.source_manager.read().await
        ).await {
            Ok(sources) => sources,
            Err(_) => return Err(PulserError::InternalError("Timeout acquiring source manager lock".to_string()))
        };
        
        let results = sources.fetch_all_prices().await;
        
        // Log the results for debugging
        let success_count = results.values().filter(|r| r.is_ok()).count();
        let total_count = results.len();
        debug!("Received {}/{} successful price responses", success_count, total_count);
        
        for (name, result) in &results {
            match result {
                Ok(source) => trace!("Source {}: ${:.2} (weight: {:.2})", name, source.price, source.weight),
                Err(e) => debug!("Source {} error: {}", name, e)
            }
        }
        
        // Calculate VWAP from available sources
        match self.aggregator.calculate_vwap(&results) {
            Ok(price_info) => {
                // Update cache
                let mut cache = match tokio::time::timeout(
                    Duration::from_secs(3),
                    PRICE_CACHE.write()
                ).await {
                    Ok(cache) => cache,
                    Err(_) => {
                        warn!("Timeout acquiring price cache write lock, proceeding without caching");
                        // Return the price info even if we can't update the cache
                        return Ok(price_info);
                    }
                };
                
                *cache = (price_info.raw_btc_usd, price_info.timestamp);
                drop(cache); // Release write lock
                
                // For backward compatibility with Deribit-dependent code
                if let Ok(mut deribit_price) = tokio::time::timeout(
                    Duration::from_secs(1),
                    self.latest_deribit_price.write()
                ).await {
                    if let Some(p) = price_info.price_feeds.get("Deribit") {
                        *deribit_price = *p;
                        
                        // Also update the timestamp
                        if let Ok(mut timestamp) = self.last_deribit_update.write().await {
                            *timestamp = now;
                        }
                    }
                }
                
                // Log the result
                info!("Calculated VWAP price: ${:.2} from {} sources", 
                      price_info.raw_btc_usd, 
                      price_info.price_feeds.len());
                
                // Save to history
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
            },
            Err(e) => {
                // First fallback: Try to use the most recent Deribit price
                let deribit_price = match tokio::time::timeout(
                    Duration::from_secs(1),
                    self.latest_deribit_price.read()
                ).await {
                    Ok(price) => *price,
                    Err(_) => 0.0
                };
                
                let last_update = match tokio::time::timeout(
                    Duration::from_secs(1),
                    self.last_deribit_update.read()
                ).await {
                    Ok(ts) => *ts,
                    Err(_) => 0
                };
                
                if deribit_price > 0.0 && (now - last_update) < 120 {
                    warn!("VWAP calculation failed: {}. Using Deribit fallback: ${:.2}", e, deribit_price);
                    
                    let mut price_feeds = HashMap::new();
                    price_feeds.insert("Deribit_Fallback".to_string(), deribit_price);
                    
                    return Ok(PriceInfo {
                        raw_btc_usd: deribit_price,
                        timestamp: now,
                        price_feeds,
                    });
                }
                
                // Second fallback: Try direct HTTP requests to emergency sources
                warn!("VWAP failed and no recent Deribit price available. Trying emergency sources.");
                http_sources::emergency_fetch_price(&self.client).await
            }
        }
    }
    
    // For backward compatibility - continue using Deribit specific methods
    pub async fn is_websocket_connected(&self) -> bool {
        *self.connected.read().await
    }
    
    pub async fn get_last_update_timestamp(&self) -> i64 {
        *self.last_deribit_update.read().await
    }
    
    // Implementation for Deribit WebSocket (keep for backward compatibility)
    pub async fn start_deribit_feed(&self, shutdown_rx: &mut broadcast::Receiver<()>) -> Result<(), PulserError> {
        // Same implementation as in paste-2.txt...
        let config: toml::Value = match std::fs::read_to_string("config/service_config.toml") {
            Ok(content) => toml::from_str(&content)?,
            Err(e) => {
                warn!("Failed to read config file: {}", e);
                return Err(PulserError::ConfigError(format!("Failed to read config: {}", e)));
            }
        };
        
        let api_key = config.get("deribit_id").and_then(|v| v.as_str()).unwrap_or("your_deribit_id").to_string();
        let secret = config.get("deribit_secret").and_then(|v| v.as_str()).unwrap_or("your_deribit_secret").to_string();

        let mut attempts = 0u32;
        let mut ping_interval = tokio::time::interval(Duration::from_secs(WS_PING_INTERVAL_SECS));
        let max_backoff = 60;
        
        // Buffer for batching price updates
        let price_buffer = Arc::new(Mutex::new(Vec::<PriceHistory>::new()));
        let price_buffer_clone = price_buffer.clone();

        // Initial state
        {
            let mut connected = self.connected.write().await;
            *connected = false;
        }

        // Spawn a task to periodically flush the buffer
        tokio::spawn({
            let buffer = price_buffer_clone;
            let mut flush_interval = tokio::time::interval(Duration::from_secs(30)); // Flush every 30s
            async move {
                loop {
                    flush_interval.tick().await;
                    let entries = {
                        let mut buf = buffer.lock().unwrap();
                        if buf.is_empty() { continue; }
                        std::mem::take(&mut *buf)
                    };
                    trace!("Saved {} batched Deribit prices", entries.len()); // Moved before save
                    if let Err(e) = save_price_history(entries).await {
                        warn!("Failed to save batched Deribit price history: {}", e);
                    }
                }
            }
        });
        
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Deribit feed shutting down");
                let mut ws_guard = self.active_ws.lock().await;
                if let Some(mut ws) = ws_guard.take() {
                    info!("Closing Deribit WebSocket connection");
                    {
                        let mut connected = self.connected.write().await;
                        *connected = false;
                    }
                    match tokio::time::timeout(Duration::from_secs(5), ws.close(None)).await {
                        Ok(result) => { if let Err(e) = result { warn!("Error closing Deribit WebSocket: {}", e); } },
                        Err(_) => { warn!("Timeout closing Deribit WebSocket, abandoning connection"); }
                    }
                }
                // Flush remaining buffer on shutdown
                let remaining = {
                    let mut buf = price_buffer.lock().unwrap(); // Changed from .lock().await
                    std::mem::take(&mut *buf)
                };
                if !remaining.is_empty() {
                    if let Err(e) = save_price_history(remaining).await {
                        warn!("Failed to save final Deribit price history: {}", e);
                    }
                }
                break;
            }
            
            _ = ping_interval.tick() => {
                let is_connected = {
                    let connected = self.connected.read().await;
                    *connected
                };
                
                if is_connected {
                    let mut ws_guard = self.active_ws.lock().await;
                    if let Some(ws) = ws_guard.as_mut() {
                        match ws.send(Message::Ping(vec![1, 2, 3, 4])).await {
                            Ok(_) => { 
                                let mut last_activity = self.last_ws_activity.write().await;
                                *last_activity = Instant::now(); 
                                trace!("Sent WebSocket ping to Deribit");
                            },
                            Err(e) => {
                                warn!("Failed to send ping to Deribit: {}", e);
                                *ws_guard = None;
                                {
                                    let mut is_connecting = self.is_connecting.write().await;
                                    *is_connecting = false;
                                }
                                {
                                    let mut connected = self.connected.write().await;
                                    *connected = false;
                                }
                            }
                        }
                    }
                }
                
                let now = Instant::now();
                let last_activity = {
                    let last_ws = self.last_ws_activity.read().await;
                    *last_ws
                };
                
                if is_connected && now.duration_since(last_activity).as_secs() > WS_PING_INTERVAL_SECS * 2 {
                    warn!("Deribit WebSocket connection stale, closing connection");
                    let mut ws_guard = self.active_ws.lock().await;
                    if let Some(mut ws) = ws_guard.take() {
                        let _ = ws.close(None).await;
                    }
                    {
                        let mut connected = self.connected.write().await;
                        *connected = false;
                    }
                    {
                        let mut is_connecting = self.is_connecting.write().await;
                        *is_connecting = false;
                    }
                }
            }
            
            _ = async {
                let should_reconnect = {
                    let is_connecting = self.is_connecting.read().await;
                    let connected = self.connected.read().await;
                    let has_active_ws = self.active_ws.lock().await.is_some();
                    !*is_connecting && !*connected && !has_active_ws
                };
                
                if should_reconnect {
                    {
                        let mut is_connecting = self.is_connecting.write().await;
                        *is_connecting = true;
                    }
                    
                    let backoff = std::cmp::min(2u64.pow(attempts), max_backoff);
                    let jitter = rand::random::<u64>() % 1000;
                    let backoff_ms = backoff * 1000 + jitter;
                    
                    if attempts > 0 {
                        info!("Waiting {}ms before reconnecting to Deribit", backoff_ms);
                        sleep(Duration::from_millis(backoff_ms)).await;
                    }
                    
                    info!("Connecting to Deribit (attempt {})", attempts + 1);
                    match self.connect_deribit(&api_key, &secret).await {
                        Ok(_) => {
                            info!("Successfully connected to Deribit");
                            attempts = 0;
                            {
                                let mut connected = self.connected.write().await;
                                *connected = true;
                            }
                            let buffer_clone = price_buffer.clone();
                            let ws_clone = self.active_ws.clone();
                            let price_clone = self.latest_deribit_price.clone();
                            let update_clone = self.last_deribit_update.clone();
                            let activity_clone = self.last_ws_activity.clone();
                            let last_update_clone = self.last_price_update.clone();
                            let connected_clone = self.connected.clone();

                            tokio::spawn(async move {
                                let mut ws_lock = ws_clone.lock().await;
                                if let Some(mut ws) = ws_lock.take() {
                                    while let Some(msg_result) = ws.next().await {
                                        match msg_result {
                                            Ok(Message::Text(text)) => {
                                                if let Ok(data) = serde_json::from_str::<Value>(&text) {
                                                    if let Some(price) = data.get("params")
                                                        .and_then(|p| p.get("data"))
                                                        .and_then(|d| d.get("last_price"))
                                                        .and_then(|p| p.as_f64())
                                                    {
                                                        let now = Instant::now();
                                                        let last_update = {
                                                            let last = last_update_clone.read().await;
                                                            *last
                                                        };
                                                        if now.duration_since(last_update).as_millis() >= PRICE_UPDATE_INTERVAL_MS as u128 {
                                                            {
                                                                let mut price_guard = price_clone.write().await;
                                                                *price_guard = price;
                                                            }
                                                            {
                                                                let mut time_guard = update_clone.write().await;
                                                                *time_guard = now_timestamp();
                                                            }
                                                            {
                                                                let mut update_guard = last_update_clone.write().await;
                                                                *update_guard = now;
                                                            }
                                                            {
                                                                let mut buffer = buffer_clone.lock().unwrap(); 
                                                                buffer.push(PriceHistory {
                                                                    timestamp: now_timestamp() as u64,
                                                                    btc_usd: price,
                                                                    source: "Deribit".to_string(),
                                                                });
                                                            }
                                                            trace!("Deribit BTC/USD: ${:.2} (throttled)", price);
                                                        }
                                                    }
                                                }
                                            }
                                            Ok(Message::Ping(data)) => {
                                                if let Err(e) = ws.send(Message::Pong(data)).await {
                                                    warn!("Failed to send pong: {}", e);
                                                    break;
                                                }
                                                let mut last_activity = activity_clone.write().await;
                                                *last_activity = Instant::now();
                                            }
                                            Ok(Message::Close(_)) => {
                                                debug!("WebSocket close frame received");
                                                break;
                                            }
                                            Err(e) => {
                                                warn!("WebSocket read error: {}", e);
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                    let mut connected = connected_clone.write().await;
                                    *connected = false;
                                    *ws_lock = None;
                                }
                            });
                        },
                        Err(e) => {
                            warn!("Deribit WebSocket connection failed: {}", e);
                            let mut ws_guard = self.active_ws.lock().await;
                            if let Some(mut ws) = ws_guard.take() {
                                let _ = ws.close(None).await;
                            }
                            attempts = attempts.saturating_add(1);
                            {
                                let mut connected = self.connected.write().await;
                                *connected = false;
                            }
                            if attempts >= 10 {
                                error!("Max reconnection attempts reached for Deribit");
                                sleep(Duration::from_secs(60)).await;
                                attempts = 5;
                            }
                        }
                    }
                    
                    {
                        let mut is_connecting = self.is_connecting.write().await;
                        *is_connecting = false;
                    }
                }
                
                sleep(Duration::from_millis(1000)).await;
            } => {}
        }
    }
    Ok(())
}
    
// For backward compatibility with Deribit-specific code
    pub async fn get_deribit_price(&self) -> Result<f64, PulserError> {
        // First try getting the VWAP price - this is the preferred method now
        match self.get_price().await {
            Ok(price_info) => {
                // Check if we have a Deribit price directly in the feeds
                if let Some(deribit_price) = price_info.price_feeds.get("Deribit") {
                    return Ok(*deribit_price);
                }
                
                // If not, we can still return the VWAP as it's typically a better price reference
                // But we should log that we're using a VWAP instead of Deribit specific
                trace!("No specific Deribit price available, using VWAP: ${:.2}", price_info.raw_btc_usd);
                return Ok(price_info.raw_btc_usd);
            },
            Err(e) => {
                // Log but continue to try direct Deribit access
                debug!("Failed to get VWAP price: {}, trying direct Deribit access", e);
            }
        }
        
        // Try using cached Deribit price if available and fresh
        let deribit_price = *self.latest_deribit_price.read().await;
        let last_update = *self.last_deribit_update.read().await;
        let now = crate::utils::now_timestamp();
        
        // Return cached price if it's less than 60 seconds old
        if deribit_price > 0.0 && (now - last_update) < 60 {
            trace!("Using cached Deribit price: ${:.2}", deribit_price);
            return Ok(deribit_price);
        }

        // As a last resort, make a direct HTTP request to Deribit
        debug!("Cached Deribit price too old or unavailable, fetching fresh price");
        self.fetch_deribit_price().await
    }
    
    // Direct HTTP fetch from Deribit (implementation method)
    // This should be considered an internal method, not part of the public API
    async fn fetch_deribit_price(&self) -> Result<f64, PulserError> {
        let url = "https://test.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL";
        
        match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), self.client.get(url).send()).await {
            Ok(Ok(response)) => {
                if !response.status().is_success() {
                    return Err(PulserError::ApiError(format!("Deribit API error: {}", response.status())));
                }
                
                match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), response.json::<Value>()).await {
                    Ok(Ok(json)) => {
                        match json["result"]["last_price"].as_f64() {
                            Some(price) if price > 0.0 => {
                                // Update the cached price
                                {
                                    let mut price_guard = self.latest_deribit_price.write().await;
                                    *price_guard = price;
                                }
                                {
                                    let mut time_guard = self.last_deribit_update.write().await;
                                    *time_guard = crate::utils::now_timestamp();
                                }
                                
                                debug!("Updated Deribit price via direct HTTP: ${:.2}", price);
                                Ok(price)
                            },
                            Some(price) => Err(PulserError::PriceFeedError(format!("Invalid Deribit price: ${:.2}", price))),
                            None => Err(PulserError::PriceFeedError("Missing price in Deribit response".to_string())),
                        }
                    }
                    Ok(Err(e)) => Err(PulserError::ApiError(format!("Failed to parse Deribit response: {}", e))),
                    Err(_) => Err(PulserError::ApiError("Timeout parsing Deribit response".to_string())),
                }
            }
            Ok(Err(e)) => Err(PulserError::NetworkError(format!("Deribit request failed: {}", e))),
            Err(_) => Err(PulserError::NetworkError("Deribit request timed out".to_string())),
        }
    }
    
    // Option price lookup forwarded to hedging module
    pub async fn get_option_price(&self, strike: f64, option_type: &str) -> Result<f64, PulserError> {
        crate::price_feed::hedging::options::get_option_price(&self.client, strike, option_type).await
    }

    // Combo price lookup forwarded to hedging module
    pub async fn get_combo_price(&self, leg1: &str, leg2: &str) -> Result<f64, PulserError> {
        crate::price_feed::hedging::futures::get_combo_price(&self.client, leg1, leg2).await
    }
    
    // New method to get option Greeks (delta, gamma, vega, theta)
    pub async fn get_option_greeks(&self, strike: f64, option_type: &str) -> Result<(f64, f64, f64, f64), PulserError> {
        crate::price_feed::hedging::options::get_option_greeks(&self.client, strike, option_type).await
    }
    
    // New method to get funding rate for perpetual futures
    pub async fn get_funding_rate(&self, instrument: &str) -> Result<f64, PulserError> {
        crate::price_feed::hedging::futures::get_funding_rate(&self.client, instrument).await
    }
