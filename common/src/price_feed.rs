use reqwest::Client;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use log::{info, trace, warn, debug, error};
use serde::{Serialize, Deserialize};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use crate::error::PulserError;
use crate::types::PriceInfo;
use crate::utils::now_timestamp;
use tokio::sync::{mpsc, broadcast};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio::sync::Mutex as TokioMutex;
use std::future::Future;
use std::sync::Mutex;



// Constants
pub const DEFAULT_CACHE_DURATION_SECS: u64 = 120; // 2 minutes
pub const DEFAULT_RETRY_MAX: u32 = 3;
pub const DEFAULT_MAX_RETRY_TIME_SECS: u64 = 120;
pub const DEFAULT_TIMEOUT_SECS: u64 = 10;
pub const WS_PING_INTERVAL_SECS: u64 = 30;         // Interval to ping WebSocket
pub const WS_CONNECTION_TIMEOUT_SECS: u64 = 30;    // WebSocket connection timeout
pub const MAX_HISTORY_ENTRIES: usize = 1440;       // Approx. 24 hours at 1 entry per minute
pub const FALLBACK_RETRY_ATTEMPTS: u32 = 2;        // Retry attempts for each fallback source
pub const PRICE_UPDATE_INTERVAL_MS: u64 = 1000;    // Rate limit: update price max once per second

lazy_static::lazy_static! {
    static ref PRICE_CACHE: Arc<RwLock<(f64, i64)>> = Arc::new(RwLock::new((0.0, now_timestamp())));
    static ref HISTORY_LOCK: Arc<tokio::sync::Mutex<()>> = Arc::new(tokio::sync::Mutex::new(()));
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PriceHistory {
    pub timestamp: u64,
    pub btc_usd: f64,
    pub source: String,
}

#[derive(Debug, Clone)]
pub struct PriceFeed {
    latest_deribit_price: Arc<RwLock<f64>>,
    last_deribit_update: Arc<RwLock<i64>>,
    client: reqwest::Client,
    active_ws: Arc<TokioMutex<Option<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>>>,
    last_ws_activity: Arc<RwLock<Instant>>,
    is_connecting: Arc<RwLock<bool>>,
    error_counts: Arc<RwLock<HashMap<String, u32>>>,
        connected: Arc<Mutex<bool>>, // New field
    last_price_update: Arc<RwLock<Instant>>,  // For rate limiting
}

impl PriceFeed {
    pub fn new() -> Self {
        // Configure client with timeouts
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
            
        PriceFeed {
            latest_deribit_price: Arc::new(RwLock::new(0.0)),
            last_deribit_update: Arc::new(RwLock::new(0)),
            client,
            active_ws: Arc::new(TokioMutex::new(None)),
            last_ws_activity: Arc::new(RwLock::new(Instant::now())),
            is_connecting: Arc::new(RwLock::new(false)),
            error_counts: Arc::new(RwLock::new(HashMap::new())),
            connected: Arc::new(RwLock::new(false)), // Initialize as disconnected
            last_price_update: Arc::new(RwLock::new(Instant::now())),
        }
    }
    
    pub async fn is_websocket_connected(&self) -> bool {
        *self.connected.read().unwrap()
    }
    pub async fn start_deribit_feed(&self, shutdown_rx: &mut broadcast::Receiver<()>) -> Result<(), PulserError> {
        let config: toml::Value = match fs::read_to_string("config/service_config.toml") {
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
        
        let max_backoff = 60; // Maximum backoff in seconds
        
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Deribit feed shutting down");
                    // Properly close any active WebSocket connection
                    let mut ws_guard = self.active_ws.lock().await;
                    if let Some(mut ws) = ws_guard.take() {
                        info!("Closing Deribit WebSocket connection");
                                                *self.connected.write().unwrap() = false; // Mark as disconnected
                        match tokio::time::timeout(Duration::from_secs(5), ws.close(None)).await {
                            Ok(result) => {
                                if let Err(e) = result {
                                    warn!("Error closing Deribit WebSocket: {}", e);
                                }
                            },
                            Err(_) => {
                                warn!("Timeout closing Deribit WebSocket, abandoning connection");
                            }
                        }
                    }
                    break;
                }
                
                _ = ping_interval.tick() => {
                    let mut ws_guard = self.active_ws.lock().await;
                    if let Some(ws) = ws_guard.as_mut() {
                        match ws.send(Message::Ping(vec![1, 2, 3, 4])).await {
                            Ok(_) => {
                                // Update last activity only on successful ping
                                // Don't update on other messages to ensure connection health checks
                                *self.last_ws_activity.write().unwrap() = Instant::now();
                            }
                            Err(e) => {
                                warn!("Failed to send ping: {}", e);
                                *ws_guard = None;
                                *self.is_connecting.write().unwrap() = false;
                                                                *self.connected.write().unwrap() = false; // Mark as disconnected

                            }
                        }
                    }
                }
                
                _ = async {
                    // Only attempt reconnection if not already connecting and no active connection
                    let should_connect = {
                        let is_connecting = *self.is_connecting.read().unwrap();
                        let has_active_ws = self.active_ws.lock().await.is_some();
                        !is_connecting && !has_active_ws
                    };
                    
                    if should_connect {
                        // Set connecting flag
                        *self.is_connecting.write().unwrap() = true;
                        
                        // Exponential backoff with jitter
                        let backoff = std::cmp::min(2u64.pow(attempts), max_backoff);
                        let jitter = rand::random::<u64>() % 1000; // 0-999ms of jitter
                        let backoff_ms = backoff * 1000 + jitter;
                        
                        if attempts > 0 {
                            info!("Waiting {}ms before reconnecting to Deribit", backoff_ms);
                            sleep(Duration::from_millis(backoff_ms)).await;
                        }
                        
                        info!("Connecting to Deribit (attempt {})", attempts + 1);
                        match self.connect_deribit(&api_key, &secret).await {
                            Ok(_) => {
                                info!("Successfully connected to Deribit");
                                attempts = 0; // Reset on success
                                                                *self.connected.write().unwrap() = true; // Mark as connected

                            },
                            Err(e) => {
                                warn!("Deribit WebSocket connection failed: {}", e);
                                attempts = attempts.saturating_add(1);
                                                                *self.connected.write().unwrap() = false; // Mark as disconnected

                            }
                        }
                        
                        // Clear connecting flag
                        *self.is_connecting.write().unwrap() = false;
                    }
                    
                    // Small sleep to prevent CPU spinning
                    sleep(Duration::from_millis(100)).await;
                } => {}
            }
        }
        
        Ok(())
    }

    async fn connect_deribit(&self, api_key: &str, secret: &str) -> Result<(), PulserError> {
        // Use timeout for connection
        let connect_result = match timeout(
            Duration::from_secs(WS_CONNECTION_TIMEOUT_SECS),
            connect_async("wss://test.deribit.com/ws/api/v2")
         ).await {
            Ok(Ok((ws_conn, _))) => {
                // Successfully connected
                *self.connected.write().unwrap() = true; // Mark as connected
                Ok(ws_conn)
            },
            Ok(Err(e)) => {
                *self.connected.write().unwrap() = false; // Mark as disconnected
                Err(PulserError::NetworkError(format!("WebSocket connection error: {}", e)))
            },
            Err(_) => {
                *self.connected.write().unwrap() = false; // Mark as disconnected
                Err(PulserError::NetworkError("WebSocket connection timeout".to_string()))
            },
        }?;
        
        let mut ws_conn = connect_result;
        
        // Reset last activity timer
        *self.last_ws_activity.write().unwrap() = Instant::now();
        
        // Auth message
        let auth_msg = json!({"jsonrpc": "2.0", "id": 1, "method": "public/auth", "params": {"grant_type": "client_credentials", "client_id": api_key, "client_secret": secret}});
        if let Err(e) = ws_conn.send(Message::Text(auth_msg.to_string())).await {
                    *self.connected.write().unwrap() = false; // Mark as disconnected

            return Err(PulserError::ApiError(format!("Failed to send auth message: {}", e)));
        }
        
// Wait for auth response with timeout
let token_msg = match timeout(Duration::from_secs(10), ws_conn.next()).await {
    Ok(Some(Ok(msg))) => msg,
    Ok(Some(Err(e))) => {
        *self.connected.write().unwrap() = false; // Mark as disconnected
        return Err(PulserError::ApiError(format!("WebSocket error: {}", e)));
    }, // Added closing brace
    Ok(None) => {
        *self.connected.write().unwrap() = false; // Mark as disconnected
        return Err(PulserError::ApiError("WebSocket closed".to_string()));
    }, // Added closing brace
    Err(_) => return Err(PulserError::ApiError("Auth response timeout".to_string())),
};
        
        // Update activity time
        *self.last_ws_activity.write().unwrap() = Instant::now();
        
        // Parse auth response
        let token_text = token_msg.into_text()?;
        let token_json: Value = serde_json::from_str(&token_text)?;
        trace!("Deribit auth response: {:?}", token_json);
        
        let access_token = token_json["result"]["access_token"]
            .as_str()
            .ok_or(PulserError::ApiError("Auth failed: no access token".to_string()))?
            .to_string();

        // Subscribe to ticker
        let sub_msg = json!({"jsonrpc": "2.0", "id": 2, "method": "public/subscribe", "params": {"channels": ["ticker.BTC-PERPETUAL.raw"]}});
        if let Err(e) = ws_conn.send(Message::Text(sub_msg.to_string())).await {
            return Err(PulserError::ApiError(format!("Failed to send subscribe message: {}", e)));
        }
        
        info!("Subscribed to Deribit ticker.BTC-PERPETUAL.raw");
        
        // Store the connection
        {
            let mut ws_guard = self.active_ws.lock().await;
            *ws_guard = Some(ws_conn);
        }
        
        // Start a separate task to process messages
        let active_ws_clone = self.active_ws.clone();
        let latest_price_clone = self.latest_deribit_price.clone();
        let last_update_clone = self.last_deribit_update.clone();
        let last_activity_clone = self.last_ws_activity.clone();
       let last_price_update_clone = self.last_price_update.clone();

// Create a bounded channel to buffer WebSocket messages
let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(100); // 100-message buffer

// Spawn a task to read from WebSocket and send to the channel
let ws_clone = self.active_ws.clone();
tokio::spawn(async move {
    let mut ws_lock = ws_clone.lock().await;
    if let Some(mut ws) = ws_lock.take() { // Take ownership to ensure cleanup
        while let Some(msg_result) = ws.next().await {
            match msg_result {
                Ok(msg) => {
                    if tx.send(msg).await.is_err() {
                        warn!("Deribit WSS buffer full, dropping messages");
                        break; // Receiver dropped or channel closed
                    }
                }
                Err(e) => {
                    warn!("WebSocket read error: {}", e);
                    break;
                }
            }
        }
        debug!("Deribit WebSocket reader task ended");
    }
    // WebSocket is implicitly closed when ws goes out of scope
});

// Spawn the processing task using the channel receiver
tokio::spawn(async move {
    while let Some(msg) = rx.recv().await {
        // Update last activity time
        *last_activity_clone.write().unwrap() = Instant::now();

        match msg {
            Message::Text(text) => {
                if let Ok(data) = serde_json::from_str::<Value>(&text) {
                    if let Some(price) = data.get("params")
                        .and_then(|p| p.get("data"))
                        .and_then(|d| d.get("last_price"))
                        .and_then(|p| p.as_f64())
                    {
                        let now = Instant::now();
                        let last_update = *last_price_update_clone.read().unwrap();
                        if now.duration_since(last_update).as_millis() >= PRICE_UPDATE_INTERVAL_MS as u128 {
                            // Only update price and timestamp if enough time has passed
                            *latest_price_clone.write().unwrap() = price;
                            *last_update_clone.write().unwrap() = now_timestamp();
                            *last_price_update_clone.write().unwrap() = now;
                            trace!("Deribit BTC/USD: ${:.2} (throttled)", price);
                        }
                    }
                }
            }
            Message::Ping(data) => {
                // Respond to ping with pong
                let mut ws_lock = active_ws_clone.lock().await;
                if let Some(ws) = ws_lock.as_mut() {
                    if let Err(e) = ws.send(Message::Pong(data)).await {
                        warn!("Failed to send pong: {}", e);
                        break;
                    }
                } else {
                    break; // WebSocket gone
                }
            }
            Message::Close(_) => {
                debug!("WebSocket close frame received");
                break;
            }
            _ => {}
        }
    }
    // Clear the WebSocket connection when the task ends
    let mut ws_lock = active_ws_clone.lock().await;
    *ws_lock = None;
    debug!("Deribit price feed processor task ended");
});
        
        Ok(())
    }
    }

    pub async fn get_deribit_price(&self) -> Result<f64, PulserError> {
        let price = *self.latest_deribit_price.read().unwrap();
        let last = *self.last_deribit_update.read().unwrap();
        let now = now_timestamp();
        
        // If we have a recent Deribit price, use it
        if price > 0.0 && (now - last) < 60_i64 {
            trace!("Using cached Deribit price: ${:.2}", price);
            return Ok(price);
        }

        // If Deribit price is stale, try to use cached median
        let (cached_median, cached_time) = {
            let cache = PRICE_CACHE.read().unwrap();
            (cache.0, cache.1)
        };
        
        if cached_median > 0.0 && (now - cached_time) < DEFAULT_CACHE_DURATION_SECS as i64 {
            warn!("Deribit price stale ({}s), using median cache: ${:.2}", now - last, cached_median);
            return Ok(cached_median);
        }

        // If both Deribit and median cache are stale, try a direct fetch from Deribit
        warn!("Deribit price stale ({}s) and no fresh median, fetching from Deribit", now - last);
        match self.fetch_deribit_price().await {
            Ok(fresh_price) => {
                if fresh_price > 0.0 {
                    let mut price_guard = self.latest_deribit_price.write().unwrap();
                    let mut time_guard = self.last_deribit_update.write().unwrap();
                    *price_guard = fresh_price;
                    *time_guard = now_timestamp();
                    info!("Updated Deribit price: ${:.2}", fresh_price);
                    Ok(fresh_price)
                } else {
                    warn!("Invalid price from Deribit direct fetch: ${:.2}", fresh_price);
                    if cached_median > 0.0 {
                        warn!("Falling back to stale median: ${:.2}", cached_median);
                        return Ok(cached_median);
                    } else {
                        return Err(PulserError::PriceFeedError("No valid price available".to_string()));
                    }
                }
            }
            Err(e) => {
                if cached_median > 0.0 {
                    warn!("Failed to fetch Deribit price: {}, falling back to stale median: ${:.2}", e, cached_median);
                    Ok(cached_median)
                } else {
                    // No prices available, try fallback sources directly
                    warn!("No valid prices available from primary sources, trying emergency fallback");
match emergency_fetch_price(&self.client).await {

                        Ok(emergency_price) => {
                            info!("Using emergency fallback price: ${:.2}", emergency_price);
                            Ok(emergency_price)
                        }
                        Err(e2) => {
                            error!("Emergency price fetch failed after all primary sources failed: {}, original error: {}", e2, e);
                            return Err(PulserError::PriceFeedError("All price sources failed including emergency sources".to_string()));
                        }
                    }
                }
            }
        }
    }

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
                            Some(price) if price > 0.0 => Ok(price),
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

// Place this outside of impl PriceFeed
pub async fn emergency_fetch_price(client: &Client) -> Result<f64, PulserError> {
    let mut prices = Vec::new();
    
    if let Ok(price) = fetch_coinbase_price(client).await {
        if price > 0.0 { prices.push(price); }
    }
    
    if let Ok(price) = fetch_binance_price(client).await {
        if price > 0.0 { prices.push(price); }
    }
    
    if let Ok(price) = fetch_kraken_price(client).await {
        if price > 0.0 { prices.push(price); }
    }
    
    if prices.is_empty() {
        return Err(PulserError::PriceFeedError("All emergency price sources failed".to_string()));
    }
    
    prices.sort_by(|a: &f64, b: &f64| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = if prices.len() % 2 == 0 {
        (prices[prices.len() / 2 - 1] + prices[prices.len() / 2]) / 2.0
    } else {
        prices[prices.len() / 2]
    };
    
    Ok(median)
}

async fn fetch_coinbase_price(client: &Client) -> Result<f64, PulserError> {
    match timeout(
        Duration::from_secs(5), 
        client.get("https://api.coinbase.com/v2/prices/BTC-USD/spot").send()
    ).await {
        Ok(Ok(response)) => {
            if !response.status().is_success() {
                return Err(PulserError::ApiError(format!("Coinbase API error: {}", response.status())));
            }
            
            let json: Value = response.json().await?;
            let price = json["data"]["amount"].as_str()
                .ok_or_else(|| PulserError::ApiError("Invalid Coinbase response".to_string()))?
                .parse::<f64>()
                .map_err(|_| PulserError::ApiError("Failed to parse Coinbase price".to_string()))?;
            
            Ok(price)
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Coinbase request failed: {}", e))),
        Err(_) => Err(PulserError::NetworkError("Coinbase request timed out".to_string())),
    }
}

async fn fetch_binance_price(client: &Client) -> Result<f64, PulserError> {
    match timeout(
        Duration::from_secs(5), 
        client.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT").send()
    ).await {
        Ok(Ok(response)) => {
            if !response.status().is_success() {
                return Err(PulserError::ApiError(format!("Binance API error: {}", response.status())));
            }
            
            let json: Value = response.json().await?;
            let price = json["price"].as_str()
                .ok_or_else(|| PulserError::ApiError("Invalid Binance response".to_string()))?
                .parse::<f64>()
                .map_err(|_| PulserError::ApiError("Failed to parse Binance price".to_string()))?;
            
            Ok(price)
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Binance request failed: {}", e))),
        Err(_) => Err(PulserError::NetworkError("Binance request timed out".to_string())),
    }
}

async fn fetch_kraken_price(client: &Client) -> Result<f64, PulserError> {
    match timeout(
        Duration::from_secs(5), 
        client.get("https://api.kraken.com/0/public/Ticker?pair=XBTUSD").send()
    ).await {
        Ok(Ok(response)) => {
            if !response.status().is_success() {
                return Err(PulserError::ApiError(format!("Kraken API error: {}", response.status())));
            }
            
            let json: Value = response.json().await?;
            let price = json["result"]["XXBTZUSD"]["c"][0].as_str()
                .ok_or_else(|| PulserError::ApiError("Invalid Kraken response".to_string()))?
                .parse::<f64>()
                .map_err(|_| PulserError::ApiError("Failed to parse Kraken price".to_string()))?;
            
            Ok(price)
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Kraken request failed: {}", e))),
        Err(_) => Err(PulserError::NetworkError("Kraken request timed out".to_string())),
    }
}

pub async fn fetch_btc_usd_price(client: &Client, price_feed: &PriceFeed) -> Result<PriceInfo, PulserError> {
    let (cached_price, cached_timestamp) = {
        let cache = PRICE_CACHE.read().unwrap();
        (cache.0, cache.1)
    };
    
    let now = now_timestamp();
    if cached_price > 0.0 && (now - cached_timestamp) < DEFAULT_CACHE_DURATION_SECS as i64 {
        trace!("Using cached median price: ${:.2}", cached_price);
        return Ok(PriceInfo { 
            raw_btc_usd: cached_price, 
            timestamp: cached_timestamp, 
            price_feeds: HashMap::new() 
        });
    }

    if let Some(price) = price_feed.get_deribit_price().await.ok() {
        if price > 0.0 {
            let now = now_timestamp();
            *PRICE_CACHE.write().unwrap() = (price, now);
            return Ok(PriceInfo { 
                raw_btc_usd: price, 
                timestamp: now, 
                price_feeds: [("Deribit".to_string(), price)].into() 
            });
        }
    }

match fetch_from_sources(client).await {
        Ok((price, feeds)) => {
            if price <= 0.0 {
                return Err(PulserError::PriceFeedError(format!("Invalid median price: ${:.2}", price)));
            }
            let now = now_timestamp();
            *PRICE_CACHE.write().unwrap() = (price, now);
            let price_info = PriceInfo { 
                raw_btc_usd: price, 
                timestamp: now, 
                price_feeds: feeds, 
            };
            let history = price_info.price_feeds.iter().map(|(source, &btc_usd)| PriceHistory {
                timestamp: now as u64,
                btc_usd,
                source: source.clone(),
            }).collect::<Vec<PriceHistory>>();
            tokio::spawn(async move {
if let Err(e) = save_price_history(history).await {
                    warn!("Failed to save price history: {}", e);
                }
            });
            Ok(price_info)
        },
        Err(e) => {
            if cached_price > 0.0 && (now - cached_timestamp) < 600 {
                warn!("All price sources failed: {}. Using stale cache: ${:.2}", e, cached_price);
                return Ok(PriceInfo { 
                    raw_btc_usd: cached_price, 
                    timestamp: cached_timestamp, 
                    price_feeds: HashMap::new() 
                });
            }
            match emergency_fetch_price(client).await {
                Ok(emergency_price) => {
                    warn!("Using emergency price: ${:.2}", emergency_price);
                    let now = now_timestamp();
                    *PRICE_CACHE.write().unwrap() = (emergency_price, now);
                    return Ok(PriceInfo { 
                        raw_btc_usd: emergency_price, 
                        timestamp: now, 
                        price_feeds: [("Emergency".to_string(), emergency_price)].into() 
                    });
                },
                Err(e2) => {
                    error!("Emergency price fetch failed: {}, original error: {}", e2, e);
                    return Err(PulserError::PriceFeedError("All price sources failed".to_string()));
                }
            }
        }
    }
}

async fn fetch_from_sources(client: &Client) -> Result<(f64, HashMap<String, f64>), PulserError> {
    let sources = vec![
        ("Kraken", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD", "result.XXBTZUSD.c.0"),
        ("Binance", "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT", "price"),
        ("Coinbase", "https://api.coinbase.com/v2/prices/BTC-USD/spot", "data.amount"),
        ("Bitstamp", "https://www.bitstamp.net/api/v2/ticker/btcusd", "last"),
    ];

    let mut prices = HashMap::new();
    let mut btc_prices = Vec::new();
    let mut success_count = 0;
    let mut error_count = 0;
    
    for (source_name, url, path) in sources {
        for retry in 0..FALLBACK_RETRY_ATTEMPTS {
            if retry > 0 {
                debug!("Retry {} for {}", retry, source_name);
                sleep(Duration::from_millis(500 * (1 << retry))).await;
            }
            
            match fetch_from_source(client, url, path).await {
                Ok(price) => {
                    if price > 0.0 {
                        debug!("BTC-USD ({}): ${:.2}", source_name, price);
                        prices.insert(source_name.to_string(), price);
                        btc_prices.push(price);
                        success_count += 1;
                        break;
                    } else {
                        warn!("Ignoring suspicious price from {}: ${:.2}", source_name, price);
                        error_count += 1;
                    }
                }
                Err(e) => {
                    warn!("Failed to fetch from {} (attempt {}/{}): {}", 
                          source_name, retry + 1, FALLBACK_RETRY_ATTEMPTS, e);
                    error_count += 1;
                }
            }
        }
    }

    if btc_prices.is_empty() {
        return Err(PulserError::PriceFeedError(format!(
            "All price sources failed ({} errors)", error_count)));
    }
    
    info!("Fetched {} prices with {} errors", success_count, error_count);

btc_prices.sort_by(|a: &f64, b: &f64| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));


    let price = if btc_prices.len() % 2 == 0 {
        (btc_prices[btc_prices.len()/2 - 1] + btc_prices[btc_prices.len()/2]) / 2.0
    } else {
        btc_prices[btc_prices.len()/2]
    };
    
    prices.insert("Median".to_string(), price);
    
    info!("Calculated median price: ${:.2} from {} sources", price, btc_prices.len());
    Ok((price, prices))
}

async fn fetch_from_source(client: &Client, url: &str, path: &str) -> Result<f64, PulserError> {
    // Use a shorter timeout for individual source fetch to allow trying multiple sources quickly
    match timeout(Duration::from_secs(5), client.get(url).send()).await {
        Ok(Ok(response)) => {
            if !response.status().is_success() {
                return Err(PulserError::ApiError(format!("API error from {}: {}", url, response.status())));
            }

            match timeout(Duration::from_secs(3), response.json::<Value>()).await {
                Ok(Ok(json)) => {
                    let mut value = &json;
                    for part in path.split('.') {
                        value = value.get(part)
                            .ok_or_else(|| PulserError::ApiError(format!("Missing field '{}' in response from {}", part, url)))?;
                    }

                    match value {
                        Value::String(s) => {
                            s.parse::<f64>()
                                .map_err(|_| PulserError::ApiError(format!("Failed to parse price '{}' from {}", s, url)))
                        }
                        Value::Number(n) => {
                            n.as_f64()
                                .ok_or_else(|| PulserError::ApiError(format!("Failed to convert to f64 from {}", url)))
                        }
                        _ => Err(PulserError::ApiError(format!("Unexpected value type from {}: {:?}", url, value))),
                    }
                },
                Ok(Err(e)) => Err(PulserError::ApiError(format!("JSON parse failed from {}: {}", url, e))),
                Err(_) => Err(PulserError::ApiError(format!("JSON parse timed out from {}", url))),
            }
        },
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Request to {} failed: {}", url, e))),
        Err(_) => Err(PulserError::NetworkError(format!("Request to {} timed out", url))),
    }
}

pub fn is_price_cache_stale() -> bool {
    let cache = PRICE_CACHE.read().unwrap();
    let now = now_timestamp();
    cache.0 <= 0.0 || (now - cache.1) > DEFAULT_CACHE_DURATION_SECS as i64
}

pub fn get_cached_price() -> Option<f64> {
    let cache = PRICE_CACHE.read().unwrap();
    if cache.0 > 0.0 { Some(cache.0) } else { None }
}

async fn save_price_history(entries: Vec<PriceHistory>) -> Result<(), PulserError> {
    if entries.is_empty() { return Ok(()); }
    
    // Use a lock to prevent concurrent modifications
    let _lock = HISTORY_LOCK.lock().await;
    
let mut history = load_price_history().await?;
    history.extend(entries);
    
    // Sort by timestamp in descending order (newest first)
    history.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    
    // Limit number of entries to avoid excessive growth
    if history.len() > MAX_HISTORY_ENTRIES { 
        history.truncate(MAX_HISTORY_ENTRIES); 
    }
    
    // Ensure data directory exists
    let path = std::path::Path::new("data");
    if !path.exists() {
        fs::create_dir_all(path).map_err(|e| 
            PulserError::StorageError(format!("Failed to create data directory: {}", e)))?;
    }
    
    // Atomically save the data using a temp file
    let json = serde_json::to_string_pretty(&history)
        .map_err(|e| PulserError::StorageError(format!("Failed to serialize price history: {}", e)))?;
    
    let temp_path = "data/price_history.json.tmp";
    let final_path = "data/price_history.json";
    
    // Write to temp file first
    fs::write(temp_path, &json)
        .map_err(|e| PulserError::StorageError(format!("Failed to write price history: {}", e)))?;
    
    // Then atomically rename
    fs::rename(temp_path, final_path)
        .map_err(|e| PulserError::StorageError(format!("Failed to rename price history file: {}", e)))?;
    
    trace!("Saved {} price history entries", history.len());
    Ok(())
}

async fn load_price_history() -> Result<Vec<PriceHistory>, PulserError> {
    let path = std::path::Path::new("data/price_history.json");
    
    if !path.exists() { 
        return Ok(Vec::new()); 
    }
    
    let content = fs::read_to_string(path)
        .map_err(|e| PulserError::StorageError(format!("Failed to read price history: {}", e)))?;
    
    if content.trim().is_empty() { 
        return Ok(Vec::new()); 
    }
    
    serde_json::from_str(&content)
        .map_err(|e| PulserError::StorageError(format!("Failed to parse price history: {}", e)))
}

