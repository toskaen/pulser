use reqwest::Client;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use log::{info, warn, debug};
use serde::{Serialize, Deserialize};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use crate::error::PulserError;
use crate::types::PriceInfo;
use crate::utils::now_timestamp;
use tokio::sync::broadcast;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio::sync::Mutex as TokioMutex;



// Constants
pub const DEFAULT_CACHE_DURATION_SECS: u64 = 120; // 2 minutes
pub const DEFAULT_RETRY_MAX: u32 = 3;
pub const DEFAULT_MAX_RETRY_TIME_SECS: u64 = 120;
pub const DEFAULT_TIMEOUT_SECS: u64 = 10;

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
active_ws: Arc<TokioMutex<Option<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>>>
}

impl PriceFeed {
    pub fn new() -> Self {
        PriceFeed {
            latest_deribit_price: Arc::new(RwLock::new(0.0)),
            last_deribit_update: Arc::new(RwLock::new(0)),
            client: reqwest::Client::new(),
active_ws: Arc::new(TokioMutex::new(None)),

        }
    }
    
    pub async fn start_deribit_feed(&self, shutdown_rx: &mut broadcast::Receiver<()>) -> Result<(), PulserError> {
        let config: toml::Value = toml::from_str(&fs::read_to_string("config/service_config.toml").unwrap_or_default())?;
        let api_key = config.get("deribit_id").and_then(|v| v.as_str()).unwrap_or("your_deribit_id").to_string();
        let secret = config.get("deribit_secret").and_then(|v| v.as_str()).unwrap_or("your_deribit_secret").to_string();

        let mut attempts = 0u32;
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Deribit feed shutting down");
                    // Properly close any active WebSocket connection
                    let mut ws_guard = self.active_ws.lock().await;
                    if let Some(mut ws) = ws_guard.take() {
                        info!("Closing Deribit WebSocket connection");
                        if let Err(e) = ws.close(None).await {
                            warn!("Error closing Deribit WebSocket: {}", e);
                        }
                    }
                    break;
                }
                _ = async {
                    if let Err(e) = self.connect_deribit(&api_key, &secret).await {
                        warn!("Deribit WebSocket failed: {}. Retrying in {}s...", e, 5 * 2u64.pow(attempts));
                        sleep(Duration::from_secs(5 * 2u64.pow(attempts))).await;
                        attempts = attempts.saturating_add(1); // Prevent overflow
                    } else {
                        attempts = 0; // Reset on success
                    }
                } => {}
            }
        }
        Ok(())
    }

    async fn connect_deribit(&self, api_key: &str, secret: &str) -> Result<(), PulserError> {
        let (mut ws_conn, _) = connect_async("wss://test.deribit.com/ws/api/v2").await?;
        
        // Auth message
        let auth_msg = json!({"jsonrpc": "2.0", "id": 1, "method": "public/auth", "params": {"grant_type": "client_credentials", "client_id": api_key, "client_secret": secret}});
        ws_conn.send(Message::Text(auth_msg.to_string())).await?;
        
        let token_msg = ws_conn.next().await.ok_or(PulserError::ApiError("No auth response".to_string()))??;
        let token_json: Value = serde_json::from_str(&token_msg.into_text()?)?;
        debug!("Deribit auth response: {:?}", token_json);
        
        let access_token = token_json["result"]["access_token"]
            .as_str()
            .ok_or(PulserError::ApiError("Auth failed: no access token".to_string()))?
            .to_string();

        ws_conn.send(Message::Text(json!({"jsonrpc": "2.0", "id": 2, "method": "public/subscribe", "params": {"channels": ["ticker.BTC-PERPETUAL.raw"]}}).to_string())).await?;
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
        
        tokio::spawn(async move {
            loop {
                let mut ws_lock = active_ws_clone.lock().await;
                let ws_opt = &mut *ws_lock;
                
                if let Some(ws) = ws_opt {
                    match ws.next().await {
                        Some(Ok(msg)) => {
                            // Process the message
                            if let Ok(text) = msg.into_text() {
                                if let Ok(data) = serde_json::from_str::<Value>(&text) {
                                    if let Some(price) = data.get("params")
                                        .and_then(|p| p.get("data"))
                                        .and_then(|d| d.get("last_price"))
                                        .and_then(|p| p.as_f64()) 
                                    {
                                        let now = now_timestamp();
                                        *latest_price_clone.write().unwrap() = price;
                                        *last_update_clone.write().unwrap() = now as i64;
                                        debug!("Deribit BTC/USD: ${:.2}", price);
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!("WebSocket error: {}", e);
                            break;
                        }
                        None => {
                            debug!("WebSocket connection closed");
                            break;
                        }
                    }
                } else {
                    break;
                }
                
                // Release the lock to allow other operations
                drop(ws_lock);
                
                // Small sleep to prevent CPU spinning
                sleep(Duration::from_millis(10)).await;
            }
        });
        
        Ok(())
    }

    pub async fn get_deribit_price(&self) -> Result<f64, PulserError> {
        let price = *self.latest_deribit_price.read().unwrap();
        let last = *self.last_deribit_update.read().unwrap();
        let now = now_timestamp();
        if price > 0.0 && (now - last) < 60 {
            debug!("Using cached Deribit price: ${:.2}", price);
            Ok(price)
        } else {
            warn!("Stale Deribit price (last update: {}s ago), fetching fresh", now - last);
            let fresh_price = self.fetch_deribit_price().await?;
            let mut price_guard = self.latest_deribit_price.write().unwrap();
            let mut time_guard = self.last_deribit_update.write().unwrap();
            *price_guard = fresh_price;
            *time_guard = now_timestamp() as i64;
            info!("Updated Deribit price: ${:.2}", fresh_price);
            Ok(fresh_price)
        }
    }

    async fn fetch_deribit_price(&self) -> Result<f64, PulserError> {
        let url = "https://test.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL";
        let response = self.client.get(url).send().await?;
        let json: Value = response.json().await?;
        let price = json["result"]["last_price"]
            .as_f64()
            .ok_or_else(|| PulserError::PriceFeedError("Invalid Deribit response".into()))?;
        Ok(price)
    }
}

pub async fn fetch_btc_usd_price(client: &Client) -> Result<PriceInfo, PulserError> {
    let (cached_price, cached_timestamp) = {
        let cache = PRICE_CACHE.read().unwrap();
        (cache.0, cache.1)
    };

    let now = now_timestamp();
    if cached_price > 0.0 && (now - cached_timestamp) < DEFAULT_CACHE_DURATION_SECS as i64 {
        debug!("Using cached price: ${:.2}", cached_price);
        return Ok(PriceInfo {
            raw_btc_usd: cached_price,
            timestamp: cached_timestamp,
            price_feeds: HashMap::new(),
        });
    }

    let start_time = Instant::now();
    let max_duration = Duration::from_secs(DEFAULT_MAX_RETRY_TIME_SECS); // Fixed here

    for retry in 0..DEFAULT_RETRY_MAX {
        if start_time.elapsed() >= max_duration {
            warn!("Price fetch exceeded {}s", DEFAULT_MAX_RETRY_TIME_SECS);
            break;
        }
        if retry > 0 {
            let backoff = Duration::from_millis(500 * 2u64.pow(retry));
            debug!("Retry attempt {}", retry + 1);
            sleep(backoff).await;
        }
        match fetch_from_sources(client).await {
            Ok((price, feeds)) => {
                let now = now_timestamp();
                *PRICE_CACHE.write().unwrap() = (price, now);
                let history = feeds.iter().map(|(source, &btc_usd)| PriceHistory {
                    timestamp: now as u64,
                    btc_usd,
                    source: source.clone(),
                }).collect::<Vec<PriceHistory>>();
                if !history.is_empty() {
                    tokio::spawn(async move {
                        if let Err(e) = save_price_history(history).await {
                            warn!("Failed to save price history: {}", e);
                        }
                    });
                }
                return Ok(PriceInfo {
                    raw_btc_usd: price,
                    timestamp: now,
                    price_feeds: feeds,
                });
            }
            Err(e) => warn!("Fetch attempt {} failed: {}", retry + 1, e),
        }
    }
    // ... rest of function unchanged ...


    let (stale_price, stale_timestamp) = {
        let cache = PRICE_CACHE.read().unwrap();
        (cache.0, cache.1)
    };

    if stale_price > 0.0 {
        warn!("Using stale price ${:.2} from {}s ago", stale_price, now - stale_timestamp);
        return Ok(PriceInfo {
            raw_btc_usd: stale_price,
            timestamp: stale_timestamp,
            price_feeds: HashMap::new(),
        });
    }
    Err(PulserError::PriceFeedError("No price available".to_string()))
}

async fn fetch_from_sources(client: &Client) -> Result<(f64, HashMap<String, f64>), PulserError> {
    let sources = vec![
        ("Coingecko", "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd", "bitcoin.usd"),
        ("Kraken", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD", "result.XBTUSD.c.0"),
        ("Binance", "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT", "price"),
        ("Coinbase", "https://api.coinbase.com/v2/prices/BTC-USD/spot", "data.amount"),
        ("Bitstamp", "https://www.bitstamp.net/api/v2/ticker/btcusd", "last"),
    ];

    let mut prices = HashMap::new();
    let mut btc_prices = Vec::new();
    let now = SystemTime::now().duration_since(UNIX_EPOCH)
        .map_err(|e| PulserError::InternalError(format!("Time error: {}", e)))?.as_secs();

    for (source_name, url, path) in sources {
        match fetch_from_source(client, url, path).await {
            Ok(price) => {
                if price >= 1000.0 && price <= 1_000_000.0 {
                    debug!("BTC-USD ({}): ${:.2}", source_name, price);
                    prices.insert(source_name.to_string(), price);
                    btc_prices.push(price);
                } else {
                    warn!("Ignoring suspicious price from {}: ${:.2}", source_name, price);
                }
            }
            Err(e) => warn!("Failed to fetch from {}: {}", source_name, e),
        }
    }

    if btc_prices.is_empty() {
        return Err(PulserError::PriceFeedError("All price sources failed".to_string()));
    }

    btc_prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let price = if btc_prices.len() % 2 == 0 {
        (btc_prices[btc_prices.len()/2 - 1] + btc_prices[btc_prices.len()/2]) / 2.0
    } else {
        btc_prices[btc_prices.len()/2]
    };
    prices.insert("Median".to_string(), price);
    Ok((price, prices))
}

async fn fetch_from_source(client: &Client, url: &str, path: &str) -> Result<f64, PulserError> {
    let response = timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), client.get(url).send())
        .await
        .map_err(|_| PulserError::NetworkError(format!("Request to {} timed out after {}s", url, DEFAULT_TIMEOUT_SECS)))?
        .map_err(|e| PulserError::NetworkError(format!("Request failed: {}", e)))?;

    if !response.status().is_success() {
        return Err(PulserError::ApiError(format!("API error: {}", response.status())));
    }

    let json: Value = timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), response.json())
        .await
        .map_err(|_| PulserError::ApiError("JSON parse timed out".to_string()))?
        .map_err(|e| PulserError::ApiError(format!("JSON parse failed: {}", e)))?;

    let mut value = &json;
    for part in path.split('.') {
        value = value.get(part).ok_or_else(|| PulserError::ApiError(format!("Missing field: {}", part)))?;
    }

    match value {
        Value::String(s) => s.parse::<f64>().map_err(|_| PulserError::ApiError("Failed to parse price".to_string())),
        Value::Number(n) => n.as_f64().ok_or_else(|| PulserError::ApiError("Failed to convert to f64".to_string())),
        _ => Err(PulserError::ApiError("Unexpected value type".to_string())),
    }
}

pub fn is_price_cache_stale() -> bool {
    let cache = PRICE_CACHE.read().unwrap();
    let now = now_timestamp();
    cache.0 == 0.0 || (now - cache.1) > DEFAULT_CACHE_DURATION_SECS as i64
}

pub fn get_cached_price() -> Option<f64> {
    let cache = PRICE_CACHE.read().unwrap();
    if cache.0 > 0.0 { Some(cache.0) } else { None }
}

async fn save_price_history(entries: Vec<PriceHistory>) -> Result<(), PulserError> {
    if entries.is_empty() { return Ok(()); }
    let mut history = load_price_history().await?;
let _lock = HISTORY_LOCK.lock().await;
    history.extend(entries);
    history.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    if history.len() > 1440 { history.truncate(1440); }
    let path = std::path::Path::new("data");
    if !path.exists() {
        fs::create_dir_all(path).map_err(|e| PulserError::StorageError(format!("Failed to create data directory: {}", e)))?;
    }
    let json = serde_json::to_string_pretty(&history)
        .map_err(|e| PulserError::StorageError(format!("Failed to serialize price history: {}", e)))?;
    let temp_path = "data/price_history.json.tmp";
    fs::write(temp_path, &json)
        .map_err(|e| PulserError::StorageError(format!("Failed to write price history: {}", e)))?;
    fs::rename(temp_path, "data/price_history.json")
        .map_err(|e| PulserError::StorageError(format!("Failed to rename price history file: {}", e)))?;
    Ok(())
}

async fn load_price_history() -> Result<Vec<PriceHistory>, PulserError> {
    let path = std::path::Path::new("data/price_history.json");
    if !path.exists() { return Ok(Vec::new()); }
    let content = fs::read_to_string(path)
        .map_err(|e| PulserError::StorageError(format!("Failed to read price history: {}", e)))?;
    if content.trim().is_empty() { return Ok(Vec::new()); }
    serde_json::from_str(&content)
        .map_err(|e| PulserError::StorageError(format!("Failed to parse price history: {}", e)))
}
