use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use log::{info, warn, error, debug};
use serde::{Serialize, Deserialize};
use crate::error::PulserError;
use crate::types::PriceInfo;
use crate::utils::now_timestamp;
use tokio::time::{sleep, timeout};

// Constants
pub const DEFAULT_CACHE_DURATION_SECS: u64 = 120; // 2 minutes
pub const DEFAULT_RETRY_MAX: u32 = 3;
pub const DEFAULT_MAX_RETRY_TIME_SECS: u64 = 120;
pub const DEFAULT_TIMEOUT_SECS: u64 = 10;

lazy_static::lazy_static! {
    static ref PRICE_CACHE: Arc<RwLock<(f64, i64)>> = 
        Arc::new(RwLock::new((0.0, now_timestamp())));
    static ref HISTORY_LOCK: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PriceHistory {
    pub timestamp: u64,
    pub btc_usd: f64,
    pub source: String,
}

/// Fetch BTC-USD price from multiple sources with retry logic
pub async fn fetch_btc_usd_price(client: &Client) -> Result<PriceInfo, PulserError> {
    let cache = PRICE_CACHE.read().unwrap();
    let now = now_timestamp();
    if cache.0 > 0.0 && (now - cache.1) < DEFAULT_CACHE_DURATION_SECS as i64 {
        debug!("Using cached price: ${:.2}", cache.0);
        return Ok(PriceInfo {
            raw_btc_usd: cache.0,
            timestamp: cache.1,
            price_feeds: HashMap::new(),
        });
    }
    drop(cache);

    let start_time = Instant::now();
    let max_duration = Duration::from_secs(DEFAULT_MAX_RETRY_TIME_SECS);

    for retry in 0..DEFAULT_RETRY_MAX {
        if start_time.elapsed() >= max_duration {
            error!("Price fetch exceeded {}s", DEFAULT_MAX_RETRY_TIME_SECS);
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
                return Ok(PriceInfo {
                    raw_btc_usd: price,
                    timestamp: now,
                    price_feeds: feeds,
                });
            }
            Err(e) => warn!("Fetch attempt {} failed: {}", retry + 1, e),
        }
    }

    let cache = PRICE_CACHE.read().unwrap();
    if cache.0 > 0.0 {
        warn!("Using stale price ${:.2} from {}s ago", cache.0, now_timestamp() - cache.1);
        return Ok(PriceInfo {
            raw_btc_usd: cache.0,
            timestamp: cache.1,
            price_feeds: HashMap::new(),
        });
    }
    Err(PulserError::PriceFeedError("No price available".to_string()))
}

/// Fetch prices from multiple sources and calculate median
async fn fetch_from_sources(client: &Client) -> Result<(f64, HashMap<String, f64>), PulserError> {
    let sources = vec![
        ("Coingecko", "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd", "bitcoin.usd"),
        ("Kraken", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD", "result.XXBTZUSD.c.0"),
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
                    info!("BTC-USD ({}): ${:.2}", source_name, price);
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
    prices.insert("BTC-USD".to_string(), price);

    let history: Vec<PriceHistory> = prices.iter().map(|(source, &price)| PriceHistory {
        timestamp: now,
        btc_usd: price,
        source: source.clone(),
    }).collect();
    if !history.is_empty() {
        tokio::spawn(async move {
            if let Err(e) = save_price_history(history).await {
                warn!("Failed to save price history: {}", e);
            }
        });
    }

    Ok((price, prices))
}

/// Fetch price from a specific source
async fn fetch_from_source(client: &Client, url: &str, path: &str) -> Result<f64, PulserError> {
    let response = match timeout(
        Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        client.get(url).send()
    ).await {
        Ok(result) => result.map_err(|e| PulserError::NetworkError(format!("Request failed: {}", e)))?,
        Err(_) => return Err(PulserError::NetworkError(format!("Request to {} timed out after {}s", url, DEFAULT_TIMEOUT_SECS))),
    };

    if !response.status().is_success() {
        return Err(PulserError::ApiError(format!("API error: {}", response.status())));
    }

    let json: Value = match timeout(
        Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        response.json()
    ).await {
        Ok(result) => result.map_err(|e| PulserError::ApiError(format!("JSON parse failed: {}", e)))?,
        Err(_) => return Err(PulserError::ApiError("JSON parse timed out".to_string())),
    };

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

/// Check if price cache is stale
pub fn is_price_cache_stale() -> bool {
    let cache = PRICE_CACHE.read().unwrap();
    let now = now_timestamp();
    cache.0 == 0.0 || (now - cache.1) > DEFAULT_CACHE_DURATION_SECS as i64
}

/// Get cached price (if available)
pub fn get_cached_price() -> Option<f64> {
    let cache = PRICE_CACHE.read().unwrap();
    if cache.0 > 0.0 {
        Some(cache.0)
    } else {
        None
    }
}

/// Save price history to file
async fn save_price_history(entries: Vec<PriceHistory>) -> Result<(), PulserError> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut history = load_price_history().await?;
    let _lock = HISTORY_LOCK.lock().unwrap(); // Lock after await
    history.extend(entries);
    history.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    if history.len() > 1440 {
        history.truncate(1440);
    }
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

/// Load price history from file
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
