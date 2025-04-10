use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, broadcast};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::error::PulserError;
use crate::types::PriceInfo;

// Re-export content from other modules
pub mod sources;
pub mod http_sources;
pub mod cache;
pub mod aggregator;
pub mod hedging;
pub mod price_feed; // Assuming PriceFeed is in price_feed.rs

// Constants
pub const DEFAULT_CACHE_DURATION_SECS: u64 = 120; // 2 minutes
pub const DEFAULT_RETRY_MAX: u32 = 3;
pub const DEFAULT_MAX_RETRY_TIME_SECS: u64 = 120;
pub const DEFAULT_TIMEOUT_SECS: u64 = 10;
pub const WS_PING_INTERVAL_SECS: u64 = 60;
pub const WS_CONNECTION_TIMEOUT_SECS: u64 = 30;
pub const MAX_HISTORY_ENTRIES: usize = 1440;
pub const FALLBACK_RETRY_ATTEMPTS: u32 = 2;
pub const PRICE_UPDATE_INTERVAL_MS: u64 = 1000;
pub const GLOBAL_PRICE_UPDATE_INTERVAL_SECS: u64 = 1000; // Update global price once per minute
pub const PRICE_STALENESS_THRESHOLD_SECS: u64 = 2100; // Consider price stale after 2 minutes

// Common types
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PriceHistory {
    pub timestamp: u64,
    pub btc_usd: f64,
    pub source: String,
}

// Static references
lazy_static::lazy_static! {
    pub(crate) static ref PRICE_CACHE: Arc<RwLock<(f64, i64)>> = Arc::new(RwLock::new((0.0, crate::utils::now_timestamp())));
    pub(crate) static ref HISTORY_LOCK: Arc<tokio::sync::Mutex<()>> = Arc::new(tokio::sync::Mutex::new(()));
}

pub trait PriceFeedTrait: Send + Sync {
    async fn get_price(&self) -> Result<PriceInfo, PulserError>;
    async fn get_spot_price(&self) -> Result<f64, PulserError>;
    async fn get_futures_price(&self) -> Result<f64, PulserError>;
    async fn is_connected(&self) -> bool;
    async fn connect(&self) -> Result<(), PulserError>;
    async fn disconnect(&self) -> Result<(), PulserError>;
}

pub trait PriceFeedExtensions {
    async fn get_price(&self) -> Result<PriceInfo, PulserError>;
    async fn get_deribit_price(&self) -> Result<f64, PulserError>;
    fn get_spot_price(&self) -> impl std::future::Future<Output = Result<f64, PulserError>> + Send;
    fn get_futures_price(&self) -> impl std::future::Future<Output = Result<f64, PulserError>> + Send;
}

// Re-export the actual price feed
pub use self::price_feed::PriceFeed; // Adjust path if PriceFeed is elsewhere

// Implement the extension trait for DeribitProvider
impl PriceFeedExtensions for sources::deribit::DeribitProvider {
    async fn get_price(&self) -> Result<PriceInfo, PulserError> {
        use crate::types::PriceInfo;
        use std::collections::HashMap;
        use sources::PriceProvider;
        
        let client = reqwest::Client::new();
        let source = self.fetch_price(&client).await?;
        
        let mut price_feeds = HashMap::new();
        price_feeds.insert("Deribit".to_string(), source.price);
        
        Ok(PriceInfo {
            raw_btc_usd: source.price,
            timestamp: source.timestamp as i64,
            price_feeds,
        })
    }
    
    async fn get_deribit_price(&self) -> Result<f64, PulserError> {
        use sources::PriceProvider;
        let client = reqwest::Client::new();
        let source = self.fetch_price(&client).await?;
        Ok(source.price)
    }
    
    fn get_spot_price(&self) -> impl std::future::Future<Output = Result<f64, PulserError>> + Send {
        async move {
            crate::price_feed::http_sources::fetch_kraken_price(&reqwest::Client::new()).await
        }
    }
    
    fn get_futures_price(&self) -> impl std::future::Future<Output = Result<f64, PulserError>> + Send {
        async move {
            self.get_deribit_price().await
        }
    }
}

pub fn now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64
}
