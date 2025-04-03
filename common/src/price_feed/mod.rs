// common/src/price_feed/mod.rs
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use tokio::sync::RwLock;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::error::PulserError;
use crate::types::PriceInfo;

// Re-export content from other modules
pub mod sources;
pub mod http_sources;
pub mod cache;
pub mod aggregator;
pub mod websocket;
pub mod hedging;

// Re-export main components
pub use self::sources::deribit::DeribitProvider as PriceFeed;
pub use self::http_sources::{emergency_fetch_price, fetch_btc_usd_price};
pub use self::cache::{get_cached_price, is_price_cache_stale, save_price_history, load_price_history};
pub use self::aggregator::PriceAggregator;

// Constants
pub const DEFAULT_CACHE_DURATION_SECS: u64 = 120; // 2 minutes
pub const DEFAULT_RETRY_MAX: u32 = 3;
pub const DEFAULT_MAX_RETRY_TIME_SECS: u64 = 120;
pub const DEFAULT_TIMEOUT_SECS: u64 = 10;
pub const WS_PING_INTERVAL_SECS: u64 = 30;
pub const WS_CONNECTION_TIMEOUT_SECS: u64 = 30;
pub const MAX_HISTORY_ENTRIES: usize = 1440;
pub const FALLBACK_RETRY_ATTEMPTS: u32 = 2;
pub const PRICE_UPDATE_INTERVAL_MS: u64 = 1000;

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

// Add the extension trait to handle the interface gap
pub trait PriceFeedExtensions {
    async fn get_price(&self) -> Result<crate::types::PriceInfo, PulserError>;
    async fn get_deribit_price(&self) -> Result<f64, PulserError>;
    async fn is_websocket_connected(&self) -> bool;
}

// Implement it for DeribitProvider
impl PriceFeedExtensions for sources::deribit::DeribitProvider {
    async fn get_price(&self) -> Result<crate::types::PriceInfo, PulserError> {
        use crate::types::PriceInfo;
        use std::collections::HashMap;
        use sources::PriceProvider;
        
        let client = reqwest::Client::new();
        let source = PriceProvider::fetch_price(self, &client).await?;
        
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
    
    async fn is_websocket_connected(&self) -> bool {
        // Implement based on existing status tracking
        // For now, just return false as a placeholder
        false
    }
}

// Utility function
pub fn now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64
}
