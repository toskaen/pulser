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
// Instead of having separate deribit.rs, use the one in sources
pub use self::sources::deribit;

// Re-export main components - using the PriceFeed from sources/deribit
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

// Static references (moved from deribit.rs)
lazy_static::lazy_static! {
    pub(crate) static ref PRICE_CACHE: Arc<RwLock<(f64, i64)>> = Arc::new(RwLock::new((0.0, crate::utils::now_timestamp())));
    pub(crate) static ref HISTORY_LOCK: Arc<tokio::sync::Mutex<()>> = Arc::new(tokio::sync::Mutex::new(()));
}

// Utility function - can be removed if you're using the one from utils module
pub fn now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64
}
