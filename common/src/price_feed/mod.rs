// common/src/price_feed/mod.rs
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
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
pub mod websocket;
pub mod hedging;

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

// In common/src/price_feed/mod.rs
pub trait PriceFeedTrait: Send + Sync {
    async fn get_price(&self) -> Result<PriceInfo, PulserError>;
    async fn get_spot_price(&self) -> Result<f64, PulserError>;
    async fn get_futures_price(&self) -> Result<f64, PulserError>;
    async fn is_connected(&self) -> bool;
    async fn connect(&self) -> Result<(), PulserError>;
    async fn disconnect(&self) -> Result<(), PulserError>;
}
// Important: Define the extension trait for price feed providers
pub trait PriceFeedExtensions {
    async fn get_price(&self) -> Result<PriceInfo, PulserError>;
    async fn get_deribit_price(&self) -> Result<f64, PulserError>;
    async fn is_websocket_connected(&self) -> bool;
    async fn start_deribit_feed(&self, shutdown_rx: &mut broadcast::Receiver<()>) -> Result<(), PulserError>;
    async fn shutdown_websocket(&self) -> Option<Result<(), PulserError>>;
    fn get_spot_price(&self) -> impl std::future::Future<Output = Result<f64, PulserError>> + Send;
fn get_futures_price(&self) -> impl std::future::Future<Output = Result<f64, PulserError>> + Send;

}

// Re-export the actual price feed to use
pub use self::sources::deribit::DeribitProvider as PriceFeed;

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
    
    async fn is_websocket_connected(&self) -> bool {
        *self.connected.read().await
    }
    
    async fn start_deribit_feed(&self, shutdown_rx: &mut broadcast::Receiver<()>) -> Result<(), PulserError> {
        // Implementation based on code from price_feed.rs
        // Simplified version shown here
        let client = reqwest::Client::new();
        let url = "https://www.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL";
        
        let mut attempts = 0;
        let max_attempts = 3;
        
        tokio::select! {
            _ = shutdown_rx.recv() => {
                return Ok(());
            }
            _ = async {
                loop {
                    match tokio::time::timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), client.get(url).send()).await {
                        Ok(Ok(resp)) => {
                            if resp.status().is_success() {
                                // Success - update connected state
                                let mut connected = self.connected.write().await;
                                *connected = true;
                                let mut last_activity = self.last_ws_activity.write().await;
                                *last_activity = Instant::now();
                                break;
                            }
                        },
                        _ => {}
                    }
                    
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(PulserError::NetworkError("Failed to connect to Deribit".to_string()));
                    }
                    
                    tokio::time::sleep(Duration::from_secs(1 << attempts)).await;
                }
                
                Ok(())
            } => {}
        }
        
        Ok(())
    }
    
    async fn shutdown_websocket(&self) -> Option<Result<(), PulserError>> {
        let mut connected = self.connected.write().await;
        *connected = false;
        Some(Ok(()))
    }
    
    fn get_spot_price(&self) -> impl std::future::Future<Output = Result<f64, PulserError>> + Send {
        async move {
            // Query spot price from reliable sources
            let client = reqwest::Client::new();
            // Try to get from a spot exchange like Kraken or Coinbase
crate::price_feed::http_sources::fetch_kraken_price(&client).await

        }
    }
    
    fn get_futures_price(&self) -> impl std::future::Future<Output = Result<f64, PulserError>> + Send {
        async move {
            // Use the Deribit perpetual futures price
            self.get_deribit_price().await
        }
    }
}

// Utility function
pub fn now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64
}
