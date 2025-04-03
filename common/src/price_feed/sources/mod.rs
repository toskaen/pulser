use crate::error::{PulserError, ErrorCategory};
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use std::time::Duration;
use reqwest::Client;
use std::collections::HashMap;

pub mod deribit; // Moved from root to maintain structure
pub mod binance;
pub mod bitfinex;
pub mod kraken;

pub use binance::BinanceProvider;
pub use bitfinex::BitfinexProvider;
pub use kraken::KrakenProvider;
pub use deribit::DeribitProvider;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceSource {
    pub name: String,
    pub url: String,
    pub weight: f64,  // Reliability/volume weight
    pub price: f64,
    pub volume: f64,
    pub timestamp: u64,
    pub error_count: u32,
    pub last_success: u64,
}

#[async_trait]
pub trait PriceProvider: Send + Sync {
    async fn fetch_price(&self, client: &Client) -> Result<PriceSource, PulserError>;
    fn name(&self) -> &str;
    fn weight(&self) -> f64;
}

pub struct SourceManager {
    sources: Vec<Arc<dyn PriceProvider>>,
    client: Client,
}

impl SourceManager {
    pub fn new(client: Client) -> Self {
        Self {
            sources: Vec::new(),
            client,
        }
    }

    pub fn register<T: PriceProvider + 'static>(&mut self, source: T) {
        self.sources.push(Arc::new(source));
    }

    pub async fn fetch_all_prices(&self) -> HashMap<String, Result<PriceSource, PulserError>> {
        let mut results = HashMap::new();
        
        for source in &self.sources {
            let result = source.fetch_price(&self.client).await;
            results.insert(source.name().to_string(), result);
        }
        
        results
    }
}
