use crate::error::PulserError;
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use std::time::Duration;
use reqwest::Client;
use std::collections::HashMap;

pub mod deribit;
pub mod binance;
pub mod bitfinex;
pub mod kraken;

pub use binance::BinanceProvider;
pub use bitfinex::BitfinexProvider;
pub use kraken::KrakenProvider;
pub use self::deribit::DeribitProvider;

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

// Use async_trait for the async trait
#[async_trait]
pub trait PriceProvider: Send + Sync {
    fn name(&self) -> &str;
    fn weight(&self) -> f64;
    async fn fetch_price(&self, client: &Client) -> Result<PriceSource, PulserError>;
}

// Define an enum to hold all possible provider types
pub enum PriceProviderEnum {
    Deribit(DeribitProvider),
    Binance(BinanceProvider),
    Bitfinex(BitfinexProvider),
    Kraken(KrakenProvider),
}

// Implement methods for the enum to delegate to the inner type
impl PriceProviderEnum {
    pub fn name(&self) -> &str {
        match self {
            Self::Deribit(p) => p.name(),
            Self::Binance(p) => p.name(),
            Self::Bitfinex(p) => p.name(),
            Self::Kraken(p) => p.name(),
        }
    }
    
    pub fn weight(&self) -> f64 {
        match self {
            Self::Deribit(p) => p.weight(),
            Self::Binance(p) => p.weight(),
            Self::Bitfinex(p) => p.weight(),
            Self::Kraken(p) => p.weight(),
        }
    }
    
    pub async fn fetch_price(&self, client: &Client) -> Result<PriceSource, PulserError> {
        match self {
            Self::Deribit(p) => p.fetch_price(client).await,
            Self::Binance(p) => p.fetch_price(client).await,
            Self::Bitfinex(p) => p.fetch_price(client).await,
            Self::Kraken(p) => p.fetch_price(client).await,
        }
    }
}

// Implement From traits to easily convert each provider to the enum
impl From<DeribitProvider> for PriceProviderEnum {
    fn from(provider: DeribitProvider) -> Self {
        Self::Deribit(provider)
    }
}

impl From<BinanceProvider> for PriceProviderEnum {
    fn from(provider: BinanceProvider) -> Self {
        Self::Binance(provider)
    }
}

impl From<BitfinexProvider> for PriceProviderEnum {
    fn from(provider: BitfinexProvider) -> Self {
        Self::Bitfinex(provider)
    }
}

impl From<KrakenProvider> for PriceProviderEnum {
    fn from(provider: KrakenProvider) -> Self {
        Self::Kraken(provider)
    }
}

// Source manager to handle multiple price sources
pub struct SourceManager {
    sources: Vec<PriceProviderEnum>,
    client: Client,
}

impl SourceManager {
    pub fn new(client: Client) -> Self {
        Self {
            sources: Vec::new(),
            client,
        }
    }

    // Register a provider that can be converted to the enum
    pub fn register<T: Into<PriceProviderEnum>>(&mut self, provider: T) {
        self.sources.push(provider.into());
    }

    // Fetch prices from all sources
    pub async fn fetch_all_prices(&self) -> HashMap<String, Result<PriceSource, PulserError>> {
        let mut results = HashMap::new();
        
        for source in &self.sources {
            let result = source.fetch_price(&self.client).await;
            results.insert(source.name().to_string(), result);
        }
        
        results
    }
}
