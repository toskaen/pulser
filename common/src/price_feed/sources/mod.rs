use crate::error::PulserError;
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use std::time::Duration;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use crate::utils;



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

// In common/src/price_feed/sources/mod.rs

pub struct SourceHealth {
    source_name: String,
    last_success: Arc<AtomicI64>,
    success_count: Arc<AtomicUsize>,
    failure_count: Arc<AtomicUsize>,
    consecutive_failures: Arc<AtomicUsize>,
    total_latency_ms: Arc<AtomicU64>,
}

impl SourceHealth {
    pub fn new(source_name: &str) -> Self {
        Self {
            source_name: source_name.to_string(),
            last_success: Arc::new(AtomicI64::new(0)),
            success_count: Arc::new(AtomicUsize::new(0)),
            failure_count: Arc::new(AtomicUsize::new(0)),
            consecutive_failures: Arc::new(AtomicUsize::new(0)),
            total_latency_ms: Arc::new(AtomicU64::new(0)),
        }
    }
    
    pub fn record_success(&self, latency_ms: u64) {
        self.last_success.store(now_timestamp(), Ordering::SeqCst);
        self.success_count.fetch_add(1, Ordering::SeqCst);
        self.consecutive_failures.store(0, Ordering::SeqCst);
        self.total_latency_ms.fetch_add(latency_ms, Ordering::SeqCst);
    }
    
    pub fn record_failure(&self) {
        self.failure_count.fetch_add(1, Ordering::SeqCst);
        self.consecutive_failures.fetch_add(1, Ordering::SeqCst);
    }
    
    pub fn get_reliability_score(&self) -> f64 {
        let success = self.success_count.load(Ordering::SeqCst) as f64;
        let total = success + self.failure_count.load(Ordering::SeqCst) as f64;
        
        if total == 0.0 {
            return 0.0;
        }
        
        // Base reliability on success rate
        let success_rate = success / total;
        
        // Factor in recency - penalize if last success was long ago
        let now = now_timestamp();
        let last_success = self.last_success.load(Ordering::SeqCst);
        let seconds_since_success = (now - last_success).max(0) as f64;
let recency_factor = f64::exp(-0.0001 * seconds_since_success);

        
        // Penalize consecutive failures
        let consecutive_failures = self.consecutive_failures.load(Ordering::SeqCst) as f64;
        let failure_penalty = (-0.1 * consecutive_failures).exp();
        
        success_rate * recency_factor * failure_penalty
    }
}

// Enhance SourceManager to track health
impl SourceManager {
    pub fn new(client: Client) -> Self {
        Self {
            sources: Vec::new(),
            client,
            health_trackers: HashMap::new(),
        }
    }
    
    pub fn register<T: Into<PriceProviderEnum>>(&mut self, provider: T) {
        let provider_enum = provider.into();
        let name = provider_enum.name().to_string();
        
        self.health_trackers.insert(name.clone(), Arc::new(SourceHealth::new(&name)));
        self.sources.push(provider_enum);
    }
    
    pub async fn fetch_all_prices(&self) -> HashMap<String, Result<PriceSource, PulserError>> {
        let mut results = HashMap::new();
        
        for source in &self.sources {
            let name = source.name().to_string();
            let health = self.health_trackers.get(&name).unwrap();
            
            let start = Instant::now();
            let result = source.fetch_price(&self.client).await;
            let elapsed = start.elapsed().as_millis() as u64;
            
            match &result {
                Ok(_) => health.record_success(elapsed),
                Err(_) => health.record_failure(),
            }
            
            results.insert(name, result);
        }
        
        results
    }
    
    pub fn get_source_health(&self, name: &str) -> Option<Arc<SourceHealth>> {
        self.health_trackers.get(name).cloned()
    }
    
    pub fn get_healthy_sources(&self) -> Vec<(String, f64)> {
        self.health_trackers.iter()
            .map(|(name, health)| (name.clone(), health.get_reliability_score()))
            .filter(|(_, score)| *score > 0.5)
            .collect()
    }
}
