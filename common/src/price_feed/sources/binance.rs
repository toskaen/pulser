use crate::error::PulserError;
use super::{PriceProvider, PriceSource};
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::timeout;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct BinanceResponse {
    #[serde(rename = "lastPrice")]
    price: String,
    #[serde(rename = "weightedAvgPrice")]
    weighted_avg_price: String,
    #[serde(rename = "volume")]
    volume: String,
}

pub struct BinanceProvider {
    weight: f64,
    timeout_secs: u64,
}

impl BinanceProvider {
    pub fn new() -> Self {
        Self {
            weight: 1.0, // Base weight for Binance (lowest priority)
            timeout_secs: 5,
        }
    }
    
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }
}

#[async_trait]
impl PriceProvider for BinanceProvider {
    fn name(&self) -> &str {
        "Binance"
    }
    
    fn weight(&self) -> f64 {
        self.weight
    }
    
    async fn fetch_price(&self, client: &Client) -> Result<PriceSource, PulserError> {
        let url = "https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT";
        
        let response = match timeout(
            Duration::from_secs(self.timeout_secs),
            client.get(url).send()
        ).await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => return Err(PulserError::NetworkError(format!("Binance request failed: {}", e))),
            Err(_) => return Err(PulserError::NetworkError("Binance request timed out".to_string())),
        };
        
        if !response.status().is_success() {
            return Err(PulserError::ApiError(format!("Binance API error: {}", response.status())));
        }
        
        let data: BinanceResponse = match timeout(
            Duration::from_secs(self.timeout_secs),
            response.json()
        ).await {
            Ok(Ok(data)) => data,
            Ok(Err(e)) => return Err(PulserError::ApiError(format!("Failed to parse Binance response: {}", e))),
            Err(_) => return Err(PulserError::ApiError("Timeout parsing Binance response".to_string())),
        };
        
        let price = data.price.parse::<f64>()
            .map_err(|_| PulserError::ApiError("Failed to parse Binance price".to_string()))?;
            
        let volume = data.volume.parse::<f64>()
            .map_err(|_| PulserError::ApiError("Failed to parse Binance volume".to_string()))?;
            
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_secs();
            
        Ok(PriceSource {
            name: "Binance".to_string(),
            url: url.to_string(),
            weight: self.weight,
            price,
            volume,
            timestamp: now,
            error_count: 0,
            last_success: now,
        })
    }
}
