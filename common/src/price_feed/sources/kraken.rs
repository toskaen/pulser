use crate::error::PulserError;
use super::{PriceProvider, PriceSource};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::timeout;
use std::time::Duration;

pub struct KrakenProvider {
    weight: f64,
    timeout_secs: u64,
}

impl KrakenProvider {
    pub fn new() -> Self {
        Self {
            weight: 1.2, // Higher weight for Kraken (our preference)
            timeout_secs: 5,
        }
    }
    
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }
}

#[async_trait]
impl PriceProvider for KrakenProvider {
    async fn fetch_price(&self, client: &Client) -> Result<PriceSource, PulserError> {
        let url = "https://api.kraken.com/0/public/Ticker?pair=XBTUSD";
        
        let response = match timeout(
            Duration::from_secs(self.timeout_secs),
            client.get(url).send()
        ).await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => return Err(PulserError::NetworkError(format!("Kraken request failed: {}", e))),
            Err(_) => return Err(PulserError::NetworkError("Kraken request timed out".to_string())),
        };
        
        if !response.status().is_success() {
            return Err(PulserError::ApiError(format!("Kraken API error: {}", response.status())));
        }
        
        let json: Value = match timeout(
            Duration::from_secs(self.timeout_secs),
            response.json()
        ).await {
            Ok(Ok(json)) => json,
            Ok(Err(e)) => return Err(PulserError::ApiError(format!("Failed to parse Kraken response: {}", e))),
            Err(_) => return Err(PulserError::ApiError("Timeout parsing Kraken response".to_string())),
        };
        
        let price = json["result"]["XXBTZUSD"]["c"][0].as_str()
            .ok_or_else(|| PulserError::ApiError("Invalid Kraken response structure".to_string()))?
            .parse::<f64>()
            .map_err(|_| PulserError::ApiError("Failed to parse Kraken price".to_string()))?;
            
        let volume = json["result"]["XXBTZUSD"]["v"][1].as_str()
            .ok_or_else(|| PulserError::ApiError("Invalid Kraken volume data".to_string()))?
            .parse::<f64>()
            .map_err(|_| PulserError::ApiError("Failed to parse Kraken volume".to_string()))?;
            
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_secs();
            
        Ok(PriceSource {
            name: "Kraken".to_string(),
            url: url.to_string(),
            weight: self.weight,
            price,
            volume,
            timestamp: now,
            error_count: 0,
            last_success: now,
        })
    }
    
    fn name(&self) -> &str {
        "Kraken"
    }
    
    fn weight(&self) -> f64 {
        self.weight
    }
}
