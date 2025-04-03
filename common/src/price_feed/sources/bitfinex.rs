use crate::error::PulserError;
use super::{PriceProvider, PriceSource};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::timeout;
use std::time::Duration;

pub struct BitfinexProvider {
    weight: f64,
    timeout_secs: u64,
}

impl BitfinexProvider {
    pub fn new() -> Self {
        Self {
            weight: 1.1, // Second preference after Kraken
            timeout_secs: 5,
        }
    }
    
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }
}

#[async_trait]
impl PriceProvider for BitfinexProvider {
    async fn fetch_price(&self, client: &Client) -> Result<PriceSource, PulserError> {
        let url = "https://api-pub.bitfinex.com/v2/ticker/tBTCUSD";
        
        let response = match timeout(
            Duration::from_secs(self.timeout_secs),
            client.get(url).send()
        ).await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => return Err(PulserError::NetworkError(format!("Bitfinex request failed: {}", e))),
            Err(_) => return Err(PulserError::NetworkError("Bitfinex request timed out".to_string())),
        };
        
        if !response.status().is_success() {
            return Err(PulserError::ApiError(format!("Bitfinex API error: {}", response.status())));
        }
        
        let data: Value = match timeout(
            Duration::from_secs(self.timeout_secs),
            response.json()
        ).await {
            Ok(Ok(data)) => data,
            Ok(Err(e)) => return Err(PulserError::ApiError(format!("Failed to parse Bitfinex response: {}", e))),
            Err(_) => return Err(PulserError::ApiError("Timeout parsing Bitfinex response".to_string())),
        };
        
        // Bitfinex ticker format: [BID, BID_SIZE, ASK, ASK_SIZE, DAILY_CHANGE, DAILY_CHANGE_RELATIVE, LAST_PRICE, VOLUME, HIGH, LOW]
        let price = data[6].as_f64()
            .ok_or_else(|| PulserError::ApiError("Failed to parse Bitfinex price".to_string()))?;
            
        let volume = data[7].as_f64()
            .ok_or_else(|| PulserError::ApiError("Failed to parse Bitfinex volume".to_string()))?;
            
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_secs();
            
        Ok(PriceSource {
            name: "Bitfinex".to_string(),
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
        "Bitfinex"
    }
    
    fn weight(&self) -> f64 {
        self.weight
    }
}
