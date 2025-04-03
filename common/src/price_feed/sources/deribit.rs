use crate::error::PulserError;
use super::{PriceProvider, PriceSource};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::timeout;
use std::time::Duration;

pub struct DeribitProvider {
    weight: f64,
    timeout_secs: u64,
    futures_only: bool,
}

impl DeribitProvider {
    pub fn new() -> Self {
        Self {
            weight: 0.8, // Lower weight as this is futures, not spot
            timeout_secs: 5,
            futures_only: true,
        }
    }
    
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }
    
    pub fn with_futures_only(mut self, futures_only: bool) -> Self {
        self.futures_only = futures_only;
        self
    }
}

#[async_trait]
impl PriceProvider for DeribitProvider {


    async fn fetch_price(&self, client: &Client) -> Result<PriceSource, PulserError> {
        // Get the perpetual price (BTC-PERPETUAL)
        let url = "https://www.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL";
        
        let response = match timeout(
            Duration::from_secs(self.timeout_secs),
            client.get(url).send()
        ).await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => return Err(PulserError::NetworkError(format!("Deribit request failed: {}", e))),
            Err(_) => return Err(PulserError::NetworkError("Deribit request timed out".to_string())),
        };
        
        if !response.status().is_success() {
            return Err(PulserError::ApiError(format!("Deribit API error: {}", response.status())));
        }
        
        let json: Value = match timeout(
            Duration::from_secs(self.timeout_secs),
            response.json()
        ).await {
            Ok(Ok(json)) => json,
            Ok(Err(e)) => return Err(PulserError::ApiError(format!("Failed to parse Deribit response: {}", e))),
            Err(_) => return Err(PulserError::ApiError("Timeout parsing Deribit response".to_string())),
        };
        
        let price = json["result"]["last_price"].as_f64()
            .ok_or_else(|| PulserError::ApiError("Invalid Deribit response structure".to_string()))?;
            
        // For volume, we'll use the 24h volume from the index (or estimate if not available)
        let volume = json["result"]["stats"]["volume"].as_f64()
            .unwrap_or_else(|| {
                // If volume isn't available, use a reasonable estimate
                json["result"]["open_interest"].as_f64().unwrap_or(1000.0)
            });
            
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_secs();
            
        Ok(PriceSource {
            name: "Deribit".to_string(),
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
        "Deribit"
    }
    
    fn weight(&self) -> f64 {
        self.weight
    }
}
