// common/src/price_feed/sources/deribit.rs
use crate::error::PulserError;
use super::{PriceProvider, PriceSource};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::time::timeout;
use log::info;
use crate::websocket::WebSocketManager;

pub struct DeribitProvider {
    pub weight: f64,
    pub timeout_secs: u64,
    pub futures_only: bool,
}

impl DeribitProvider {
    pub fn new() -> Self {
        Self {
            weight: 0.8,
            timeout_secs: 5,
            futures_only: true,
        }
    }

    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }
}

#[async_trait]
impl PriceProvider for DeribitProvider {
    async fn fetch_price(&self, client: &Client) -> Result<PriceSource, PulserError> {
        let url = "https://www.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL";
        let response = timeout(Duration::from_secs(self.timeout_secs), client.get(url).send()).await??;
        if !response.status().is_success() {
            return Err(PulserError::ApiError(format!("Deribit API error: {}", response.status())));
        }
        let json: Value = timeout(Duration::from_secs(self.timeout_secs), response.json()).await??;
        let price = json["result"]["last_price"].as_f64()
            .ok_or_else(|| PulserError::ApiError("Invalid Deribit response structure".to_string()))?;
        let volume = json["result"]["stats"]["volume"].as_f64().unwrap_or(1000.0);
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
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

    fn name(&self) -> &str { "Deribit" }
    fn weight(&self) -> f64 { self.weight }
}
