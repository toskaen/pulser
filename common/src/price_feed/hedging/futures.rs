use crate::error::PulserError;
use serde_json::Value;
use tokio::time::timeout;
use std::time::Duration;
use reqwest::Client;
use super::super::DEFAULT_TIMEOUT_SECS;

/// Gets the price of a BTC futures contract from Deribit
pub async fn get_futures_price(
    client: &Client, 
    instrument: &str
) -> Result<f64, PulserError> {
    let url = format!("https://test.deribit.com/api/v2/public/ticker?instrument_name={}", instrument);
    
    match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), client.get(&url).send()).await {
        Ok(Ok(response)) => {
            if !response.status().is_success() {
                return Err(PulserError::ApiError(format!("Deribit API error: {}", response.status())));
            }
            match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), response.json::<Value>()).await {
                Ok(Ok(json)) => {
                    json["result"]["last_price"].as_f64()
                        .filter(|&price| price > 0.0)
                        .ok_or_else(|| PulserError::PriceFeedError("Missing or invalid futures price".to_string()))
                }
                Ok(Err(e)) => Err(PulserError::ApiError(format!("Failed to parse Deribit futures response: {}", e))),
                Err(_) => Err(PulserError::ApiError("Timeout parsing Deribit futures response".to_string())),
            }
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Deribit futures request failed: {}", e))),
        Err(_) => Err(PulserError::NetworkError("Deribit futures request timed out".to_string())),
    }
}

/// Gets the price of a combined futures legs (combo) from Deribit
pub async fn get_combo_price(
    client: &Client, 
    leg1: &str, 
    leg2: &str
) -> Result<f64, PulserError> {
    let instrument = format!("{}_{}", leg1, leg2);
    let url = format!("https://test.deribit.com/api/v2/public/ticker?instrument_name={}", instrument);
    
    match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), client.get(&url).send()).await {
        Ok(Ok(response)) => {
            if !response.status().is_success() {
                return Err(PulserError::ApiError(format!("Deribit API error: {}", response.status())));
            }
            match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), response.json::<Value>()).await {
                Ok(Ok(json)) => {
                    json["result"]["last_price"].as_f64()
                        .filter(|&price| price > 0.0)
                        .ok_or_else(|| PulserError::PriceFeedError("Missing or invalid combo price".to_string()))
                }
                Ok(Err(e)) => Err(PulserError::ApiError(format!("Failed to parse Deribit combo response: {}", e))),
                Err(_) => Err(PulserError::ApiError("Timeout parsing Deribit combo response".to_string())),
            }
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Deribit combo request failed: {}", e))),
        Err(_) => Err(PulserError::NetworkError("Deribit combo request timed out".to_string())),
    }
}

/// Gets the funding rate for a perpetual futures contract
pub async fn get_funding_rate(
    client: &Client,
    instrument: &str
) -> Result<f64, PulserError> {
    let url = format!("https://test.deribit.com/api/v2/public/get_funding_rate_history?instrument_name={}&count=1", instrument);
    
    match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), client.get(&url).send()).await {
        Ok(Ok(response)) => {
            if !response.status().is_success() {
                return Err(PulserError::ApiError(format!("Deribit API error: {}", response.status())));
            }
            match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), response.json::<Value>()).await {
                Ok(Ok(json)) => {
                    json["result"][0]["funding_rate"].as_f64()
                        .ok_or_else(|| PulserError::PriceFeedError("Missing or invalid funding rate".to_string()))
                }
                Ok(Err(e)) => Err(PulserError::ApiError(format!("Failed to parse funding rate response: {}", e))),
                Err(_) => Err(PulserError::ApiError("Timeout parsing funding rate response".to_string())),
            }
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Funding rate request failed: {}", e))),
        Err(_) => Err(PulserError::NetworkError("Funding rate request timed out".to_string())),
    }
}
