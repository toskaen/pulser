use crate::error::PulserError;
use serde_json::Value;
use tokio::time::timeout;
use std::time::Duration;
use reqwest::Client;
use super::super::DEFAULT_TIMEOUT_SECS;

/// Gets the price of a BTC option from Deribit
pub async fn get_option_price(
    client: &Client, 
    strike: f64, 
    option_type: &str
) -> Result<f64, PulserError> {
    let instrument = format!("BTC-{}-{}", strike.round() as i64, option_type.to_uppercase());
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
                        .ok_or_else(|| PulserError::PriceFeedError("Missing or invalid option price".to_string()))
                }
                Ok(Err(e)) => Err(PulserError::ApiError(format!("Failed to parse Deribit option response: {}", e))),
                Err(_) => Err(PulserError::ApiError("Timeout parsing Deribit option response".to_string())),
            }
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Deribit option request failed: {}", e))),
        Err(_) => Err(PulserError::NetworkError("Deribit option request timed out".to_string())),
    }
}

/// Gets the Greeks for a BTC option from Deribit
pub async fn get_option_greeks(
    client: &Client, 
    strike: f64, 
    option_type: &str
) -> Result<(f64, f64, f64, f64), PulserError> {
    let instrument = format!("BTC-{}-{}", strike.round() as i64, option_type.to_uppercase());
    let url = format!("https://test.deribit.com/api/v2/public/ticker?instrument_name={}", instrument);
    
    match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), client.get(&url).send()).await {
        Ok(Ok(response)) => {
            if !response.status().is_success() {
                return Err(PulserError::ApiError(format!("Deribit API error: {}", response.status())));
            }
            match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), response.json::<Value>()).await {
                Ok(Ok(json)) => {
                    let delta = json["result"]["greeks"]["delta"].as_f64()
                        .ok_or_else(|| PulserError::PriceFeedError("Missing delta value".to_string()))?;
                    let gamma = json["result"]["greeks"]["gamma"].as_f64()
                        .ok_or_else(|| PulserError::PriceFeedError("Missing gamma value".to_string()))?;
                    let vega = json["result"]["greeks"]["vega"].as_f64()
                        .ok_or_else(|| PulserError::PriceFeedError("Missing vega value".to_string()))?;
                    let theta = json["result"]["greeks"]["theta"].as_f64()
                        .ok_or_else(|| PulserError::PriceFeedError("Missing theta value".to_string()))?;
                    
                    Ok((delta, gamma, vega, theta))
                }
                Ok(Err(e)) => Err(PulserError::ApiError(format!("Failed to parse Deribit option response: {}", e))),
                Err(_) => Err(PulserError::ApiError("Timeout parsing Deribit option response".to_string())),
            }
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Deribit option request failed: {}", e))),
        Err(_) => Err(PulserError::NetworkError("Deribit option request timed out".to_string())),
    }
}
