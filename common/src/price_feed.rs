// common/src/price_feed.rs
use crate::error::PulserError;
use crate::types::PriceInfo;
use crate::utils::now_timestamp;
use reqwest::Client;
use std::collections::HashMap;
use log::{debug, warn};
use serde_json::Value;
use std::time::Duration;

/// Fetch BTC price from multiple sources
pub async fn fetch_btc_price(client: &Client) -> Result<PriceInfo, PulserError> {
    let sources = vec![
        ("Coingecko", "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd", "bitcoin.usd"),
        ("Kraken", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD", "result.XXBTZUSD.c.0"),
        ("Binance", "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT", "price"),
    ];
    
    let mut prices = HashMap::new();
    let mut btc_prices = Vec::new();
    
    // Fetch from all sources
    for (source_name, url, path) in sources {
        match fetch_from_source(client, url, path).await {
            Ok(price) => {
                debug!("BTC-USD ({}): ${:.2}", source_name, price);
                prices.insert(source_name.to_string(), price);
                btc_prices.push(price);
            },
            Err(e) => warn!("Failed to fetch from {}: {}", source_name, e),
        }
    }
    
    if btc_prices.is_empty() {
        return Err(PulserError::PriceFeedError("All price sources failed".to_string()));
    }
    
    // Use median price for robustness
btc_prices.sort_by(|a: &f64, b: &f64| a.partial_cmp(b).unwrap());
    let raw_btc_usd = if btc_prices.len() % 2 == 0 {
        (btc_prices[btc_prices.len()/2 - 1] + btc_prices[btc_prices.len()/2]) / 2.0
    } else {
        btc_prices[btc_prices.len()/2]
    };
    
    // Add the price to our price feeds
    prices.insert("BTC-USD".to_string(), raw_btc_usd);
    
    // Return price info
    Ok(PriceInfo {
        raw_btc_usd,
        synthetic_price: None, // Will be calculated separately
        timestamp: now_timestamp(),
        price_feeds: prices,
    })
}

/// Fetch price from a specific source
async fn fetch_from_source(client: &Client, url: &str, path: &str) -> Result<f64, PulserError> {
    // Make request
    let response = client.get(url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| PulserError::NetworkError(format!("Request failed: {}", e)))?;
    
    if !response.status().is_success() {
        return Err(PulserError::ApiError(format!("API error: {}", response.status())));
    }
    
    // Parse JSON
    let json: Value = response.json().await
        .map_err(|e| PulserError::ApiError(format!("JSON parse failed: {}", e)))?;
    
    // Extract price using path
    let mut value = &json;
    for part in path.split('.') {
        match value.get(part) {
            Some(v) => value = v,
            None => return Err(PulserError::ApiError(format!("Missing field: {}", part))),
        }
    }
    
    // Convert to f64
    match value {
        Value::String(s) => s.parse::<f64>()
            .map_err(|_| PulserError::ApiError("Failed to parse price".to_string())),
        Value::Number(n) => n.as_f64()
            .ok_or_else(|| PulserError::ApiError("Failed to convert to f64".to_string())),
        _ => Err(PulserError::ApiError("Unexpected value type".to_string())),
    }
}

/// Calculate synthetic price based on various indicators
pub fn calculate_synthetic_price(prices: HashMap<String, f64>) -> Result<f64, PulserError> {
    // Get raw BTC price
    let raw_btc_usd = match prices.get("BTC-USD") {
        Some(price) => *price,
        None => return Err(PulserError::PriceFeedError("Missing BTC-USD price".to_string())),
    };
    
    // For now, this is a simplified version - you can replace with your full algorithm later
    // Apply a small adjustment for demonstration
    let synthetic_price = raw_btc_usd * 0.99; // 1% discount
    
    Ok(synthetic_price)
}

/// Fetch network fee estimates
pub async fn fetch_network_fee(client: &Client) -> Result<u64, PulserError> {
    let url = "https://mempool.space/api/v1/fees/recommended";
    
    let response = client.get(url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| PulserError::NetworkError(format!("Fee request failed: {}", e)))?;
    
    let json: Value = response.json()
        .await
        .map_err(|e| PulserError::ApiError(format!("Fee JSON parsing failed: {}", e)))?;
    
    json["fastestFee"].as_u64()
        .ok_or_else(|| PulserError::PriceFeedError("Missing fee data".to_string()))
}
