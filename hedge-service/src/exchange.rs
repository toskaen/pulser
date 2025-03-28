// hedging-service/src/exchange.rs
use reqwest::Client;
use serde_json::json;
use common::error::PulserError;
use log::{info, warn};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Duration;

pub struct ExchangeClient {
    client: Client,
    deribit_api_key: String,
    deribit_secret: String,
}

impl ExchangeClient {
    pub fn new(deribit_api_key: String, deribit_secret: String) -> Self {
        ExchangeClient {
            client: Client::new(),
            deribit_api_key,
            deribit_secret,
        }
    }
    
    pub async fn get_user_balance(&self, user_id: &str) -> Result<f64, PulserError> {
    let auth = format!("Bearer {}", self.deribit_api_key);
    let resp = self.client.get("https://test.deribit.com/api/v2/private/get_account_summary")
        .header("Authorization", auth)
        .query(&[("currency", "BTC")])
        .send().await?
        .json::<serde_json::Value>().await?;
    let balance = resp["result"]["available_funds"].as_f64()
        .ok_or(PulserError::ApiError("No balance data".to_string()))?;
    info!("Fetched balance for {}: {:.6} BTC", user_id, balance);
    Ok(balance)
}

    pub async fn place_limit_short(&self, amount: f64, price: f64, user_id: &str) -> Result<String, PulserError> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis().to_string();
        let order_id = format!("{}-{}", user_id, nonce);
        let auth = format!("Bearer {}", self.deribit_api_key);
        let max_attempts = 3;

        for attempt in 0..max_attempts {
            let adjusted_price = price * (1.0 - (attempt as f64 * 0.00021)); // 0.021% tweak
            let payload = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "private/sell",
                "params": {
                    "instrument_name": "BTC-PERPETUAL",
                    "amount": amount,
                    "type": "limit",
                    "price": adjusted_price,
                    "label": order_id.clone()
                }
            });

            info!("Attempting limit short for {}: {:.6} BTC at ${:.2} (Attempt {}/{})", user_id, amount, adjusted_price, attempt + 1, max_attempts);
            let resp = self.client.post("https://test.deribit.com/api/v2/private/sell")
                .header("Authorization", auth.clone())
                .json(&payload)
                .send().await?
                .json::<serde_json::Value>().await?;

            if resp["result"]["order"]["order_state"] == "filled" {
                info!("Limit short filled for {}: {:.6} BTC at ${:.2}", user_id, amount, adjusted_price);
                return Ok(order_id);
            }

            if attempt < max_attempts - 1 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }

        warn!("Limit short failed after {} attempts for {}, falling back to market", max_attempts, user_id);
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "private/sell",
            "params": {
                "instrument_name": "BTC-PERPETUAL",
                "amount": amount,
                "type": "market",
                "label": order_id.clone()
            }
        });
        let resp = self.client.post("https://test.deribit.com/api/v2/private/sell")
            .header("Authorization", auth)
            .json(&payload)
            .send().await?
            .json::<serde_json::Value>().await?;

        if resp["result"]["order"]["order_state"] == "filled" {
            let fill_price = resp["result"]["order"]["average_price"].as_f64().unwrap_or(price);
            info!("Market short filled for {}: {:.6} BTC at ${:.2}", user_id, amount, fill_price);
            Ok(order_id)
        } else {
            Err(PulserError::ApiError("Market fallback failed".to_string()))
        }
    }

    pub async fn get_funding_rate(&self) -> Result<f64, PulserError> {
        let resp = self.client.get("https://test.deribit.com/api/v2/public/get_funding_rate_history")
            .query(&[("instrument_name", "BTC-PERPETUAL"), ("count", "1")])
            .send().await?
            .json::<serde_json::Value>().await?;
        Ok(resp["result"][0]["funding_rate"].as_f64().unwrap_or(0.0))
    }

    pub async fn get_implied_volatility(&self) -> Result<f64, PulserError> {
        let resp = self.client.get("https://test.deribit.com/api/v2/public/get_instruments")
            .query(&[("currency", "BTC"), ("kind", "option")])
            .send().await?
            .json::<serde_json::Value>().await?;
        Ok(resp["result"].as_array().unwrap().iter()
            .filter(|i| i["expiration_timestamp"].as_i64().unwrap() > chrono::Utc::now().timestamp_millis())
            .map(|i| i["implied_volatility"].as_f64().unwrap_or(0.0))
            .sum::<f64>() / 10.0)
    }

    pub async fn get_depth(&self) -> Result<f64, PulserError> {
        let resp = self.client.get("https://test.deribit.com/api/v2/public/get_book_summary_by_instrument")
            .query(&[("instrument_name", "BTC-PERPETUAL")])
            .send().await?
            .json::<serde_json::Value>().await?;
        Ok(resp["result"][0]["volume"].as_f64().unwrap_or(1.0))
    }
}

    pub async fn get_historical_volatility(&self) -> Result<f64, PulserError> {
        let resp = self.client.get("https://test.deribit.com/api/v2/public/get_historical_volatility")
            .query(&[("currency", "BTC"), ("period", "21d")])
            .send().await?
            .json::<serde_json::Value>().await?;
        let vol_data = resp["result"].as_array()
            .ok_or(PulserError::ApiError("Invalid vol data".to_string()))?;
        let latest_vol = vol_data.last()
            .and_then(|v| v["volatility"].as_f64())
            .unwrap_or(1.0); // Fallback to 1% if API fails
        info!("Fetched 21-day BTC volatility: {:.2}%", latest_vol * 100.0);
        Ok(latest_vol) // Returns annualized vol as decimal (e.g., 0.015 for 1.5%)
    }
