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

// In hedging-service/src/exchange.rs, modify place_limit_short
pub async fn place_limit_short(&self, amount: f64, price: f64, user_id: &str) -> Result<String, PulserError> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis().to_string();
    let order_id = format!("{}-{}", user_id, nonce);
    let auth = format!("Bearer {}", self.deribit_api_key);
    let max_attempts = 3;

    // Check spread matrix for combo opportunity
    let spreads = self.fetch_spread_matrix("BTC").await?;
    if let Some(spread_price) = spreads.get("BTC-PERPETUAL_BTC-27JUN25") {
        if spread_price.abs() > 0.02 * price { // Spread > 2% of price
            return self.place_combo_order("BTC-PERPETUAL", "BTC-27JUN25", amount, *spread_price, user_id).await;
        }
    }

    // Fallback to single-leg short
    for attempt in 0..max_attempts {
        let adjusted_price = price * (1.0 - (attempt as f64 * 0.00021));
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
    
// In hedging-service/src/exchange.rs, replace place_option_buy
pub async fn place_option_buy(&self, amount: f64, strike_price: f64, user_id: &str, option_price: Option<f64>) -> Result<String, PulserError> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis().to_string();
    let order_id = format!("{}-opt-{}", user_id, nonce);
    let auth = format!("Bearer {}", self.deribit_api_key);
    let expiry = chrono::Utc::now()
        .checked_add_signed(chrono::Duration::days(7 - chrono::Utc::now().weekday().num_days_from_monday() as i64))
        .unwrap()
        .format("%d%b%y")
        .to_string()
        .to_uppercase();
    let instrument = format!("BTC-{}-{}-P", expiry, strike_price.round() as i64);
    let price = option_price.unwrap_or(strike_price * 0.02); // Fallback if not provided

    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "private/buy",
        "params": {
            "instrument_name": instrument,
            "amount": amount,
            "type": "limit",
            "price": price,
            "label": order_id.clone()
        }
    });

    let resp = self.client.post("https://test.deribit.com/api/v2/private/buy")
        .header("Authorization", auth)
        .json(&payload)
        .send().await?
        .json::<serde_json::Value>().await?;

    if resp["result"]["order"]["order_state"] == "filled" {
        info!("Option buy filled for {}: {:.6} BTC puts at ${:.2}", user_id, amount, strike_price);
        Ok(order_id)
    } else {
        warn!("Option buy failed for {}", user_id);
        Err(PulserError::ApiError("Option buy failed".to_string()))
    }
}

        let resp = self.client.post("https://test.deribit.com/api/v2/private/buy")
            .header("Authorization", auth)
            .json(&payload)
            .send().await?
            .json::<serde_json::Value>().await?;

        if resp["result"]["order"]["order_state"] == "filled" {
            info!("Option buy filled for {}: {:.6} BTC puts at ${:.2}", user_id, amount, strike_price);
            Ok(order_id)
        } else {
            warn!("Option buy failed for {}", user_id);
            Err(PulserError::ApiError("Option buy failed".to_string()))
        }
    
    // In hedging-service/src/exchange.rs, add to ExchangeClient impl
pub async fn fetch_spread_matrix(&self, currency: &str) -> Result<HashMap<String, f64>, PulserError> {
    let resp = self.client.get("https://test.deribit.com/api/v2/public/get_instruments")
        .query(&[("currency", currency), ("kind", "future_combo")])
        .send().await?
        .json::<serde_json::Value>().await?;

    let mut spreads = HashMap::new();
    if let Some(instruments) = resp["result"].as_array() {
        for instr in instruments {
            let combo_name = instr["instrument_name"].as_str().unwrap_or("").to_string();
            if let Some(last_price) = instr["last_price"].as_f64() {
                spreads.insert(combo_name, last_price);
            }
        }
    }

    info!("Fetched {} BTC futures spreads from matrix", spreads.len());
    Ok(spreads)
}
    
    // In hedging-service/src/exchange.rs, add to ExchangeClient impl
pub async fn place_combo_order(
    &self,
    leg1_instrument: &str, // e.g., "BTC-PERPETUAL"
    leg2_instrument: &str, // e.g., "BTC-27JUN25"
    amount: f64,
    price: f64,
    user_id: &str,
) -> Result<String, PulserError> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis().to_string();
    let order_id = format!("{}-combo-{}", user_id, nonce);
    let auth = format!("Bearer {}", self.deribit_api_key);

    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "private/buy", // "buy" for combo assumes net long; adjust to "sell" if net short
        "params": {
            "instrument_name": format!("{}_{}", leg1_instrument, leg2_instrument), // Simplified combo name
            "amount": amount,
            "type": "limit",
            "price": price, // Net price of the combo
            "label": order_id.clone(),
            "combo": true // Indicates a combo order (Deribit-specific)
        }
    });

    info!("Placing combo order for {}: {:.6} BTC {} vs {} at ${:.2}", user_id, amount, leg1_instrument, leg2_instrument, price);
    let resp = self.client.post("https://test.deribit.com/api/v2/private/buy")
        .header("Authorization", auth)
        .json(&payload)
        .send().await?
        .json::<serde_json::Value>().await?;

    if resp["result"]["order"]["order_state"] == "filled" {
        info!("Combo order filled for {}: {:.6} BTC at ${:.2}", user_id, amount, price);
        Ok(order_id)
    } else {
        warn!("Combo order failed for {}", user_id);
        Err(PulserError::ApiError("Combo order failed".to_string()))
    }
}

    pub async fn close_short_position(&self, amount: f64, price: f64, user_id: &str) -> Result<(), PulserError> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis().to_string();
        let order_id = format!("{}-close-{}", user_id, nonce);
        let auth = format!("Bearer {}", self.deribit_api_key);
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "private/buy",
            "params": {
                "instrument_name": "BTC-PERPETUAL",
                "amount": amount,
                "type": "limit",
                "price": price,
                "label": order_id.clone()
            }
        });

        let resp = self.client.post("https://test.deribit.com/api/v2/private/buy")
            .header("Authorization", auth)
            .json(&payload)
            .send().await?
            .json::<serde_json::Value>().await?;

        if resp["result"]["order"]["order_state"] == "filled" {
            info!("Closed short for {}: {:.6} BTC at ${:.2}", user_id, amount, price);
            Ok(())
        } else {
            Err(PulserError::ApiError("Close short failed".to_string()))
        }
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
