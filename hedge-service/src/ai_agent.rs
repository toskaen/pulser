// hedging-service/src/ai_agent.rs
use crate::hedging::HedgeManager;
use common::error::PulserError;
use common::price_feed::PriceFeed;
use log::{info, warn};
use std::sync::Arc;
use tokio::sync::Mutex;
use statrs::statistics::Data;
use rand::Rng; // Placeholder for ML; replace with actual ML crate
use chrono::Utc;

pub struct AiAgent {
    hedge_manager: Arc<Mutex<HedgeManager>>,
    model: RandomForestModel, // Placeholder struct
    retrain_interval_secs: u64,
}

struct RandomForestModel {
    // Simplified placeholder; real implementation would use an ML library (e.g., rustlearn)
    weights: Vec<f64>,
}

impl AiAgent {
    pub fn new(hedge_manager: Arc<Mutex<HedgeManager>>) -> Self {
        AiAgent {
            hedge_manager,
            model: RandomForestModel {
                weights: vec![0.3, 0.2, 0.15, 0.15, 0.1, 0.1], // Dummy weights for features
            },
            retrain_interval_secs: 30 * 24 * 60 * 60, // Monthly retraining (30 days)
            
// TODO: WEEKLY RETRAINING INSTEAD ALIGNED WITH 7DMA/21DMA/MONTHLY OPTIONS EXPIRATIONS?

        }
    }

// In ai_agent.rs, start
pub async fn start(&self, mut shutdown_rx: tokio::sync::broadcast::Receiver<()>, mut deposit_rx: tokio::sync::broadcast::Receiver<String>) -> Result<(), PulserError> {
    let mut check_interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => { break; }
            user_id = deposit_rx.recv() => { self.adjust_hedging_for_user(&user_id).await?; }
            _ = check_interval.tick() => { self.adjust_hedging().await?; }
        }
    }
    Ok(())
}

async fn adjust_hedging_for_user(&self, user_id: &str) -> Result<(), PulserError> {
    let mut manager = self.hedge_manager.lock().await;
    let stablechain: StableChain = manager.state_manager.load(&PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id))).await?;
    let total_btc = stablechain.accumulated_btc.to_btc();
    let probability = self.predict_drop(user_id).await?;
    if probability > 0.6 {
        manager.add_short(user_id, total_btc * 0.2, manager.price_feed.get_latest_price().await?, Utc::now().timestamp() as u64, "AI Deposit").await?;
    }
    Ok(())
}

    async fn retrain_model(&self) -> Result<(), PulserError> {
        let manager = self.hedge_manager.lock().await;
        let prices = manager.price_feed.get_historical_prices(365).await?; // 1 year of data
        let returns: Vec<f64> = prices.windows(2).map(|w| (w[1] / w[0]).ln()).collect();
        let mut data = Data::new(returns.clone());

        // Placeholder: Simulate training on price drops >10%
        let labels: Vec<bool> = returns.windows(7).map(|w| w.iter().sum::<f64>() < -0.1).collect();
        info!("Retraining AI model with {} days of data", prices.len());
        // Real ML would update self.model.weights here using a library like rustlearn or smartcore
        Ok(())
    }

    async fn predict_drop(&self, user_id: &str) -> Result<f64, PulserError> {
        let manager = self.hedge_manager.lock().await;
        let current_price = manager.price_feed.get_latest_price().await?;
        let iv = manager.exchange_client.get_implied_volatility().await?;
        let ma_7d = manager.price_feed.get_moving_average(7).await?;
        let ma_21d = manager.price_feed.get_moving_average(21).await?;
        let vol = manager.exchange_client.get_historical_volatility().await?;
        let spreads = manager.exchange_client.fetch_spread_matrix("BTC").await?;
        let spread = spreads.get("BTC-PERPETUAL_BTC-27JUN25").copied().unwrap_or(0.0);

        // Features: [ma_drop_7d, ma_drop_21d, iv, vol, spread, price_change]
        let ma_drop_7d = (ma_7d - current_price) / ma_7d;
        let ma_drop_21d = (ma_21d - current_price) / ma_21d;
        let price_change = (current_price - manager.state.shorts.iter()
            .filter(|p| p.user_id == user_id)
            .map(|p| p.entry_price)
            .next()
            .unwrap_or(current_price)) / current_price;
        let features = vec![ma_drop_7d, ma_drop_21d, iv / 100.0, vol, spread / current_price, price_change];

        // Dummy prediction: Weighted sum (replace with real Random Forest)
        let probability: f64 = features.iter()
            .zip(self.model.weights.iter())
            .map(|(f, w)| f * w)
            .sum::<f64>()
            .max(0.0)
            .min(1.0);

        info!("Predicted drop probability for {}: {:.2}%", user_id, probability * 100.0);
        Ok(probability)
    }

    async fn adjust_hedging(&self) -> Result<(), PulserError> {
        let mut manager = self.hedge_manager.lock().await;
        let users: Vec<String> = manager.state.shorts.iter()
            .map(|p| p.user_id.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        for user_id in users {
            let probability = self.predict_drop(&user_id).await?;
            let current_price = manager.price_feed.get_latest_price().await?;
            let total_btc = manager.state.shorts.iter()
                .filter(|p| p.user_id == user_id)
                .map(|p| p.amount_btc)
                .sum::<f64>()
                .max(10.0); // Assume 10 BTC if no data
            let current_hedge = manager.state.shorts.iter()
                .filter(|p| p.user_id == user_id)
                .map(|p| p.amount_btc)
                .sum::<f64>();
            let max_hedge_cap = manager.config.get("max_hedge_cap_base")
                .and_then(|v| v.as_float())
                .unwrap_or(3.0);

            if probability > 0.6 && current_hedge < total_btc * max_hedge_cap {
                // High probability: Increase hedge with combo or options
                let spreads = manager.exchange_client.fetch_spread_matrix("BTC").await?;
                if let Some(spread_price) = spreads.get("BTC-PERPETUAL_BTC-27JUN25") {
                    if spread_price.abs() > 0.02 * current_price {
                        manager.exchange_client.place_combo_order(
                            "BTC-PERPETUAL",
                            "BTC-27JUN25",
                            total_btc * 0.2,
                            *spread_price,
                            &user_id,
                        ).await?;
                        info!("AI: Combo hedge for {}: {:.6} BTC", user_id, total_btc * 0.2);
                        continue;
                    }
                }

                // Fallback to option or short
                let strike_price = current_price * 0.8; // 80% strike as example
                let option_cost = manager.price_feed.get_option_price(strike_price, "PUT").await?;
                if manager.check_collateralization(user_id, current_price).0 && manager.state.collateral.get(user_id).unwrap_or(&0.0) > &(option_cost * total_btc) {
                    manager.exchange_client.place_option_buy(total_btc, strike_price, user_id).await?;
                    info!("AI: Option hedge for {}: {:.6} BTC puts at ${:.2}", user_id, total_btc, strike_price);
                } else {
                    manager.add_short(user_id, total_btc * 0.2, current_price, Utc::now().timestamp() as u64, "AI Predict").await?;
                }
            } else if probability < 0.4 && current_hedge > total_btc * 0.21 {
                // Low probability: Reduce hedge
                let reduce_amount = current_hedge * 0.15;
                manager.reduce_hedge(user_id, reduce_amount, current_price).await?;
                info!("AI: Reduced hedge for {}: {:.6} BTC", user_id, reduce_amount);
            }
        }
        Ok(())
    }
}

impl RandomForestModel {
    // Placeholder; real ML would use a library
    fn predict(&self, _features: Vec<f64>) -> f64 {
        0.5 // Dummy return
    }
}
