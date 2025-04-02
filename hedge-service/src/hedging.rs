use crate::state::HedgeState;
use crate::exchange::ExchangeClient;
use common::price_feed::PriceFeed;
use common::StateManager;
use common::{StableChain, Bitcoin, HedgePosition};
use common::error::PulserError;
use log::{info, warn};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use statrs::statistics::{Data, Distribution};
use chrono::{Utc, Weekday};

pub struct HedgeManager {
    pub price_feed: PriceFeed,
    pub state_manager: StateManager,
    pub exchange_client: ExchangeClient,
    pub state: HedgeState,
    pub config: HashMap<String, toml::Value>, // Configuration loaded from TOML
}

impl HedgeManager {
    pub async fn new(data_dir: &str) -> Result<Self, PulserError> {
        let config_str = std::fs::read_to_string("config/service_config.toml")?;
        let config: toml::Value = toml::from_str(&config_str)?;
        let config_map = config.as_table().ok_or(PulserError::ConfigError("Invalid config format".to_string()))?.clone();
        let state_manager = StateManager::new(data_dir);
        let state = state_manager.load("hedge_state.json").await.unwrap_or(HedgeState {
            shorts: Vec::new(),
            rainy_fund: 0.0,
            last_hedge_time: Default::default(),
            user_pnl: Default::default(),
            collateral: Default::default(),
        });

        Ok(HedgeManager {
            price_feed: PriceFeed::new(),
            state_manager,
            exchange_client: ExchangeClient::new(
                config_map["deribit_id"].as_str().unwrap_or("").to_string(),
                config_map["deribit_secret"].as_str().unwrap_or("").to_string(),
            ),
            state,
            config: config_map.into_iter().collect(),
        })
    }

    async fn forecast_volatility(&self) -> Result<f64, PulserError> {
        let prices = self.price_feed.get_historical_prices(21).await?;
        let returns: Vec<f64> = prices.windows(2).map(|w| (w[1] / w[0]).ln()).collect();
        let mut data = Data::new(returns);
        let variance = data.variance().unwrap_or(0.01);
        let last_vol = self.exchange_client.get_historical_volatility().await?;
        let long_run_vol = (variance * 252.0).sqrt();
        Ok(0.05 * long_run_vol + 0.9 * last_vol + 0.05 * variance.sqrt())
    }

    pub async fn hedge_stablechain(&mut self, user_id: &str) -> Result<(), PulserError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        if now - self.state.last_hedge_time.get(user_id).unwrap_or(&0) < 600 {
            return Ok(());
        }

                    let stablechain_path = PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id));
            let stablechain: StableChain = manager.state_manager.load(&stablechain_path).await?; // Reload every tim
        let total_btc = stablechain.accumulated_btc.to_btc();
        let target_usd = stablechain.stabilized_usd.0;
        let initial_price = stablechain.initial_price;

        let current_price = self.price_feed.get_latest_price().await?;
        let collateral = self.exchange_client.get_user_balance(user_id).await?;
        self.state.collateral.insert(user_id.to_string(), collateral);
        let (funding_rate, vol, iv, depth) = tokio::try_join!(
            self.exchange_client.get_funding_rate(),
            self.exchange_client.get_historical_volatility(),
            self.exchange_client.get_implied_volatility(),
            self.exchange_client.get_depth()
        )?;

        let (ma_7d, ma_21d, std_dev_21d, volume_mean) = tokio::try_join!(
            self.price_feed.get_moving_average(7),
            self.price_feed.get_moving_average(21),
            self.price_feed.get_standard_deviation(21),
            self.price_feed.get_volume_average_historical(7, 30) // 30-day historical mean
        )?;
        let upper_band = ma_21d + 2.0 * std_dev_21d;
        let lower_band = ma_21d - 2.0 * std_dev_21d;
        let ma_drop_7d = (ma_7d - current_price) / ma_7d;
        let ma_drop_21d = (ma_21d - current_price) / ma_21d;
        let drop_percent = (initial_price - current_price) / initial_price;

        let current_hedge = self.state.shorts.iter()
            .filter(|p| p.user_id == user_id)
            .map(|p| p.amount_btc)
            .sum::<f64>();
        let (is_collateralized, _) = self.check_collateralization(user_id, current_price);

        // Load config values
        let base_short_percent = self.config.get("base_short_percent").and_then(|v| v.as_float()).unwrap_or(0.21);
        let trigger_step_base = self.config.get("trigger_step_base").and_then(|v| v.as_float()).unwrap_or(0.03);
        let max_hedge_cap_base = self.config.get("max_hedge_cap_base").and_then(|v| v.as_float()).unwrap_or(3.0);
        let target_volatility = self.config.get("target_volatility").and_then(|v| v.as_float()).unwrap_or(0.15);
        let iv_high_threshold = self.config.get("iv_high_threshold").and_then(|v| v.as_float()).unwrap_or(80.0);
        let drop_urgency_threshold = self.config.get("drop_urgency_threshold").and_then(|v| v.as_float()).unwrap_or(0.3);
        let ma_drop_7d_threshold = self.config.get("ma_drop_7d_threshold").and_then(|v| v.as_float()).unwrap_or(0.02);
        let ma_drop_21d_threshold = self.config.get("ma_drop_21d_threshold").and_then(|v| v.as_float()).unwrap_or(0.05);
        let vol_adjust_lower = self.config.get("vol_adjust_lower").and_then(|v| v.as_float()).unwrap_or(0.9);
        let vol_adjust_upper = self.config.get("vol_adjust_upper").and_then(|v| v.as_float()).unwrap_or(1.1);
        let hedge_reduction_factor = self.config.get("hedge_reduction_factor").and_then(|v| v.as_float()).unwrap_or(0.15);
        let hedge_increase_factor = self.config.get("hedge_increase_factor").and_then(|v| v.as_float()).unwrap_or(0.20);
        let depth_threshold = self.config.get("depth_threshold").and_then(|v| v.as_float()).unwrap_or(2.0);
        let volume_increase_factor = self.config.get("volume_increase_factor").and_then(|v| v.as_float()).unwrap_or(1.2);

        // Dynamic adjustments
        let forecast_vol = self.forecast_volatility().await?;
        let vol_factor = forecast_vol / target_volatility;
        let trigger_step = (trigger_step_base * vol_factor).min(0.09).max(0.015);
        let max_drawdown = self.price_feed.get_max_drawdown(365).await.unwrap_or(0.8); // Default to 80%
        let max_hedge_cap = (1.0 / (1.0 - max_drawdown)).max(max_hedge_cap_base);

let spreads = self.exchange_client.fetch_spread_matrix("BTC").await?;
if let Some(spread_price) = spreads.get("BTC-PERPETUAL_BTC-27JUN25") {
    if spread_price.abs() > 0.02 * current_price && current_hedge < total_btc * max_hedge_cap {
        self.exchange_client.place_combo_order("BTC-PERPETUAL", "BTC-27JUN25", total_btc * base_short_percent, *spread_price, user_id).await?;
        info!("Combo hedge for {}: {:.6} BTC spread at ${:.2}", user_id, total_btc * base_short_percent, spread_price);
    }
}

        // Base hedge
        if current_hedge < total_btc * base_short_percent && is_collateralized {
            let hedge_amount = (total_btc * base_short_percent - current_hedge).max(0.0) * (depth / depth_threshold).min(1.0);
            self.add_short(user_id, hedge_amount, current_price, now, "Base").await?;
        }

        // Dynamic Delta Hedging
        if ma_drop_7d > ma_drop_7d_threshold * vol_factor && current_hedge < total_btc * max_hedge_cap && is_collateralized {
            let hedge_amount = total_btc * trigger_step * ma_drop_7d * (depth / depth_threshold).min(1.0);
            self.add_short(user_id, hedge_amount, current_price, now, "Dynamic 7d").await?;
        }
        if ma_drop_21d > ma_drop_21d_threshold * vol_factor && current_hedge < total_btc * max_hedge_cap && is_collateralized {
            let hedge_amount = total_btc * trigger_step * 2.0 * ma_drop_21d * (depth / depth_threshold).min(1.0);
            self.add_short(user_id, hedge_amount, current_price, now, "Dynamic 21d").await?;
        }

        // IV Preemption
        if iv > iv_high_threshold && current_hedge < total_btc * max_hedge_cap && is_collateralized {
            let hedge_amount = 0.5 * (depth / depth_threshold).min(1.0);
            self.add_short(user_id, hedge_amount, current_price, now, "IV Preempt").await?;
        }

        // Options Laddering with Dynamic Costs
let is_weekend = Utc::now().weekday() == Weekday::Sat || Utc::now().weekday() == Weekday::Sun;
let options_strikes = self.config.get("options_strikes").and_then(|v| v.as_array())
    .map(|arr| arr.iter().filter_map(|v| v.as_float()).collect::<Vec<f64>>())
    .unwrap_or(vec![0.9, 0.8, 0.7, 0.6, 0.5]);
for (i, &strike_percent) in options_strikes.iter().enumerate() {
    let threshold = 0.1 * (i + 1) as f64;
    let should_buy = drop_percent > threshold && is_collateralized;
    let is_urgent = drop_percent > drop_urgency_threshold;
if should_buy && (is_weekend || is_urgent) && iv < iv_high_threshold {
    let strike_price = current_price * strike_percent;
    let option_cost = self.price_feed.get_option_price(strike_price, "PUT").await? * if is_weekend { 0.9 } else { 1.0 };
    if collateral > option_cost * total_btc {
        let order_id = self.exchange_client.place_option_buy(total_btc, strike_price, user_id).await?; // Pass option_cost if updated
        info!("Options ladder for {}: {:.6} BTC puts at ${:.2} (urgent: {})", user_id, total_btc, strike_price, is_urgent);
    }
}

        // Volatility Targeting
        if forecast_vol < target_volatility * vol_adjust_lower && current_hedge > total_btc * base_short_percent {
            let reduce_amount = current_hedge * hedge_reduction_factor;
            self.reduce_hedge(user_id, reduce_amount, current_price).await?;
            info!("Volatility reduced hedge for {}: {:.6} BTC", user_id, reduce_amount);
        } else if forecast_vol > target_volatility * vol_adjust_upper && current_hedge < total_btc * max_hedge_cap {
            let increase_amount = total_btc * hedge_increase_factor * (depth / depth_threshold).min(1.0);
            self.add_short(user_id, increase_amount, current_price, now, "Volatility").await?;
        }

        // Mean Reversion
        if current_price < lower_band && current_hedge < total_btc * max_hedge_cap && is_collateralized {
            let hedge_amount = total_btc * hedge_increase_factor * (depth / depth_threshold).min(1.0);
            self.add_short(user_id, hedge_amount, current_price, now, "Mean Reversion").await?;
        } else if current_price > upper_band && current_hedge > total_btc * base_short_percent {
            let reduce_amount = current_hedge * hedge_reduction_factor;
            self.reduce_hedge(user_id, reduce_amount, current_price).await?;
            info!("Mean reversion reduced hedge for {}: {:.6} BTC", user_id, reduce_amount);
        }

        // Momentum (Fixed Bug)
        let volume_avg = self.price_feed.get_volume_average(7).await?;
        if current_price > ma_21d && depth > depth_threshold && volume_avg > volume_increase_factor * volume_mean && current_hedge > total_btc * base_short_percent {
            let reduce_amount = current_hedge * hedge_reduction_factor;
            self.reduce_hedge(user_id, reduce_amount, current_price).await?;
            info!("Momentum reduced hedge for {}: {:.6} BTC", user_id, reduce_amount);
        }

        // Breakouts
        if iv > iv_high_threshold && current_price < ma_21d && current_hedge < total_btc * max_hedge_cap && is_collateralized {
            let hedge_amount = total_btc * hedge_increase_factor * (depth / depth_threshold).min(1.0);
            self.add_short(user_id, hedge_amount, current_price, now, "Breakout").await?;
        }

        // Harvest and Float Management
        let pnl = self.calc_pnl(user_id, current_price);
        let total_hedge = self.state.shorts.iter().map(|p| p.amount_btc).sum::<f64>();
        let harvest_pnl_threshold = self.config.get("harvest_pnl_threshold").and_then(|v| v.as_float()).unwrap_or(0.15);
        if pnl > target_usd * harvest_pnl_threshold && depth >= depth_threshold {
            let harvest_amount = total_hedge * 0.5;
            self.reduce_hedge(user_id, harvest_amount, current_price).await?;
            self.state.rainy_fund += harvest_amount;
            info!("Harvested for {}: {:.6} BTC to rainy fund", user_id, harvest_amount);
        }

        let stop_loss_factor = self.config.get("stop_loss_factor").and_then(|v| v.as_float()).unwrap_or(1.1);
        if current_price > initial_price * stop_loss_factor && current_hedge > 0.0 {
            let reduce_amount = current_hedge * 0.25;
            self.reduce_hedge(user_id, reduce_amount, current_price).await?;
            info!("Stop-loss reduced hedge for {}: {:.6} BTC", user_id, reduce_amount);
        }

        let float_sweep_threshold = self.config.get("float_sweep_threshold").and_then(|v| v.as_float()).unwrap_or(0.03);
        let float_usd = total_btc * current_price - target_usd - pnl;
        if float_usd > float_sweep_threshold * current_price {
            let sweep = float_usd * 0.25 / current_price;
            self.state.rainy_fund += sweep;
            info!("Float sweep for {}: {:.6} BTC to rainy fund", user_id, sweep);
        }

        self.state.user_pnl.insert(user_id.to_string(), pnl);
        self.state.last_hedge_time.insert(user_id.to_string(), now);
        self.state_manager.save("hedge_state.json", &self.state).await?;
        Ok(())
    }

    async fn add_short(&mut self, user_id: &str, amount: f64, price: f64, timestamp: u64, label: &str) -> Result<(), PulserError> {
        if amount > 0.0 {
            let order_id = self.exchange_client.place_limit_short(amount, price, user_id).await?;
            self.state.shorts.push(HedgePosition {
                entry_price: price,
                amount_btc: amount,
                order_id,
                user_id: user_id.to_string(),
                timestamp,
            });
            info!("{} hedge for {}: {:.6} BTC at ${:.2}", label, user_id, amount, price);
        }
        Ok(())
    }

    async fn reduce_hedge(&mut self, user_id: &str, amount: f64, price: f64) -> Result<(), PulserError> {
        let mut remaining = amount;
        self.state.shorts.retain(|p| {
            if p.user_id == user_id && remaining > 0.0 {
                remaining -= p.amount_btc.min(remaining);
                false
            } else {
                true
            }
        });
        if amount > 0.0 {
            self.exchange_client.close_short_position(amount, price, user_id).await?;
        }
        Ok(())
    }

    fn check_collateralization(&self, user_id: &str, price: f64) -> (bool, f64) {
        let user_shorts = self.state.shorts.iter()
            .filter(|p| p.user_id == user_id)
            .map(|p| p.amount_btc * price)
            .sum::<f64>();
        let collateral = self.state.collateral.get(user_id).unwrap_or(&0.0);
            let stablechain: StableChain = self.state_manager.load(&PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id))).await.unwrap_or_default();
 let required_collateral = user_shorts * 0.05; // Deribit 5% maintenance
    if *collateral < required_collateral {
        // Reduce shorts to match available collateral
        let excess = (required_collateral - *collateral) / price;
        self.reduce_hedge(user_id, excess, price).await.ok();
    }
    let ratio = if user_shorts > 0.0 { *collateral / user_shorts } else { f64::INFINITY };
    (ratio >= 1.2, ratio) // 120% margin requirement
}

    pub fn calc_pnl(&self, user_id: &str, price: f64) -> f64 {
        self.state.shorts.iter()
            .filter(|p| p.user_id == user_id)
            .map(|p| (p.entry_price - price) * p.amount_btc - p.amount_btc * p.entry_price * 0.0005)
            .sum()
    }
}
