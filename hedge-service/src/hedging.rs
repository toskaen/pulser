// hedging-service/src/hedging.rs
use crate::state::HedgeState;
use crate::exchange::ExchangeClient;
use common::price_feed::PriceFeed;
use common::StateManager;
use common::{StableChain, Bitcoin};
use common::error::PulserError;
use log::{info, warn};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use common::{StableChain, HedgePosition};
use std::path::Path;

pub struct HedgeManager {
    pub price_feed: PriceFeed,
    pub state_manager: StateManager,
    pub exchange_client: ExchangeClient,
    pub state: HedgeState,
    pub base_short_percent: f64,
    pub trigger_step: f64,
    pub max_hedge_cap: f64,
    pub harvest_pnl_threshold: f64,
    pub maintenance_margin_requirement: f64,
}

impl HedgeManager {
    pub async fn new(data_dir: &str) -> Result<Self, PulserError> {
        let config_str = std::fs::read_to_string("config/service_config.toml")?;
        let config: toml::Value = toml::from_str(&config_str)?;
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
                config["deribit_id"].as_str().unwrap_or("").to_string(),
                config["deribit_secret"].as_str().unwrap_or("").to_string(),
            ),
            state,
            base_short_percent: 0.10,  // Default, adjusted dynamically
            trigger_step: 0.015,       // Base, vol-adjusted
            max_hedge_cap: 0.30,       // 30% cap
            harvest_pnl_threshold: 0.05, // 5% PnL trigger
            maintenance_margin_requirement: 1.2, // Collateral ratio
        })
    }

    pub async fn hedge_stablechain(&mut self, user_id: &str) -> Result<(), PulserError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let stablechain_path = PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id));
        let stablechain: StableChain = self.state_manager.load(&stablechain_path).await?;
    let collateral = self.exchange_client.get_user_balance(user_id).await?;
    self.state.collateral.insert(user_id.to_string(), collateral);
        let total_btc = stablechain.accumulated_btc.to_btc();
        let target_usd = stablechain.stabilized_usd.0;
        let current_price = self.price_feed.get_latest_price().await?;
        let current_value = total_btc * current_price;
        let funding_rate = self.exchange_client.get_funding_rate().await?;
let vol = self.exchange_client.get_historical_volatility().await?;
        let iv = self.exchange_client.get_implied_volatility().await?;
        let depth = self.exchange_client.get_depth().await?;

  let total_hedge = self.state.shorts.iter().map(|p| p.amount_btc).sum::<f64>();
    let pool_limit = self.state.collateral.values().sum::<f64>();
    if total_hedge + hedge_amount > pool_limit {
        let prorate = pool_limit / (total_hedge + hedge_amount);
        hedge_amount *= prorate;
        info!("Prorated hedge for {}: {:.6} BTC due to pool limit {:.6}", user_id, hedge_amount, pool_limit);
    }

        // Dynamic adjustments
        let base_short_percent = if funding_rate > 0.1 { 0.05 }
            else if funding_rate < -0.05 { 0.20 } // Arbitrage
            else if funding_rate < 0.0 { 0.15 }
            else { 0.10 };
        let trigger_step = (self.trigger_step * (vol / 1.0)).min(0.05).max(0.01);
        let (is_collateralized, ratio) = self.check_collateralization(user_id, current_price);
        let last_hedge = *self.state.last_hedge_time.get(user_id).unwrap_or(&0);

        // Base hedge + funding arbitrage
        let current_hedge = self.state.shorts.iter().filter(|p| p.user_id == user_id).map(|p| p.amount_btc).sum::<f64>();
        if current_hedge < total_btc * base_short_percent && now - last_hedge > 600 && is_collateralized {
            let hedge_amount = (total_btc * base_short_percent - current_hedge).max(0.0);
            let order_id = self.exchange_client.place_limit_short(hedge_amount, current_price, user_id).await?;
            self.state.shorts.push(HedgePosition {
                entry_price: current_price,
                amount_btc: hedge_amount,
                order_id,
                user_id: user_id.to_string(),
                timestamp: now,
            });
            info!("Base hedge for {}: {:.6} BTC at ${:.2}", user_id, hedge_amount, current_price);
        }

        // Volatility-adjusted hedge
        let last_price = self.state.shorts.iter().filter(|p| p.user_id == user_id).map(|p| p.entry_price).last().unwrap_or(current_price);
        let drop_percent = (last_price - current_price) / last_price;
        if drop_percent >= 0.01 && current_hedge < total_btc * self.max_hedge_cap && now - last_hedge > 600 && is_collateralized {
            let additional_hedge = total_btc * trigger_step * drop_percent * if depth < 1.0 { 0.5 } else { 1.0 };
            let order_id = self.exchange_client.place_limit_short(additional_hedge, current_price, user_id).await?;
            self.state.shorts.push(HedgePosition {
                entry_price: current_price,
                amount_btc: additional_hedge,
                order_id,
                user_id: user_id.to_string(),
                timestamp: now,
            });
            info!("Trend hedge for {}: {:.6} BTC at ${:.2}", user_id, additional_hedge, current_price);
        }

        // IV preemption
        if iv > 80.0 && current_hedge < total_btc * self.max_hedge_cap && now - last_hedge > 600 && is_collateralized {
            let pre_hedge = total_btc * 0.05;
            let order_id = self.exchange_client.place_limit_short(pre_hedge, current_price, user_id).await?;
            self.state.shorts.push(HedgePosition {
                entry_price: current_price,
                amount_btc: pre_hedge,
                order_id,
                user_id: user_id.to_string(),
                timestamp: now,
            });
            info!("IV pre-hedge for {}: {:.6} BTC at ${:.2}", user_id, pre_hedge, current_price);
        }

        // Harvest
        let pnl = self.calc_pnl(user_id, current_price);
        if drop_percent >= 0.05 && pnl > target_usd * self.harvest_pnl_threshold && depth >= 1.0 {
            let to_harvest = self.state.shorts.iter().filter(|p| p.user_id == user_id).map(|p| p.amount_btc).sum::<f64>();
            let harvest_amount = to_harvest * if pnl > target_usd * 0.10 { 0.31 } else { 0.21 };
            self.state.shorts.retain(|p| p.user_id != user_id || p.amount_btc > harvest_amount);
            self.state.rainy_fund += pnl * if pnl > target_usd * 0.10 { 0.31 } else { 0.21 };
            info!("Harvested for {}: PnL ${:.2}, Rainy Fund ${:.2}", user_id, pnl, self.state.rainy_fund);
        }

        // Float sweep
        let float_usd = current_value - target_usd - pnl;
        if float_usd > 3000.0 {
            let sweep = float_usd * 0.25;
            self.state.rainy_fund += sweep;
            info!("Float sweep for {}: ${:.2} to Rainy Fund", user_id, sweep);
        }

        self.state.user_pnl.insert(user_id.to_string(), pnl);
        self.state.last_hedge_time.insert(user_id.to_string(), now);
self.state_manager.save(Path::new("hedge_state.json"), &self.state).await?;
        Ok(())
    }

    fn check_collateralization(&self, user_id: &str, price: f64) -> (bool, f64) {
        let user_shorts = self.state.shorts.iter()
            .filter(|p| p.user_id == user_id)
            .map(|p| p.amount_btc * price)
            .sum::<f64>();
        let collateral = self.state.collateral.get(user_id).unwrap_or(&0.0);
        let ratio = if user_shorts > 0.0 { *collateral / user_shorts } else { f64::INFINITY };
        (ratio >= self.maintenance_margin_requirement, ratio)
    }

    pub fn calc_pnl(&self, user_id: &str, price: f64) -> f64 {
        self.state.shorts.iter()
            .filter(|p| p.user_id == user_id)
            .map(|p| (p.entry_price - price) * p.amount_btc - p.amount_btc * p.entry_price * 0.0005) // 0.05% fee
            .sum()
    }
}
