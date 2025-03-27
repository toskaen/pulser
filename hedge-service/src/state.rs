// hedging-service/src/state.rs
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HedgePosition {
    pub entry_price: f64,
    pub amount_btc: f64,
    pub order_id: String,
    pub user_id: String,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HedgeState {
    pub shorts: Vec<HedgePosition>,
    pub rainy_fund: f64,
    pub last_hedge_time: HashMap<String, u64>,
    pub user_pnl: HashMap<String, f64>,
    pub collateral: HashMap<String, f64>,
}
