// hedging-service/src/state.rs
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use common::HedgePosition;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HedgeState {
    pub shorts: Vec<HedgePosition>,
    pub rainy_fund: f64,
    pub last_hedge_time: HashMap<String, u64>,
    pub user_pnl: HashMap<String, f64>,
    pub collateral: HashMap<String, f64>,
}
