// In deposit-service/src/monitoring.rs
use crate::types::StableChain;
use log::info;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;

use crate::wallet::DepositWallet;

/// Structure to hold all monitoring state
pub struct MonitoringService {
    pub wallets: Arc<RwLock<HashMap<u32, (DepositWallet, StableChain)>>>,
    pub config: Arc<RwLock<crate::config::Config>>,
    pub current_price: Arc<RwLock<f64>>,
    pub synthetic_price: Arc<RwLock<f64>>,
    pub should_stop: Arc<RwLock<bool>>,
}

impl MonitoringService {
    pub fn new(
        wallets: Arc<RwLock<HashMap<u32, (DepositWallet, StableChain)>>>,
        config: Arc<RwLock<crate::config::Config>>,
        current_price: Arc<RwLock<f64>>,
        synthetic_price: Arc<RwLock<f64>>,
    ) -> Self {
        Self {
            wallets,
            config,
            current_price,
            synthetic_price,
            should_stop: Arc::new(RwLock::new(false)),
        }
    }
    
    /// Start the monitoring service
    pub async fn start(&self) {
        // Just log for now
        info!("Monitoring service started");
    }
    
    /// Stop the monitoring service
    pub fn stop(&self) {
        *self.should_stop.write().unwrap() = true;
        info!("Monitoring service stopping...");
    }
}
