// hedging-service/src/hedge_monitor.rs
use crate::hedging::HedgeManager;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::sync::{Mutex, broadcast};
use serde::{Serialize, Deserialize};
use log::{info, warn};
use common::error::PulserError;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HedgeMonitorStatus {
    up_since: u64,
    last_success: u64,
    errors_since_startup: u32,
    consecutive_errors: u32,
    users_monitored: u32,
    total_hedge_value_usd: f64,
    total_pnl_usd: f64,
    health: String,
     ai_drop_probability: f64, // New field
    last_ai_action: String,   // e.g., "Increased hedge", "Reduced hedge"
}

pub struct HedgeMonitor {
    manager: Arc<Mutex<HedgeManager>>,
    status: Arc<Mutex<HedgeMonitorStatus>>,
    interval_secs: u64,
    max_retries: u32,
}

impl HedgeMonitor {
    pub fn new(manager: Arc<Mutex<HedgeManager>>) -> Self {
        let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        HedgeMonitor {
            manager,
            status: Arc::new(Mutex::new(HedgeMonitorStatus {
                up_since: start,
                last_success: 0,
                errors_since_startup: 0,
                consecutive_errors: 0,
                users_monitored: 0,
                total_hedge_value_usd: 0.0,
                total_pnl_usd: 0.0,
                health: "starting".to_string(),
            })),
            interval_secs: 30,
            max_retries: 5,
        }
    }

// In hedging-service/src/hedge_monitor.rs
pub async fn start(&self, mut shutdown_rx: broadcast::Receiver<()>) -> Result<(), PulserError> {
    let ai_agent = crate::ai_agent::AiAgent::new(self.manager.clone());
    info!("Starting hedge monitor");
    let mut retry_count = 0;
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Hedge monitor shutting down");
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(self.interval_secs)) => {
                match self.monitor_once().await {
                    Ok(_) => {
                        retry_count = 0;
                        let mut status = self.status.lock().await;
                        status.last_success = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                        status.consecutive_errors = 0;
                        status.health = "healthy".to_string();
                    }
                    Err(e) => {
                        warn!("Monitor failed: {}", e);
                        let mut status = self.status.lock().await;
                        status.errors_since_startup += 1;
                        status.consecutive_errors += 1;
                        retry_count += 1;
                        if retry_count >= self.max_retries || status.consecutive_errors >= 3 {
                            status.health = if status.total_pnl_usd < -0.05 * status.total_hedge_value_usd { "critical" } else { "unhealthy" };
                            ai_agent.adjust_hedging().await?; // Trigger AI on critical/unhealthy
                            break;
                        }
                        tokio::time::sleep(Duration::from_secs(5 * (1 << retry_count))).await;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn monitor_once(&self) -> Result<(), PulserError> {
    let mut manager = self.manager.lock().await;
    let users: Vec<String> = /* unchanged */;
    let mut total_hedge_value = 0.0;
    let mut total_pnl = 0.0;
    let ai_agent = crate::ai_agent::AiAgent::new(self.manager.clone());
    let mut max_probability = 0.0;

    for chunk in users.chunks(10) {
        let mut batch_hedge = 0.0;
        for user_id in chunk {
            let stablechain_path = PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id));
            let stablechain: StableChain = manager.state_manager.load(&stablechain_path).await?; // Reload every time
            let total_btc = stablechain.accumulated_btc.to_btc();
            manager.state.shorts.retain(|p| p.amount_btc <= total_btc * 3.0); // Cap at max_hedge_cap
            manager.hedge_stablechain(user_id).await?;
            let price = manager.price_feed.get_latest_price().await?;
            batch_hedge += manager.state.shorts.iter()
                .filter(|p| p.user_id == user_id)
                .map(|p| p.amount_btc * price)
                .sum::<f64>();
            total_pnl += manager.calc_pnl(user_id, price);
            let probability = ai_agent.predict_drop(user_id).await?;
            max_probability = max_probability.max(probability);
        }
        total_hedge_value += batch_hedge;
    }

    let mut status = self.status.lock().await;
    status.users_monitored = users.len() as u32;
    status.total_hedge_value_usd = total_hedge_value;
    status.total_pnl_usd = total_pnl;
    status.ai_drop_probability = max_probability;
    status.last_ai_action = if max_probability > 0.6 { "Increased hedge" } else if max_probability < 0.4 { "Reduced hedge" } else { "No action" }.to_string();
    info!("Monitor: {} users, ${:.2} hedged, ${:.2} PnL, AI prob {:.2}%", users.len(), total_hedge_value, total_pnl, max_probability * 100.0);
    manager.state_manager.save("hedge_monitor_status.json", &*status).await?;
    Ok(())
}
}
