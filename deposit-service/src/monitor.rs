// deposit-service/src/monitor.rs
use tokio::sync::{Mutex, mpsc};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use log::{info, warn};
use bdk_wallet::bitcoin::{Address, Network};
use deposit_service::wallet::DepositWallet;
use deposit_service::types::{StableChain, UserStatus};
use common::price_feed::PriceInfo;
use tokio::time::sleep;

#[derive(Clone)]
pub struct MonitorConfig {
    pub check_interval_secs: u64,
    pub deposit_window_hours: u64,
    pub min_check_interval_secs: u64,
}

impl MonitorConfig {
    pub fn from_toml(config: &toml::Value) -> Self {
        Self {
            check_interval_secs: config.get("monitor_check_interval_secs").and_then(|v| v.as_integer()).unwrap_or(60) as u64,
            deposit_window_hours: config.get("monitor_deposit_window_hours").and_then(|v| v.as_integer()).unwrap_or(24) as u64,
            min_check_interval_secs: config.get("monitor_min_check_interval_secs").and_then(|v| v.as_integer()).unwrap_or(3600) as u64,
        }
    }
}

pub async fn start_periodic_monitor(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    last_activity_check: Arc<Mutex<HashMap<String, u64>>>,
    sync_tx: mpsc::Sender<String>,
    price_info: Arc<Mutex<PriceInfo>>,
    config: MonitorConfig,
) {
    loop {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let price_info_data = price_info.lock().await.unwrap().clone();

        {
            let wallets_lock = wallets.lock().await.unwrap();
            let mut statuses_lock = user_statuses.lock().await.unwrap();
            let mut last_check_lock = last_activity_check.lock().await.unwrap();

            for (user_id, (wallet, chain)) in wallets_lock.iter() {
                let status = match statuses_lock.get_mut(user_id) {
                    Some(status) => status,
                    None => continue, // Skip if status missing (shouldn’t happen)
                };

                let last_deposit = status.last_deposit_time.unwrap_or(0);
                let last_check = *last_check_lock.get(user_id).unwrap_or(&0);

                // Check if within deposit window and hasn’t been checked recently
                if now - last_deposit < config.deposit_window_hours * 3600 && now - last_check >= config.min_check_interval_secs {
                    match Address::from_str(&status.current_deposit_address) {
                        Ok(addr) => match addr.require_network(Network::Testnet) {
                            Ok(addr) => {
                                match wallet.check_address(&addr, &price_info_data).await {
                                    Ok(utxos) => {
                                        if utxos.iter().any(|u| !chain.utxos.iter().any(|c| c.txid == u.txid && c.vout == u.vout)) {
                                            info!("Pending deposit detected for user {}: {} new UTXOs", user_id, utxos.len());
                                            if let Err(e) = sync_tx.send(user_id.clone()).await {
                                                warn!("Failed to send sync trigger for user {}: {}", user_id, e);
                                            }
                                        }
                                    }
                                    Err(e) => warn!("Periodic check failed for user {}: {}", user_id, e),
                                }
                            }
                            Err(e) => warn!("Invalid network for address {} of user {}: {}", status.current_deposit_address, user_id, e),
                        },
                        Err(e) => warn!("Invalid address {} for user {}: {}", status.current_deposit_address, user_id, e),
                    }
                    last_check_lock.insert(user_id.clone(), now);
                }
            }
        }

        sleep(Duration::from_secs(config.check_interval_secs)).await;
    }
}
