// deposit-service/src/webhook.rs
use tokio::sync::{Mutex, broadcast};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use reqwest::Client;
use log::{info, warn, error};
use serde_json::json;
use common::error::PulserError;
use tokio::time::{sleep, timeout};
use std::collections::VecDeque;
use common::StateManager; // Updated importfv
use common::storage::UtxoInfo as StorageUtxoInfo;
use common::{StableChain, Bitcoin};
use common::types::DepositAddressInfo;
use common::types::HedgeNotification;
use common::WebhookRetry;
use common::UtxoInfo as TypesUtxoInfo;

#[derive(Clone)]
pub struct WebhookConfig {
    pub max_retries: u32,
    pub timeout_secs: u64,
    pub max_retry_time_secs: u64,
    pub retry_max_attempts: u32,
    pub base_backoff_ms: u64,
    pub retry_interval_secs: u64,
}

impl WebhookConfig {
    pub fn from_toml(config: &toml::Value) -> Self {
        Self {
            max_retries: config.get("webhook_max_retries").and_then(|v| v.as_integer()).unwrap_or(3) as u32,
            timeout_secs: config.get("webhook_timeout_secs").and_then(|v| v.as_integer()).unwrap_or(5) as u64,
            max_retry_time_secs: config.get("webhook_max_retry_time_secs").and_then(|v| v.as_integer()).unwrap_or(120) as u64,
            retry_max_attempts: config.get("webhook_retry_max_attempts").and_then(|v| v.as_integer()).unwrap_or(3) as u32,
            base_backoff_ms: config.get("webhook_base_backoff_ms").and_then(|v| v.as_integer()).unwrap_or(500) as u64,
            retry_interval_secs: config.get("retry_interval_secs").and_then(|v| v.as_integer()).unwrap_or(60) as u64,
        }
    }
}

pub async fn notify_new_utxos(
    client: &Client,
    user_id: &str,
    new_utxos: &[StorageUtxoInfo],
    stable_chain: &StableChain,
    webhook_url: &str,
    retry_queue: Arc<Mutex<VecDeque<WebhookRetry>>>,
    config: &WebhookConfig,
) -> Result<(), PulserError> {
    if new_utxos.is_empty() || webhook_url.is_empty() {
        return Ok(());
    }

    let total_stable_usd: f64 = new_utxos.iter().map(|u| u.stable_value_usd).sum();
    let total_btc: f64 = new_utxos.iter().map(|u| u.amount_sat as f64 / 100_000_000.0).sum();
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    let payload = json!({
        "event": "new_utxos",
        "user_id": user_id,
        "utxos": new_utxos,
        "total_btc": total_btc,
        "total_stable_usd": total_stable_usd,
        "timestamp": timestamp,
        "multisig_addr": stable_chain.multisig_addr,
        "hedge_position_id": stable_chain.hedge_position_id,
        "current_price": stable_chain.raw_btc_usd,
    });

    let start_time = Instant::now();
    let max_duration = Duration::from_secs(config.max_retry_time_secs);

    for retry in 0..config.max_retries {
        if start_time.elapsed() >= max_duration {
            break;
        }
        if retry > 0 {
            let backoff = Duration::from_millis(config.base_backoff_ms * 2u64.pow(retry));
            sleep(backoff).await;
            if start_time.elapsed() >= max_duration {
                break;
            }
        }
        match timeout(Duration::from_secs(config.timeout_secs), client.post(webhook_url).json(&payload).send()).await {
            Ok(Ok(response)) if response.status().is_success() => {
                info!("Webhook sent for user {}: {} UTXOs, ${:.2} USD", user_id, new_utxos.len(), total_stable_usd);
                return Ok(());
            }
            Ok(Ok(response)) => warn!("Webhook failed for user {} with status: {}", user_id, response.status()),
            Ok(Err(e)) => warn!("Webhook error for user {}: {}", user_id, e),
            Err(_) => warn!("Webhook timed out for user {}", user_id),
        }
    }

    let mut queue = retry_queue.lock().await;
    // Convert from storage::UtxoInfo to types::UtxoInfo
    let type_utxos = new_utxos.iter().map(|utxo| TypesUtxoInfo {
        txid: utxo.txid.clone(),
        vout: utxo.vout,
        amount_sat: utxo.amount_sat,
        address: utxo.address.clone(),
        keychain: utxo.keychain.clone(),
        timestamp: utxo.timestamp,
        confirmations: utxo.confirmations,
        participants: utxo.participants.clone(),
        stable_value_usd: utxo.stable_value_usd,
        spendable: utxo.spendable,
        derivation_path: utxo.derivation_path.clone(),
        spent: utxo.spent,
    }).collect();

    queue.push_back(WebhookRetry {
        user_id: user_id.to_string(),
        utxos: type_utxos,
        attempts: 0,
        next_attempt: timestamp + config.retry_interval_secs,
    });

    warn!("Queued webhook retry for user {}: {} UTXOs", user_id, new_utxos.len());
    Err(PulserError::NetworkError(format!("Webhook attempts failed for user {}, queued for retry", user_id)))
}

pub async fn start_retry_task(
    client: Client,
    retry_queue: Arc<Mutex<VecDeque<WebhookRetry>>>,
    webhook_url: String,
    config: WebhookConfig,
    mut shutdown_rx: broadcast::Receiver<()>,
    state_manager: Arc<StateManager>,
) -> Result<(), PulserError> {
    info!("Starting webhook retry task");
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Retry task shutting down");
                break;
            }
            _ = sleep(Duration::from_secs(config.retry_interval_secs)) => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                let retry = {
                    let mut queue = retry_queue.lock().await;
                    queue.pop_front()
                };
                if let Some(retry) = retry {
                    if retry.next_attempt > now {
                        let mut queue = retry_queue.lock().await;
                        queue.push_front(retry);
                    } else if retry.attempts >= config.retry_max_attempts {
                        error!("Webhook for user {} failed after {} retries", retry.user_id, config.retry_max_attempts);
                    } else if let Ok(stable_chain) = state_manager.load_stable_chain(&retry.user_id).await {
                        let next_retry = WebhookRetry {
                            user_id: retry.user_id.clone(),
                            utxos: retry.utxos.clone(),
                            attempts: retry.attempts + 1,
                            next_attempt: now + (config.retry_interval_secs * 2u64.pow(retry.attempts)),
                        };
                        let storage_utxos: Vec<StorageUtxoInfo> = retry.utxos.iter().map(|utxo| StorageUtxoInfo {
                            txid: utxo.txid.clone(),
                            vout: utxo.vout,
                            amount_sat: utxo.amount_sat,
                            address: utxo.address.clone(),
                            keychain: utxo.keychain.clone(),
                            timestamp: utxo.timestamp,
                            confirmations: utxo.confirmations,
                            participants: utxo.participants.clone(),
                            stable_value_usd: utxo.stable_value_usd,
                            spendable: utxo.spendable,
                            derivation_path: utxo.derivation_path.clone(),
                            spent: utxo.spent,
                        }).collect();
                        match notify_new_utxos(&client, &retry.user_id, &storage_utxos, &stable_chain, &webhook_url, retry_queue.clone(), &config).await {
                            Ok(_) => info!("Webhook retry succeeded for user {}", retry.user_id),
                            Err(_) => {
                                let mut queue = retry_queue.lock().await;
                                let attempts = next_retry.attempts;
                                queue.push_back(next_retry);
                                warn!("Webhook retry failed for user {}, queued again (attempt {}/{})", retry.user_id, attempts, config.retry_max_attempts);
                            }
                        }
                    } else {
                        error!("Failed to load stable chain for user {}", retry.user_id);
                    }
                }
            }
        }
    }
    info!("Retry task shutdown complete");
    Ok(())
}
async fn get_stable_chain_for_user(user_id: &str) -> Result<StableChain, PulserError> {
  
    Err(PulserError::UserNotFound(user_id.to_string()))
}
