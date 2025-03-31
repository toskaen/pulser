use tokio::sync::{Mutex, broadcast};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use reqwest::Client;
use log::{info, warn, error};
use serde_json::json;
use common::error::PulserError;
use tokio::time::{sleep, timeout};
use std::collections::VecDeque;
use common::StateManager;
use common::types::{UtxoInfo, StableChain, WebhookRetry};
use std::path::Path;
use serde::{Serialize, Deserialize};


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

// Updated to take Vec<WebhookRetry>
async fn save_retry_queue(state_manager: &StateManager, retry_queue: Vec<WebhookRetry>) -> Result<(), PulserError> {
    state_manager.save(Path::new("webhook_retry_queue.json"), &retry_queue).await
}

async fn load_retry_queue(state_manager: &StateManager) -> Result<VecDeque<WebhookRetry>, PulserError> {
    match state_manager.load::<Vec<WebhookRetry>>(Path::new("webhook_retry_queue.json")).await {
        Ok(retries) => Ok(VecDeque::from(retries)),
        Err(_) => {
            warn!("No existing webhook retry queue found, starting with empty queue");
            Ok(VecDeque::new())
        }
    }
}

pub async fn notify_new_utxos(
    client: &Client,
    user_id: &str,
    new_utxos: &[UtxoInfo],
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

    match tokio::time::timeout(Duration::from_secs(5), retry_queue.lock()).await {
        Ok(mut queue) => {
            queue.push_back(WebhookRetry {
                user_id: user_id.to_string(),
                utxos: new_utxos.to_vec(),
                attempts: 0,
                next_attempt: timestamp + config.retry_interval_secs,
            });
            warn!("Queued webhook retry for user {}: {} UTXOs", user_id, new_utxos.len());
        },
        Err(_) => warn!("Timeout queueing webhook retry for user {}", user_id),
    }
    
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
    
    let persisted_retries = load_retry_queue(&state_manager).await.unwrap_or_default();
    {
        let mut queue = retry_queue.lock().await;
        for retry in persisted_retries {
            queue.push_back(retry);
        }
    }
    
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                let retry_queue_data = {
                    let queue = retry_queue.lock().await;
                    queue.iter().cloned().collect::<Vec<WebhookRetry>>()
                };
                save_retry_queue(&state_manager, retry_queue_data).await.ok();
                break;
            },
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
                        let updated_queue_data = {
                            let queue = retry_queue.lock().await;
                            queue.iter().cloned().collect::<Vec<WebhookRetry>>()
                        };
                        save_retry_queue(&state_manager, updated_queue_data).await.ok();
                    } else if let Ok(stable_chain) = state_manager.load_stable_chain(&retry.user_id).await {
                        let next_retry = WebhookRetry {
                            user_id: retry.user_id.clone(),
                            utxos: retry.utxos.clone(),
                            attempts: retry.attempts + 1,
                            next_attempt: now + (config.retry_interval_secs * 2u64.pow(retry.attempts)),
                        };
                        match notify_new_utxos(&client, &retry.user_id, &retry.utxos, &stable_chain, &webhook_url, retry_queue.clone(), &config).await {
                            Ok(_) => {
                                info!("Webhook retry succeeded for user {}", retry.user_id);
                                let updated_queue_data = {
                                    let queue = retry_queue.lock().await;
                                    queue.iter().cloned().collect::<Vec<WebhookRetry>>()
                                };
                                save_retry_queue(&state_manager, updated_queue_data).await.ok();
                            }
                            Err(_) => {
                                let mut queue = retry_queue.lock().await;
                                queue.push_back(next_retry.clone());
                                warn!("Webhook retry failed for user {}, queued again (attempt {}/{})", retry.user_id, next_retry.attempts, config.retry_max_attempts);
                                drop(queue);
                                let updated_queue_data = {
                                    let queue = retry_queue.lock().await;
                                    queue.iter().cloned().collect::<Vec<WebhookRetry>>()
                                };
                                save_retry_queue(&state_manager, updated_queue_data).await.ok();
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
