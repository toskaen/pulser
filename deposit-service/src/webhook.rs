// deposit-service/src/webhook.rs
use tokio::sync::{Mutex, broadcast};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use reqwest::Client;
use log::{info, warn, error};
use serde_json::json;
use common::error::PulserError;
use deposit_service::storage::UtxoInfo;
use deposit_service::types::{HedgeNotification, StableChain, WebhookRetry};
use tokio::time::{sleep, timeout};
use std::collections::VecDeque;

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

#[derive(Clone, Debug)]
pub struct WebhookRetry {
    pub user_id: String,
    pub utxos: Vec<UtxoInfo>,
    pub stable_chain: StableChain, // Full context for retries
    pub attempts: u32,
    pub next_attempt: u64,
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
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

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

    let mut queue = retry_queue.lock().await.unwrap();
    queue.push_back(WebhookRetry {
        user_id: user_id.to_string(),
        utxos: new_utxos.to_vec(),
        stable_chain: stable_chain.clone(),
        attempts: 0,
        next_attempt: timestamp + config.retry_interval_secs,
    });
    warn!("Queued webhook retry for user {}: {} UTXOs", user_id, new_utxos.len());
    Err(PulserError::NetworkError(format!("Webhook attempts failed for user {}, queued for retry", user_id)))
}

pub async fn notify_hedge_service(
    client: &Client,
    stable_chain: &StableChain,
    hedge_url: &str,
    action: &str,
) -> Result<(), PulserError> {
    let notification = HedgeNotification {
        user_id: stable_chain.user_id,
        action: action.to_string(),
        btc_amount: stable_chain.accumulated_btc.to_btc(),
        usd_amount: stable_chain.stabilized_usd.0,
        current_price: stable_chain.raw_btc_usd,
        timestamp: stable_chain.timestamp,
        transaction_id: stable_chain.pending_sweep_txid.clone(),
    };

    match client.post(hedge_url).json(&notification).send().await {
        Ok(response) if response.status().is_success() => {
            info!("Hedge notification sent for user {}: {}", stable_chain.user_id, action);
            Ok(())
        }
        Ok(response) => Err(PulserError::NetworkError(format!(
            "Hedge notification failed with status: {}", response.status()
        ))),
        Err(e) => Err(PulserError::NetworkError(format!(
            "Hedge notification error: {}", e
        ))),
    }
}

pub async fn start_retry_task(
    client: Client,
    retry_queue: Arc<Mutex<VecDeque<WebhookRetry>>>,
    webhook_url: String,
    config: WebhookConfig,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<(), PulserError> {
    info!("Starting webhook retry task");
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Retry task shutting down");
                break;
            }
            _ = sleep(Duration::from_secs(config.retry_interval_secs)) => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let retry = {
                    let mut queue = retry_queue.lock().await.unwrap();
                    queue.pop_front()
                };

                if let Some(mut retry) = retry {
                    if retry.next_attempt > now {
                        let mut queue = retry_queue.lock().await.unwrap();
                        queue.push_front(retry);
                    } else if retry.attempts >= config.retry_max_attempts {
                        error!("Webhook for user {} failed after {} retries", retry.user_id, config.retry_max_attempts);
                    } else {
                        let next_retry = WebhookRetry {
                            user_id: retry.user_id.clone(),
                            utxos: retry.utxos.clone(),
                            stable_chain: retry.stable_chain.clone(),
                            attempts: retry.attempts + 1,
                            next_attempt: now + (config.retry_interval_secs * 2u64.pow(retry.attempts)),
                        };
                        match notify_new_utxos(
                            &client,
                            &retry.user_id,
                            &retry.utxos,
                            &retry.stable_chain,
                            &webhook_url,
                            retry_queue.clone(),
                            &config,
                        ).await {
                            Ok(_) => info!("Webhook retry succeeded for user {}", retry.user_id),
                            Err(_) => {
                                let mut queue = retry_queue.lock().await.unwrap();
                                queue.push_back(next_retry);
                                warn!("Webhook retry failed for user {}, queued again (attempt {}/{})",
                                      retry.user_id, next_retry.attempts, config.retry_max_attempts);
                            }
                        }
                    }
                }
            }
        }
    }
    info!("Retry task shutdown complete");
    Ok(())
}
