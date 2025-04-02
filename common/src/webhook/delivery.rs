use super::config::WebhookConfig;
use super::retry::{RetryQueue, WebhookEntry};
use crate::error::PulserError;
use hmac::{Hmac, Mac};
use log::{debug, info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::timeout;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookPayload {
    pub event: String,
    pub user_id: String,
    pub data: Value,
    pub timestamp: u64,
}

pub struct WebhookManager {
    client: Client,
    config: WebhookConfig,
    batch_queues: Arc<RwLock<HashMap<String, VecDeque<WebhookPayload>>>>,
    endpoint_states: Arc<RwLock<HashMap<String, EndpointState>>>,
    retry_queue: Arc<Mutex<RetryQueue>>,
    tx: mpsc::Sender<WebhookCommand>,
}

struct EndpointState {
    last_call: Instant,
    consecutive_failures: u32,
    rate_limit_remaining: u32,
    rate_limit_reset: Instant,
}

enum WebhookCommand {
    Deliver {
        endpoint: String,
        payload: WebhookPayload,
    },
    ProcessBatch {
        endpoint: String,
    },
    Shutdown,
}

impl WebhookManager {
    pub async fn new(
        client: Client,
        config: WebhookConfig,
        retry_queue: Arc<Mutex<RetryQueue>>,
    ) -> Self {
        // Create channels
        let (tx, mut rx) = mpsc::channel::<WebhookCommand>(100);

        let batch_queues = Arc::new(RwLock::new(HashMap::new()));
        let endpoint_states = Arc::new(RwLock::new(HashMap::new()));
        let tx_clone = tx.clone();
        let batch_queues_clone = batch_queues.clone();
        let endpoint_states_clone = endpoint_states.clone();
        let client_clone = client.clone();
        let config_clone = config.clone();
        let retry_queue_clone = retry_queue.clone();

        // Start worker task to process webhooks
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    WebhookCommand::Deliver { endpoint, payload } => {
                        let mut queues = batch_queues_clone.write().await;
                        let queue = queues.entry(endpoint.clone()).or_insert_with(VecDeque::new);
                        queue.push_back(payload);

                        // Schedule batch processing if this is the first item
                        if queue.len() == 1 {
                            let tx = tx_clone.clone();
                            let endpoint_clone = endpoint.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(Duration::from_millis(config_clone.batch_interval_ms)).await;
                                if let Err(e) = tx.send(WebhookCommand::ProcessBatch { endpoint: endpoint_clone }).await {
                                    warn!("Failed to schedule batch processing: {}", e);
                                }
                            });
                        }
                    }
                    WebhookCommand::ProcessBatch { endpoint } => {
                        let payloads = {
                            let mut queues = batch_queues_clone.write().await;
                            let queue = queues.entry(endpoint.clone()).or_insert_with(VecDeque::new);
                            let mut batch = Vec::new();
                            while let Some(payload) = queue.pop_front() {
                                batch.push(payload);
                                if batch.len() >= config_clone.batch_size {
                                    break;
                                }
                            }
                            batch
                        };

                        if !payloads.is_empty() {
                            let combined_payload = if payloads.len() == 1 {
                                serde_json::to_value(&payloads[0]).unwrap_or(Value::Null)
                            } else {
                                // Batch format
                                serde_json::json!({
                                    "batch": true,
                                    "count": payloads.len(),
                                    "events": payloads
                                })
                            };

                            // Check rate limiting
                            let should_deliver = {
                                let mut states = endpoint_states_clone.write().await;
                                let state = states.entry(endpoint.clone()).or_insert_with(|| EndpointState {
                                    last_call: Instant::now() - Duration::from_secs(60),
                                    consecutive_failures: 0,
                                    rate_limit_remaining: 60, // Default: 60 per minute
                                    rate_limit_reset: Instant::now(),
                                });

                                let now = Instant::now();
                                if now.duration_since(state.rate_limit_reset).as_secs() >= 60 {
                                    state.rate_limit_remaining = 60;
                                    state.rate_limit_reset = now;
                                }

                                if state.rate_limit_remaining > 0 {
                                    state.rate_limit_remaining -= 1;
                                    state.last_call = now;
                                    true
                                } else {
                                    false
                                }
                            };

                            if should_deliver {
                                // Generate signature
                                let signature = generate_signature(&combined_payload, &config_clone.secret);

                                match deliver_single_webhook(
                                    &client_clone,
                                    &endpoint,
                                    &combined_payload,
                                    &signature,
                                    config_clone.timeout_secs,
                                ).await {
                                    Ok(_) => {
                                        // Reset failure count on success
                                        let mut states = endpoint_states_clone.write().await;
                                        if let Some(state) = states.get_mut(&endpoint) {
                                            state.consecutive_failures = 0;
                                        }
                                        debug!("Successfully delivered webhook batch of {} to {}", payloads.len(), endpoint);
                                    }
                                    Err(e) => {
                                        let backoff = {
                                            let mut states = endpoint_states_clone.write().await;
                                            let state = states.entry(endpoint.clone()).or_insert_with(|| EndpointState {
                                                last_call: Instant::now(),
                                                consecutive_failures: 0,
                                                rate_limit_remaining: 60,
                                                rate_limit_reset: Instant::now(),
                                            });
                                            state.consecutive_failures += 1;
                                            config_clone.calculate_backoff(state.consecutive_failures)
                                        };

                                        warn!("Failed to deliver webhook to {}: {}. Adding to retry queue with {}ms backoff.", 
                                             endpoint, e, backoff.as_millis());

                                        // Add to retry queue
                                        let retry_time = super::now() + backoff.as_secs();
                                        for payload in payloads {
                                            let entry = WebhookEntry {
                                                id: uuid::Uuid::new_v4().to_string(),
                                                endpoint: endpoint.clone(),
                                                payload: serde_json::to_value(&payload).unwrap_or(Value::Null),
                                                attempts: 1,
                                                next_attempt: retry_time,
                                                created_at: super::now(),
                                            };

                                            let mut retry_queue = retry_queue_clone.lock().await;
                                            retry_queue.push(entry).await.ok();
                                        }
                                    }
                                }
                            } else {
                                // Rate limited - requeue with backoff
                                let mut states = endpoint_states_clone.write().await;
                                let state = states.entry(endpoint.clone()).or_insert_with(|| EndpointState {
                                    last_call: Instant::now(),
                                    consecutive_failures: 0,
                                    rate_limit_remaining: 0,
                                    rate_limit_reset: Instant::now() + Duration::from_secs(60),
                                });

                                let reset_in = state.rate_limit_reset.duration_since(Instant::now());
                                debug!("Rate limited for endpoint {}. Will reset in {}s", endpoint, reset_in.as_secs());

                                // Add back to batch queue
                                let mut queues = batch_queues_clone.write().await;
                                let queue = queues.entry(endpoint.clone()).or_insert_with(VecDeque::new);
                                for payload in payloads {
                                    queue.push_back(payload);
                                }

                                // Schedule next attempt
                                let tx = tx_clone.clone();
                                let endpoint_clone = endpoint.clone();
                                tokio::spawn(async move {
                                    tokio::time::sleep(reset_in).await;
                                    if let Err(e) = tx.send(WebhookCommand::ProcessBatch { endpoint: endpoint_clone }).await {
                                        warn!("Failed to schedule batch retry: {}", e);
                                    }
                                });
                            }
                        }

                        // Check if there are more items in queue
                        let has_more = {
                            let queues = batch_queues_clone.read().await;
                            queues.get(&endpoint).map(|q| !q.is_empty()).unwrap_or(false)
                        };

                        if has_more {
                            let tx = tx_clone.clone();
                            let endpoint_clone = endpoint.clone();
                            tokio::spawn(async move {
                                // Add slight delay to avoid hammering
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                if let Err(e) = tx.send(WebhookCommand::ProcessBatch { endpoint: endpoint_clone }).await {
                                    warn!("Failed to schedule remaining batch: {}", e);
                                }
                            });
                        }
                    }
                    WebhookCommand::Shutdown => {
                        info!("Webhook manager shutting down");
                        break;
                    }
                }
            }
        });

        Self {
            client,
            config,
            batch_queues,
            endpoint_states,
            retry_queue,
            tx,
        }
    }

    pub async fn send(&self, endpoint: &str, payload: WebhookPayload) -> Result<(), PulserError> {
        if !self.config.enabled {
            debug!("Webhooks disabled, skipping delivery to {}", endpoint);
            return Ok(());
        }

        if let Err(e) = self.tx.send(WebhookCommand::Deliver {
            endpoint: endpoint.to_string(),
            payload,
        }).await {
            warn!("Failed to queue webhook: {}", e);
            return Err(PulserError::WebhookError(format!("Failed to queue webhook: {}", e)));
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), PulserError> {
        if let Err(e) = self.tx.send(WebhookCommand::Shutdown).await {
            warn!("Error sending shutdown command to webhook manager: {}", e);
            return Err(PulserError::WebhookError(format!("Shutdown error: {}", e)));
        }
        Ok(())
    }
}

/// Generate HMAC signature for webhook payload
fn generate_signature(payload: &Value, secret: &str) -> String {
    let payload_bytes = serde_json::to_string(payload).unwrap_or_default().into_bytes();
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(&payload_bytes);
    let result = mac.finalize();
    format!("sha256={}", hex::encode(result.into_bytes()))
}

/// Deliver a webhook with timeout
async fn deliver_single_webhook(
    client: &Client,
    endpoint: &str,
    payload: &Value,
    signature: &str,
    timeout_secs: u64,
) -> Result<(), PulserError> {
    match timeout(
        Duration::from_secs(timeout_secs),
        client
            .post(endpoint)
            .json(payload)
            .header("Content-Type", "application/json")
            .header("X-Webhook-Signature", signature)
            .send(),
    )
    .await
    {
        Ok(Ok(response)) => {
            if response.status().is_success() {
                Ok(())
            } else {
                Err(PulserError::WebhookError(format!(
                    "Webhook returned non-success status: {}",
                    response.status()
                )))
            }
        }
        Ok(Err(e)) => Err(PulserError::WebhookError(format!("Webhook request error: {}", e))),
        Err(_) => Err(PulserError::WebhookError(format!(
            "Webhook timed out after {}s",
            timeout_secs
        ))),
    }
}

/// Simple function to deliver a webhook without the manager
pub async fn deliver_webhook(
    client: &Client,
    endpoint: &str,
    payload: &Value,
    secret: &str,
    timeout_secs: u64,
) -> Result<(), PulserError> {
    let signature = generate_signature(payload, secret);
    deliver_single_webhook(client, endpoint, payload, &signature, timeout_secs).await
}
