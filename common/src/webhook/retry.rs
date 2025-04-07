// common/src/webhook/retry.rs
use crate::error::PulserError;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::sleep;
use tokio::time::timeout;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookEntry {
    pub id: String,
    pub endpoint: String,
    pub payload: Value,
    pub attempts: u32,
    pub next_attempt: u64,
    pub created_at: u64,
}

pub enum RetryStrategy {
    Memory(VecDeque<WebhookEntry>),
    Redis {
        client: redis::Client,
        queue_key: String,
        deadletter_key: String,
    },
}

pub struct RetryQueue {
    strategy: RetryStrategy,
    max_attempts: u32,
}

impl RetryQueue {
    pub fn new_memory(max_attempts: u32) -> Self {
        Self {
            strategy: RetryStrategy::Memory(VecDeque::new()),
            max_attempts,
        }
    }

    pub fn new_redis(
        redis_url: &str,
        queue_key: &str,
        deadletter_key: &str,
        max_attempts: u32,
    ) -> Result<Self, PulserError> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| PulserError::StorageError(format!("Redis error: {}", e)))?;

        Ok(Self {
            strategy: RetryStrategy::Redis {
                client,
                queue_key: queue_key.to_string(),
                deadletter_key: deadletter_key.to_string(),
            },
            max_attempts,
        })
    }

    pub async fn push(&mut self, entry: WebhookEntry) -> Result<(), PulserError> {
        match &mut self.strategy {
            RetryStrategy::Memory(queue) => {
                queue.push_back(entry);
                Ok(())
            }
            RetryStrategy::Redis {
                client,
                queue_key,
                ..
            } => {
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| PulserError::StorageError(format!("Redis error: {}", e)))?;

                let entry_json = serde_json::to_string(&entry)
                    .map_err(|e| PulserError::StorageError(format!("JSON error: {}", e)))?;

                // Store in sorted set with score as next_attempt time
                let _: () = redis::cmd("ZADD")
                    .arg(queue_key.clone())
                    .arg(entry.next_attempt)
                    .arg(entry_json)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| PulserError::StorageError(format!("Redis error: {}", e)))?;

                Ok(())
            }
        }
    }

    pub async fn pop_ready(&mut self, now: u64) -> Result<Option<WebhookEntry>, PulserError> {
        match &mut self.strategy {
            RetryStrategy::Memory(queue) => {
                // Find first ready entry
                let idx = queue
                    .iter()
                    .position(|e| e.next_attempt <= now)
                    .unwrap_or(queue.len());

                if idx < queue.len() {
                    Ok(Some(queue.remove(idx).unwrap()))
                } else {
                    Ok(None)
                }
            }
            RetryStrategy::Redis {
                client,
                queue_key,
                ..
            } => {
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| PulserError::StorageError(format!("Redis error: {}", e)))?;

                // Get and remove first entry ready for retry (with score <= now)
                let result: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                    .arg(queue_key.clone())
                    .arg(0)
                    .arg(now)
                    .arg("LIMIT")
                    .arg(0)
                    .arg(1)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| PulserError::StorageError(format!("Redis error: {}", e)))?;

                if result.is_empty() {
                    return Ok(None);
                }

                let entry_json = &result[0];
                
                // Remove from queue
                let _: () = redis::cmd("ZREM")
                    .arg(queue_key.clone())
                    .arg(entry_json)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| PulserError::StorageError(format!("Redis error: {}", e)))?;

                // Parse the entry
                let entry: WebhookEntry = serde_json::from_str(entry_json)
                    .map_err(|e| PulserError::StorageError(format!("JSON error: {}", e)))?;

                Ok(Some(entry))
            }
        }
    }

    pub async fn move_to_deadletter(&mut self, entry: WebhookEntry) -> Result<(), PulserError> {
        match &mut self.strategy {
            RetryStrategy::Memory(_) => {
                // Just log in memory mode
                warn!(
                    "Webhook to {} failed after {} attempts, entry id: {}",
                    entry.endpoint, entry.attempts, entry.id
                );
                Ok(())
            }
            RetryStrategy::Redis {
                client,
                deadletter_key,
                ..
            } => {
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| PulserError::StorageError(format!("Redis error: {}", e)))?;

                let entry_json = serde_json::to_string(&entry)
                    .map_err(|e| PulserError::StorageError(format!("JSON error: {}", e)))?;

                // Add to deadletter queue
                let _: () = redis::cmd("ZADD")
                    .arg(deadletter_key.clone())
                    .arg(super::now()) // Current time as score
                    .arg(entry_json)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| PulserError::StorageError(format!("Redis error: {}", e)))?;

                warn!(
                    "Webhook to {} failed after {} attempts, moved to deadletter, entry id: {}",
                    entry.endpoint, entry.attempts, entry.id
                );

                Ok(())
            }
        }
    }

// In common/src/webhook/retry.rs
// Enhance the retry_failed method

pub async fn retry_failed(
    &mut self,
    client: &reqwest::Client,
    webhook_secret: &str,
    backoff_fn: impl Fn(u32) -> Duration,
) -> Result<(), PulserError> {
    let now = super::now();
    let max_retries = 3; // Process up to 3 retries at once to avoid overwhelming
    
    for _ in 0..max_retries {
        if let Some(mut entry) = self.pop_ready(now).await? {
            // Generate a unique request ID for tracing
            let request_id = uuid::Uuid::new_v4().to_string();
            debug!("Retrying webhook (id: {}): attempt {}/{})", request_id, entry.attempts, self.max_attempts);
            
            // Add extra headers for improved diagnostics
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("X-Retry-Attempt", entry.attempts.to_string().parse().unwrap());
            headers.insert("X-Request-ID", request_id.parse().unwrap());
            
            // Generate signature
            let signature = super::delivery::generate_signature(&entry.payload, webhook_secret);
            
            // Increase timeout for retries
            let timeout_secs = 15; // Longer timeout for retries
            
            match timeout(
                Duration::from_secs(timeout_secs),
                client
                    .post(&entry.endpoint)
                    .json(&entry.payload)
                    .headers(headers)
                    .header("X-Webhook-Signature", signature)
                    .send(),
            ).await {
                Ok(Ok(response)) => {
                    if response.status().is_success() {
                        info!("Successfully delivered retry webhook to {}, entry id: {}", 
                              entry.endpoint, entry.id);
                    } else {
                        // Non-success status code
                        let status = response.status();
                        
                        // Check if we should retry based on status code
                        let should_retry = match status.as_u16() {
                            408 | 429 | 500..=599 => true, // Timeouts, rate limits, server errors
                            _ => false, // Don't retry client errors
                        };
                        
                        if should_retry && entry.attempts < self.max_attempts {
                            entry.attempts += 1;
                            let backoff = backoff_fn(entry.attempts);
                            entry.next_attempt = now + backoff.as_secs();
                            self.push(entry).await?;
                            debug!("Webhook retry failed with status {}, rescheduled for {}s later", 
                                  status, backoff.as_secs());
                        } else {
                            // Too many attempts or non-retryable error
                            self.move_to_deadletter(entry).await?;
                        }
                    }
                },
                Ok(Err(e)) => {
                    // Network error
                    entry.attempts += 1;
                    if entry.attempts >= self.max_attempts {
                        self.move_to_deadletter(entry).await?;
                    } else {
                        let backoff = backoff_fn(entry.attempts);
                        entry.next_attempt = now + backoff.as_secs();
                        self.push(entry).await?;
                        debug!("Webhook retry failed with error: {}, rescheduled for {}s later", 
                              e, backoff.as_secs());
                    }
                },
                Err(_) => {
                    // Timeout
                    entry.attempts += 1;
                    if entry.attempts >= self.max_attempts {
                        self.move_to_deadletter(entry).await?;
                    } else {
                        let backoff = backoff_fn(entry.attempts);
                        entry.next_attempt = now + backoff.as_secs();
                        self.push(entry).await?;
                        debug!("Webhook retry timed out, rescheduled for {}s later", backoff.as_secs());
                    }
                }
            }
            
            // Add a small delay between retries to avoid flooding
            tokio::time::sleep(Duration::from_millis(50)).await;
        } else {
            // No more ready entries
            break;
        }
    }
    
    Ok(())
}

    pub async fn process_until_empty(
        &mut self,
        client: &reqwest::Client,
        webhook_secret: &str,
        backoff_fn: impl Fn(u32) -> Duration,
    ) -> Result<(), PulserError> {
        let mut consecutive_empty = 0;
        
        while consecutive_empty < 3 {
            let before_count = match &self.strategy {
                RetryStrategy::Memory(queue) => queue.len(),
                RetryStrategy::Redis { client, queue_key, .. } => {
                    let mut conn = client.get_multiplexed_async_connection().await?;
                    let count: usize = redis::cmd("ZCARD")
                        .arg(queue_key.clone())
                        .query_async(&mut conn)
                        .await?;
                    count
                }
            };
            
            self.retry_failed(client, webhook_secret, &backoff_fn).await?;
            
            let after_count = match &self.strategy {
                RetryStrategy::Memory(queue) => queue.len(),
                RetryStrategy::Redis { client, queue_key, .. } => {
                    let mut conn = client.get_multiplexed_async_connection().await?;
                    let count: usize = redis::cmd("ZCARD")
                        .arg(queue_key.clone())
                        .query_async(&mut conn)
                        .await?;
                    count
                }
            };
            
            if after_count >= before_count {
                consecutive_empty += 1;
            } else {
                consecutive_empty = 0;
            }
            
            // Small pause to avoid hammering
            sleep(Duration::from_millis(100)).await;
        }
        
        Ok(())
    }
}
