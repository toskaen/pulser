use crate::error::PulserError;
use log::{debug, info, warn};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::sleep;

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
                    .get_async_connection()
                    .await
                    .map_err(|e| PulserError::StorageError(format!("Redis error: {}", e)))?;

                let entry_json = serde_json::to_string(&entry)
                    .map_err(|e| PulserError::StorageError(format!("JSON error: {}", e)))?;

                // Store in sorted set with score as next_attempt time
                redis::cmd("ZADD")
                    .arg(queue_key)
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
                    .get_async_connection()
                    .await
                    .map_err(|e| PulserError::StorageError(format!("Redis error: {}", e)))?;

                // Get and remove first entry ready for retry (with score <= now)
                let result: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                    .arg(queue_key)
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
                redis::cmd("ZREM")
                    .arg(queue_key)
                    .arg(entry_json)
                    .query_async::<_, i32>(&mut conn)
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
                    .get_async_connection()
                    .await
                    .map_err(|e| PulserError::StorageError(format!("Redis error: {}", e)))?;

                let entry_json = serde_json::to_string(&entry)
                    .map_err(|e| PulserError::StorageError(format!("JSON error: {}", e)))?;

                // Add to deadletter queue
                redis::cmd("ZADD")
                    .arg(deadletter_key)
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

    pub async fn retry_failed(
        &mut self,
        client: &reqwest::Client,
        webhook_secret: &str,
        backoff_fn: impl Fn(u32) -> Duration,
    ) -> Result<(), PulserError> {
        let now = super::now();
        
        // Process up to 20 items at a time
        for _ in 0..20 {
            if let Some(mut entry) = self.pop_ready(now).await? {
                // Deliver the webhook
                let signature = super::delivery::generate_signature(&entry.payload, webhook_secret);
                
                match super::delivery::deliver_single_webhook(
                    client,
                    &entry.endpoint,
                    &entry.payload,
                    &signature,
                    10, // timeout_secs
                ).await {
                    Ok(_) => {
                        info!("Successfully delivered retry webhook to {}, entry id: {}", entry.endpoint, entry.id);
                    }
                    Err(e) => {
                        entry.attempts += 1;
                        
                        if entry.attempts >= self.max_attempts {
                            // Too many failures, move to deadletter
                            self.move_to_deadletter(entry).await?;
                        } else {
                            // Calculate next attempt time with backoff
                            let backoff = backoff_fn(entry.attempts);
                            entry.next_attempt = now + backoff.as_secs();
                            
                            // Push back to queue
                            self.push(entry).await?;
                            debug!("Webhook retry failed, rescheduled for {}s later", backoff.as_secs());
                        }
                    }
                }
            } else {
                // No more ready webhooks
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
                    let mut conn = client.get_async_connection().await?;
                    redis::cmd("ZCARD")
                        .arg(queue_key)
                        .query_async::<_, usize>(&mut conn)
                        .await?
                }
            };
            
            self.retry_failed(client, webhook_secret, &backoff_fn).await?;
            
            let after_count = match &self.strategy {
                RetryStrategy::Memory(queue) => queue.len(),
                RetryStrategy::Redis { client, queue_key, .. } => {
                    let mut conn = client.get_async_connection().await?;
                    redis::cmd("ZCARD")
                        .arg(queue_key)
                        .query_async::<_, usize>(&mut conn)
                        .await?
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
