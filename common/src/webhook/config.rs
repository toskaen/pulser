use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub enabled: bool,
    pub secret: String,
    pub max_retries: u32,
    pub timeout_secs: u64,
    pub max_retry_time_secs: u64,
    pub retry_max_attempts: u32,
    pub base_backoff_ms: u64,
    pub max_backoff_secs: u64,
    pub jitter_factor: f64,
    pub batch_size: usize,
    pub batch_interval_ms: u64,
}

impl WebhookConfig {
    pub fn new(secret: &str) -> Self {
        Self {
            enabled: true,
            secret: secret.to_string(),
            max_retries: 5,
            timeout_secs: 10,
            max_retry_time_secs: 3600, // 1 hour
            retry_max_attempts: 10,
            base_backoff_ms: 500,
            max_backoff_secs: 3600,
            jitter_factor: 0.25,
            batch_size: 10,
            batch_interval_ms: 100,
        }
    }

    pub fn from_toml(config: &toml::Value) -> Self {
        Self {
            enabled: config
                .get("webhook_enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            secret: config
                .get("webhook_secret")
                .and_then(|v| v.as_str())
                .unwrap_or("default_secret")
                .to_string(),
            max_retries: config
                .get("webhook_max_retries")
                .and_then(|v| v.as_integer())
                .unwrap_or(5) as u32,
            timeout_secs: config
                .get("webhook_timeout_secs")
                .and_then(|v| v.as_integer())
                .unwrap_or(10) as u64,
            max_retry_time_secs: config
                .get("webhook_max_retry_time_secs")
                .and_then(|v| v.as_integer())
                .unwrap_or(3600) as u64,
            retry_max_attempts: config
                .get("webhook_retry_max_attempts")
                .and_then(|v| v.as_integer())
                .unwrap_or(10) as u32,
            base_backoff_ms: config
                .get("webhook_base_backoff_ms")
                .and_then(|v| v.as_integer())
                .unwrap_or(500) as u64,
            max_backoff_secs: config
                .get("webhook_max_backoff_secs")
                .and_then(|v| v.as_integer())
                .unwrap_or(3600) as u64,
            jitter_factor: config
                .get("webhook_jitter_factor")
                .and_then(|v| v.as_float())
                .unwrap_or(0.25),
            batch_size: config
                .get("webhook_batch_size")
                .and_then(|v| v.as_integer())
                .unwrap_or(10) as usize,
            batch_interval_ms: config
                .get("webhook_batch_interval_ms")
                .and_then(|v| v.as_integer())
                .unwrap_or(100) as u64,
        }
    }

    pub fn get_timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }

    pub fn calculate_backoff(&self, attempt: u32) -> Duration {
        // Exponential backoff with jitter
        let base_ms = self.base_backoff_ms;
        let max_ms = self.max_backoff_secs * 1000;
        let exponent = attempt as u32;

        let backoff_ms = std::cmp::min(
            base_ms * 2u64.saturating_pow(exponent),
            max_ms,
        );

        // Add jitter
        let jitter_ms = (backoff_ms as f64 * self.jitter_factor * rand::random::<f64>()) as u64;
        Duration::from_millis(backoff_ms + jitter_ms)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookEndpoint {
    pub url: String,
    pub service: String,
    pub rate_limit_per_min: u32,
    pub headers: Option<std::collections::HashMap<String, String>>,
}
