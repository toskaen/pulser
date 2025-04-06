// In deposit-service/src/config.rs
use common::error::PulserError;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    // Service identification
    pub service_name: String,
    pub version: String,
    
    // Network settings
    pub network: String,           // "bitcoin", "testnet", or "regtest"
    pub esplora_url: String,
    pub fallback_esplora_url: String,
    
    // HTTP server settings
    pub listening_address: String,
    pub listening_port: u16,
    
    // File storage
    pub data_dir: String,          // Covers stablechain.json, service_status.json
    
    // Logging
    pub log_level: String,
    
    // Bitcoin settings
    pub lsp_pubkey: String,
    pub trustee_pubkey: String,
    pub min_confirmations: u32,    // For 1-conf stabilization
    pub channel_threshold_usd: f64,
    pub fee_percentage: f64,
    
    // Service integration
    pub channel_service_url: String,
    pub hedge_service_url: String,
    pub api_key: String,
    pub request_timeout_secs: u64,
    
    // Redis settings
    #[serde(default = "default_redis_enabled")]
    pub redis_enabled: bool,
    #[serde(default = "default_redis_url")]
    pub redis_url: String,
    
    pub role: String,           // Service role (user, lsp, trustee)
    pub lsp_endpoint: String,
    pub trustee_endpoint: String,
    pub kraken_btc_address: Option<String>,
    pub ldk_address: String,
    pub max_fee_percent: f64,
    
    // Monitoring settings
    pub max_concurrent_users: usize,
    pub monitor: MonitorConfig, // Nested struct
    
    // Price settings
    pub price_feed_url: String,
    pub price_cache_duration_secs: u64,

    // Webhook settings
    #[serde(default = "default_webhook_enabled")]
    pub webhook_enabled: bool,
    pub webhook_url: String,
    #[serde(default = "default_webhook_secret")]
    pub webhook_secret: String,
    #[serde(default = "default_webhook_max_retries")]
    pub webhook_max_retries: u32,
    #[serde(default = "default_webhook_timeout_secs")]
    pub webhook_timeout_secs: u64,
    #[serde(default = "default_webhook_max_retry_time_secs")]
    pub webhook_max_retry_time_secs: u64,
    #[serde(default = "default_webhook_retry_max_attempts")]
    pub webhook_retry_max_attempts: u32,
    #[serde(default = "default_webhook_base_backoff_ms")]
    pub webhook_base_backoff_ms: u64,
    #[serde(default = "default_webhook_jitter_factor")]
    pub webhook_jitter_factor: f64,
    #[serde(default = "default_webhook_batch_size")]
    pub webhook_batch_size: usize,
    #[serde(default = "default_webhook_batch_interval_ms")]
    pub webhook_batch_interval_ms: u64,
    #[serde(default = "default_webhook_max_backoff_secs")]
    pub webhook_max_backoff_secs: u64,
    
    pub deribit_id: String,
    #[serde(default = "default_deribit_secret")]
    pub deribit_secret: String,
    #[serde(default = "default_deribit_testnet_address")]
    pub deribit_testnet_address: String,
    pub kraken: String,
    pub threshold_btc_address: String,
    #[serde(default = "default_hedge_update_interval_secs")]
    pub hedge_update_interval_secs: u64,
    #[serde(default = "default_stagger_delay_secs")]
    pub stagger_delay_secs: u64,
    #[serde(default = "default_max_sync_retries")]
    pub max_sync_retries: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonitorConfig {
    pub deposit_window_hours: u64,
    pub batch_size: usize,
    pub esplora_url: String,
    pub websocket_url: String,
    pub fallback_sync_interval_secs: u64,
    pub websocket_ping_interval_secs: u64,
    pub websocket_reconnect_max_attempts: u32,
    pub websocket_reconnect_base_delay_secs: u64,
    pub max_concurrent_syncs: usize,
    pub sync_batch_timeout_secs: u64,
    pub min_confirmations: u32, // Add this

}

impl MonitorConfig {
    pub fn from_toml(config: &toml::Value) -> Self {
        Self {
            deposit_window_hours: config.get("deposit_window_hours").and_then(|v| v.as_integer()).unwrap_or(24) as u64,
            batch_size: config.get("monitor_batch_size").and_then(|v| v.as_integer()).unwrap_or(15) as usize,
            esplora_url: config.get("esplora_url").and_then(|v| v.as_str()).unwrap_or("https://blockstream.info/testnet/api").to_string(),
            websocket_url: config.get("websocket_url").and_then(|v| v.as_str()).unwrap_or("wss://mempool.space/testnet/api/v1/ws").to_string(),
            fallback_sync_interval_secs: config.get("fallback_sync_interval_secs").and_then(|v| v.as_integer()).unwrap_or(900) as u64,
            websocket_ping_interval_secs: config.get("websocket_ping_interval_secs").and_then(|v| v.as_integer()).unwrap_or(30) as u64,
            websocket_reconnect_max_attempts: config.get("websocket_reconnect_max_attempts").and_then(|v| v.as_integer()).unwrap_or(5) as u32,
            websocket_reconnect_base_delay_secs: config.get("websocket_reconnect_base_delay_secs").and_then(|v| v.as_integer()).unwrap_or(2) as u64,
            max_concurrent_syncs: config.get("max_concurrent_syncs").and_then(|v| v.as_integer()).unwrap_or(5) as usize,
            sync_batch_timeout_secs: config.get("sync_batch_timeout_secs").and_then(|v| v.as_integer()).unwrap_or(60) as u64,
                        min_confirmations: config.get("min_confirmations").and_then(|v| v.as_integer()).unwrap_or(1) as u32,

        }
    }
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, PulserError> {
        let config_str = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&config_str)?;
        Ok(config)
    }
}

// Default functions (unchanged for brevity)
fn default_redis_enabled() -> bool { false }
fn default_redis_url() -> String { "redis://127.0.0.1:6379".to_string() }
fn default_webhook_enabled() -> bool { true }
fn default_webhook_secret() -> String { "your_webhook_secret".to_string() }
fn default_webhook_max_retries() -> u32 { 3 }
fn default_webhook_timeout_secs() -> u64 { 5 }
fn default_webhook_max_retry_time_secs() -> u64 { 120 }
fn default_webhook_retry_max_attempts() -> u32 { 5 }
fn default_webhook_base_backoff_ms() -> u64 { 500 }
fn default_webhook_jitter_factor() -> f64 { 0.25 }
fn default_webhook_batch_size() -> usize { 10 }
fn default_webhook_batch_interval_ms() -> u64 { 100 }
fn default_webhook_max_backoff_secs() -> u64 { 3600 }
fn default_deribit_secret() -> String { "i2ZQLPi6nlnwfQKCHVF0VqYEyC9jTxMBiPfWvSpk7t0".to_string() }
fn default_deribit_testnet_address() -> String { "tb1q_deribit_testnet_dummy".to_string() }
fn default_hedge_update_interval_secs() -> u64 { 30 }
fn default_stagger_delay_secs() -> u64 { 5 }
fn default_max_sync_retries() -> u32 { 3 }
