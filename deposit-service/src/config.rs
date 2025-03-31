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
    
    pub role: String,           // Service role (user, lsp, trustee)
    pub lsp_endpoint: String,
    pub trustee_endpoint: String,
    pub kraken_btc_address: Option<String>,
    pub ldk_address: String,
    pub max_fee_percent: f64,
    
    // Webhook settings
    pub webhook_url: String,
    pub webhook_max_retries: u32,
    pub webhook_timeout_secs: u64,
    
    // Monitoring settings
    pub max_concurrent_users: usize,
    
    // Price settings
    pub price_feed_url: String,        // NEW: For Deribit WebSocket
    pub price_cache_duration_secs: u64,

    #[serde(default = "default_webhook_enabled")]
    pub webhook_enabled: bool,
    #[serde(default = "default_webhook_secret")]
    pub webhook_secret: String,
    #[serde(default = "default_webhook_max_retry_time_secs")]
    pub webhook_max_retry_time_secs: u64,
    #[serde(default = "default_webhook_retry_max_attempts")]
    pub webhook_retry_max_attempts: u32,
    #[serde(default = "default_webhook_base_backoff_ms")]
    pub webhook_base_backoff_ms: u64,
    #[serde(default = "default_webhook_retry_interval_secs")]
    pub webhook_retry_interval_secs: u64,
    
        #[serde(default = "default_deposit_window_hours")]
    pub deposit_window_hours: u64,
    #[serde(default = "default_monitor_batch_size")]
    pub monitor_batch_size: usize,
    #[serde(default = "default_websocket_url")]
    pub websocket_url: String,
    #[serde(default = "default_fallback_sync_interval_secs")]
    pub fallback_sync_interval_secs: u64,
    #[serde(default = "default_websocket_ping_interval_secs")]
    pub websocket_ping_interval_secs: u64,
    #[serde(default = "default_websocket_reconnect_max_attempts")]
    pub websocket_reconnect_max_attempts: u32,
    #[serde(default = "default_websocket_reconnect_base_delay_secs")]
    pub websocket_reconnect_base_delay_secs: u64,
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

impl Config {
    pub fn from_file(path: &str) -> Result<Self, PulserError> {
        let config_str = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&config_str)?;
        Ok(config)
    }
}

fn default_webhook_enabled() -> bool { true }
fn default_webhook_secret() -> String { "your_webhook_secret".to_string() }
fn default_webhook_max_retry_time_secs() -> u64 { 120 }
fn default_webhook_retry_max_attempts() -> u32 { 5 }
fn default_webhook_base_backoff_ms() -> u64 { 500 }
fn default_webhook_retry_interval_secs() -> u64 { 5 }
fn default_deposit_window_hours() -> u64 { 24 }
fn default_monitor_batch_size() -> usize { 15 }
fn default_websocket_url() -> String { "wss://mempool.space/testnet/api/v1/ws".to_string() }
fn default_fallback_sync_interval_secs() -> u64 { 60 }
fn default_websocket_ping_interval_secs() -> u64 { 30 }
fn default_websocket_reconnect_max_attempts() -> u32 { 5 }
fn default_websocket_reconnect_base_delay_secs() -> u64 { 2 }
fn default_deribit_id() -> String { "kYlR7m6s".to_string() }
fn default_deribit_secret() -> String { "i2ZQLPi6nlnwfQKCHVF0VqYEyC9jTxMBiPfWvSpk7t0".to_string() }
fn default_deribit_testnet_address() -> String { "tb1q_deribit_testnet_dummy".to_string() }
fn default_hedge_update_interval_secs() -> u64 { 30 }
fn default_stagger_delay_secs() -> u64 { 5 }
fn default_max_sync_retries() -> u32 { 3 }
