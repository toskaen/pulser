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
    pub sync_interval_secs: u64,       // System-wide sync
    pub user_scan_interval_secs: u64,  // User monitoring interval
    pub max_concurrent_users: usize,
    
    // Price settings
    pub price_feed_url: String,        // NEW: For Deribit WebSocket
    pub price_cache_duration_secs: u64,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, PulserError> {
        let config_str = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&config_str)?;
        Ok(config)
    }
}
