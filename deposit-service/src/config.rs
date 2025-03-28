// In deposit-service/src/config.rs
use common::error::PulserError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use log::{info, LevelFilter};
use bitcoin::Network;

/// Main configuration struct for deposit service
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
    pub data_dir: String,
    pub wallet_dir: String,

    
    // Logging
    pub log_level: String,
    
    // Bitcoin settings
    pub lsp_pubkey: String,
    pub trustee_pubkey: String,
    pub min_confirmations: u32,
    pub channel_threshold_usd: f64,
    pub fee_percentage: f64,
    
    // Service integration
    pub channel_service_url: String,
    pub hedge_service_url: String,
    pub api_key: String,
    pub request_timeout_secs: u64,
    
    pub role: String,           // Service role (user, lsp, trustee)
    pub lsp_endpoint: String,    // LSP service endpoint
    pub trustee_endpoint: String, // Trustee service endpoint
    pub kraken_btc_address: Option<String>, // Kraken BTC address for USDT withdrawals
    pub ldk_address: String,     // LDK node address for channel opening
    pub max_fee_percent: f64,    // Maximum fee percentage allowed for transactions
    
    // Webhook settings
    pub webhook_url: String,
    pub webhook_max_retries: u32,
    pub webhook_timeout_secs: u64,
    
    // Monitoring settings
    pub sync_interval_secs: u64,
    pub user_scan_interval_secs: u64,
    pub max_concurrent_users: usize,
    
    // Price settings
    pub price_cache_duration_secs: u64,
  }
impl Config {
    pub fn from_file(path: &str) -> Result<Self, PulserError> {
        let config_str = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&config_str)?;
        Ok(config)
    }
}

