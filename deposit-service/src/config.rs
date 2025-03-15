// In deposit-service/src/config.rs
// Add the missing dependencies
use common::PulserError;
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
    
    // HTTP server settings
    pub listening_address: String,
    pub listening_port: u16,
    
    // File storage
    pub data_dir: String,
    pub wallet_dir: String,
    pub event_log_dir: String,
    
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
}
