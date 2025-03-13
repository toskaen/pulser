// deposit-service/src/config.rs
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
}

// Default configuration for testnet
impl Default for Config {
    fn default() -> Self {
        Self {
            service_name: "Pulser Deposit Service".to_string(),
            version: "0.1.0".to_string(),
            network: "testnet".to_string(),
            esplora_url: "https://blockstream.info/testnet/api/".to_string(),
            listening_address: "127.0.0.1".to_string(),
            listening_port: 8081,
            data_dir: "data".to_string(),
            wallet_dir: "wallets".to_string(),
            event_log_dir: "events".to_string(),
            log_level: "info".to_string(),
            lsp_pubkey: "".to_string(),  // This must be set in config file
            trustee_pubkey: "".to_string(), // This must be set in config file
            min_confirmations: 2,   // 2 confirmations for testnet (faster than mainnet)
            channel_threshold_usd: 420.0, // $420 minimum for channel opening
            fee_percentage: 0.21,   // 0.21% maximum fee percentage
            channel_service_url: "http://127.0.0.1:8082".to_string(),
            hedge_service_url: "http://127.0.0.1:8083".to_string(),
            api_key: "test_api_key".to_string(),
            request_timeout_secs: 30,
        }
    }
}

impl Config {
    /// Convert network string to bitcoin::Network enum
    pub fn bitcoin_network(&self) -> Network {
        match self.network.to_lowercase().as_str() {
            "testnet" => Network::Testnet,
            "regtest" => Network::Regtest,
            "signet" => Network::Signet,
            _ => Network::Bitcoin,
        }
    }
    
    /// Get appropriate Esplora URL for the selected network
    pub fn get_esplora_url(&self) -> String {
        if !self.esplora_url.is_empty() {
            return self.esplora_url.clone();
        }
        
        match self.network.to_lowercase().as_str() {
            "testnet" => "https://blockstream.info/testnet/api/".to_string(),
            "regtest" => "http://localhost:3002/".to_string(),
            "signet" => "https://mempool.space/signet/api/".to_string(),
            _ => "https://blockstream.info/api/".to_string(),
        }
    }
    
    /// Get wallet directory path
    pub fn wallet_dir_path(&self) -> String {
        format!("{}/{}", self.data_dir, self.wallet_dir)
    }
    
    /// Get event log directory path
    pub fn event_log_dir_path(&self) -> String {
        format!("{}/{}", self.data_dir, self.event_log_dir)
    }
}

/// Load configuration from a file
pub fn load_config<T: for<'de> Deserialize<'de> + Default>(path: &str) -> Result<T, PulserError> {
    if !Path::new(path).exists() {
        info!("Config file not found: {}. Using defaults.", path);
        return Ok(T::default());
    }

    let config_str = fs::read_to_string(path)
        .map_err(|e| PulserError::ConfigError(format!("Failed to read config file: {}", e)))?;
    
    let config: T = match path.ends_with(".toml") {
        true => toml::from_str(&config_str)
            .map_err(|e| PulserError::ConfigError(format!("Failed to parse TOML: {}", e)))?,
        false => serde_json::from_str(&config_str)
            .map_err(|e| PulserError::ConfigError(format!("Failed to parse JSON: {}", e)))?,
    };
    
    Ok(config)
}

/// Initialize logging based on config
pub fn init_logging(config: &Config, debug_override: bool) {
    let level = if debug_override {
        LevelFilter::Debug
    } else {
        match config.log_level.to_lowercase().as_str() {
            "trace" => LevelFilter::Trace,
            "debug" => LevelFilter::Debug,
            "info" => LevelFilter::Info,
            "warn" => LevelFilter::Warn,
            "error" => LevelFilter::Error,
            _ => LevelFilter::Info,
        }
    };
    
    env_logger::Builder::new()
        .filter_level(level)
        .format_timestamp_millis()
        .init();
        
    info!("Logging initialized at level: {:?}", level);
    info!("Network: {}", config.network);
}

/// Ensure required directories exist
pub fn ensure_directories(config: &Config) -> Result<(), PulserError> {
    // Create main data directory
    if !Path::new(&config.data_dir).exists() {
        fs::create_dir_all(&config.data_dir)
            .map_err(|e| PulserError::StorageError(format!("Failed to create data directory: {}", e)))?;
    }
    
    // Create wallet directory
    let wallet_dir = format!("{}/{}", config.data_dir, config.wallet_dir);
    if !Path::new(&wallet_dir).exists() {
        fs::create_dir_all(&wallet_dir)
            .map_err(|e| PulserError::StorageError(format!("Failed to create wallet directory: {}", e)))?;
    }
    
    // Create event log directory
    let event_log_dir = format!("{}/{}", config.data_dir, config.event_log_dir);
    if !Path::new(&event_log_dir).exists() {
        fs::create_dir_all(&event_log_dir)
            .map_err(|e| PulserError::StorageError(format!("Failed to create event log directory: {}", e)))?;
    }
    
    info!("Directories ensured: {}", config.data_dir);
    
    Ok(())
}
