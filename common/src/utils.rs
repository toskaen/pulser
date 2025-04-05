// common/src/utils.rs
use chrono::{DateTime, Utc};
use sha2::{Sha256, Digest};
use std::path::{Path, PathBuf};
use log::warn;
use crate::StableChain;
use crate::error::PulserError;
use crate::types::Amount;
use std::fs;
use crate::types::PriceInfo;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicI64, AtomicBool, Ordering};
use reqwest::Client;
use std::time::Instant;


/// Get current timestamp in seconds
pub fn now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_secs() as i64
}

/// Format timestamp as human-readable string
pub fn format_timestamp(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .unwrap_or_else(|| Utc::now())
        .format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

/// Helper to generate deterministic IDs
pub fn generate_id(data: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data.as_bytes());
    hex::encode(&hasher.finalize()[0..8])
}

/// Helper to safely store sensitive information
pub fn store_sensitive_data(data: &str, file_path: &str) -> Result<(), std::io::Error> {
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::path::Path;
    
    if let Some(parent) = Path::new(file_path).parent() {
        fs::create_dir_all(parent)?;
    }
    
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(file_path)?;
        
    file.write_all(data.as_bytes())?;
    file.sync_all()?;
    
    // Set more restrictive permissions if on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = file.metadata()?;
        let mut perms = metadata.permissions();
        perms.set_mode(0o600); // rw for owner only
        fs::set_permissions(file_path, perms)?;
    }
    
    Ok(())
}

/// Returns the standardized path for a user's StableChain file
pub fn get_stablechain_path(data_dir: &str, user_id: &str) -> PathBuf {
    PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id))
}

pub fn get_user_status_path(data_dir: &str, user_id: &str) -> PathBuf {
    PathBuf::from(format!("user_{}/status_{}.json", user_id, user_id))
}

pub fn get_user_public_path(data_dir: &str, user_id: &str) -> PathBuf {
    PathBuf::from(format!("user_{}/user_{}_public.json", user_id, user_id))
}

pub fn get_changeset_path(data_dir: &str, user_id: &str) -> PathBuf {
    PathBuf::from(format!("user_{}/changeset.bin", user_id))
}

pub fn get_activity_path(data_dir: &str, user_id: &str) -> PathBuf {
    PathBuf::from(format!("user_{}/activity_{}.json", user_id, user_id))
}

pub fn get_multisig_dir(data_dir: &str, user_id: &str) -> PathBuf {
    PathBuf::from(format!("user_{}/multisig", user_id))
}

/// Validates a StableChain to ensure it's not corrupted
pub fn validate_stablechain(chain: &StableChain) -> Result<(), PulserError> {
    if chain.user_id == 0 {
        return Err(PulserError::InvalidInput("Invalid user_id in StableChain".to_string()));
    }
    if chain.accumulated_btc.to_sats() > u64::MAX / 2 {
        return Err(PulserError::InvalidInput("Invalid BTC amount in StableChain".to_string()));
    }
    if chain.stabilized_usd.0 < 0.0 {
        return Err(PulserError::InvalidInput("Invalid USD amount in StableChain".to_string()));
    }
    if chain.raw_btc_usd < 0.0 || chain.raw_btc_usd > 1_000_000.0 {
        warn!("Suspicious BTC/USD price in StableChain: ${:.2}", chain.raw_btc_usd);
    }
    let total_sats: u64 = chain.utxos.iter().map(|u| u.amount).sum();
    if total_sats != chain.accumulated_btc.to_sats() {
        warn!(
            "UTXO total ({}) doesn't match accumulated_btc ({})",
            total_sats, chain.accumulated_btc.to_sats()
        );
    }
    if chain.multisig_addr.is_empty() {
        warn!("Empty multisig address in StableChain");
    }
    Ok(())
}

/// Returns a standardized path for any price-related files
pub fn get_price_history_path(data_dir: &str) -> PathBuf {
    PathBuf::from(format!("{}/price_history.json", data_dir))
}

/// Returns a standardized path for service status
pub fn get_service_status_path(data_dir: &str) -> PathBuf {
    PathBuf::from(format!("{}/service_status.json", data_dir))
}

/// Ensures a user directory exists
pub fn ensure_user_dir(data_dir: &str, user_id: &str) -> Result<(), std::io::Error> {
    let user_dir = PathBuf::from(format!("{}/user_{}", data_dir, user_id));
    if !user_dir.exists() {
        std::fs::create_dir_all(&user_dir)?;
    }
    Ok(())
}

// Add to utils.rs
pub fn write_file_atomically(path: &str, content: &str) -> Result<(), std::io::Error> {
    let temp_path = format!("{}.tmp", path);
    
    // Ensure parent directory exists
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
    }
    
    // Write to temp file
    fs::write(&temp_path, content)?;
    
    // Rename to target (atomic on most filesystems)
    fs::rename(&temp_path, path)?;
    
    // Set permissions if needed
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    
    Ok(())
}

// Convert from simple f64 price to PriceInfo struct
pub fn price_to_price_info(price: f64, source: &str) -> PriceInfo {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64;
    
    let mut price_feeds = HashMap::new();
    price_feeds.insert(source.to_string(), price);
    
    PriceInfo {
        raw_btc_usd: price,
        timestamp: now,
        price_feeds,
    }
}

// In common/src/utils.rs

pub struct CircuitBreaker {
    failures: AtomicUsize,
    last_success: AtomicI64,
    tripped: AtomicBool,
    failure_threshold: usize,
    reset_timeout_secs: u64,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: usize, reset_timeout_secs: u64) -> Self {
        Self {
            failures: AtomicUsize::new(0),
            last_success: AtomicI64::new(0),
            tripped: AtomicBool::new(false),
            failure_threshold,
            reset_timeout_secs,
        }
    }
    
    pub fn record_success(&self) {
        self.failures.store(0, Ordering::SeqCst);
        self.last_success.store(now_timestamp(), Ordering::SeqCst);
        self.tripped.store(false, Ordering::SeqCst);
    }
    
    pub fn record_failure(&self) -> bool {
        let failures = self.failures.fetch_add(1, Ordering::SeqCst) + 1;
        if failures >= self.failure_threshold {
            self.tripped.store(true, Ordering::SeqCst);
            true // Circuit is tripped
        } else {
            false
        }
    }
    
    pub fn is_open(&self) -> bool {
        if !self.tripped.load(Ordering::SeqCst) {
            return false;
        }
        
        // Check if reset timeout has elapsed since last success
        let last_success = self.last_success.load(Ordering::SeqCst);
        let now = now_timestamp();
        
        if now - last_success > self.reset_timeout_secs as i64 {
            self.tripped.store(false, Ordering::SeqCst);
            false
        } else {
            true
        }
    }
}

// In deposit-service/src/monitor.rs - Apply the circuit breaker
pub struct EndpointManager {
    endpoints: Vec<(String, Arc<CircuitBreaker>)>,
    current_index: AtomicUsize,
    client: Client,
}

impl EndpointManager {
    pub fn new(endpoints: Vec<String>, client: Client) -> Self {
        let endpoints = endpoints.into_iter()
            .map(|url| (url, Arc::new(CircuitBreaker::new(5, 60))))
            .collect::<Vec<_>>();
            
        Self {
            endpoints,
            current_index: AtomicUsize::new(0),
            client,
        }
    }
    
    pub fn get_current_endpoint(&self) -> Option<(String, Arc<CircuitBreaker>)> {
        let index = self.current_index.load(Ordering::SeqCst);
        self.endpoints.get(index).cloned()
    }
    
    pub fn get_next_available_endpoint(&self) -> Option<(String, Arc<CircuitBreaker>)> {
        for i in 0..self.endpoints.len() {
            let idx = (self.current_index.load(Ordering::SeqCst) + i) % self.endpoints.len();
            let (_, breaker) = &self.endpoints[idx];
            
            if !breaker.is_open() {
                self.current_index.store(idx, Ordering::SeqCst);
                return self.endpoints.get(idx).cloned();
            }
        }
        None
    }
    
    pub async fn request<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, PulserError> {
        let (endpoint, breaker) = match self.get_next_available_endpoint() {
            Some(endpoint) => endpoint,
            None => return Err(PulserError::ApiError("All endpoints are unavailable".to_string())),
        };
        
        let url = format!("{}{}", endpoint, path);
        match self.client.get(&url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    match response.json::<T>().await {
                        Ok(data) => {
                            breaker.record_success();
                            Ok(data)
                        },
                        Err(e) => {
                            breaker.record_failure();
                            Err(PulserError::ApiError(format!("Failed to parse response: {}", e)))
                        }
                    }
                } else {
                    breaker.record_failure();
                    Err(PulserError::ApiError(format!("API error: {}", response.status())))
                }
            },
            Err(e) => {
                breaker.record_failure();
                Err(PulserError::NetworkError(format!("Request failed: {}", e)))
            }
        }
    }
}
