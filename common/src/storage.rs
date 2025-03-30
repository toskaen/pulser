// common/src/storage.rs
use serde::{Serialize, Deserialize};
use std::fs;
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock}; // Using both Mutex and RwLock for appropriate scenarios
use std::collections::HashMap;
use std::time::{Duration, Instant};
use crate::error::PulserError;
use log::{error, debug, info, warn, trace};
use chrono::Utc;
use bdk_wallet::ChangeSet;
use crate::types::StableChain;
use bincode;
use crate::utils; // For path utility functions
use std::future::Future;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

// Constants
const MUTEX_TIMEOUT_SECS: u64 = 5;
const RWLOCK_TIMEOUT_SECS: u64 = 3; // Shorter timeout for read locks
const FILE_OPERATION_RETRIES: u32 = 3;

#[derive(Debug, Clone)]
pub struct StateManager {
    pub data_dir: PathBuf,
    // Use RwLock for the master lock map to allow concurrent access
    file_locks: Arc<RwLock<HashMap<String, Arc<Mutex<()>>>>>,
    // Cache frequently accessed data
    price_cache: Arc<RwLock<(f64, i64)>>,
}

impl StateManager {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            file_locks: Arc::new(RwLock::new(HashMap::new())),
            price_cache: Arc::new(RwLock::new((0.0, 0))),
        }
    }

async fn get_file_lock(&self, path: &str) -> Result<Arc<Mutex<()>>, PulserError> {
    let read_result = tokio::time::timeout(
        Duration::from_secs(RWLOCK_TIMEOUT_SECS),
        self.file_locks.read()
    ).await;

    match read_result {
        Ok(locks) => {
            if let Some(lock) = locks.get(path) {
                return Ok(lock.clone());
            }
        }
        Err(_) => {
            warn!("Timeout acquiring read lock for file_locks map for path: {}", path);
            return Err(PulserError::StorageError(format!(
                "Timeout acquiring read lock for file_locks map for {}",
                path
            )));
        }
    };

    // If lock doesn't exist, acquire write lock to create it
    match tokio::time::timeout(Duration::from_secs(MUTEX_TIMEOUT_SECS), self.file_locks.write()).await {
        Ok(mut locks) => {
            Ok(locks
                .entry(path.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone())
        }
        Err(_) => {
            warn!("Timeout acquiring write lock for file_locks map for path: {}", path);
            Err(PulserError::StorageError(format!(
                "Timeout acquiring write lock for file_locks map for {}",
                path
            )))
        }
    }
}

    pub async fn save<T: Serialize>(&self, file_path: &Path, data: &T) -> Result<(), PulserError> {
        let start_time = Instant::now();
        let full_path = self.data_dir.join(file_path);
        let path_str = full_path.to_str().unwrap_or("unknown");
        let temp_path = full_path.with_extension("temp");

        trace!("Saving to path: {}", path_str);

        // Create parent directories if they don't exist
        if let Some(parent) = full_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(|e| {
                    error!("Failed to create directory {}: {}", parent.display(), e);
                    PulserError::StorageError(format!("Failed to create directory: {}", e))
                })?;
            }
        }

        // Get file lock with timeout
        let lock = self.get_file_lock(path_str).await?;

        // Acquire the file lock with timeout
        let _guard = match tokio::time::timeout(
            Duration::from_secs(MUTEX_TIMEOUT_SECS), 
            lock.lock()
        ).await {
            Ok(guard) => guard,
            Err(_) => {
                warn!("Timeout acquiring file lock for {}", path_str);
                return Err(PulserError::StorageError(
                    format!("Timeout acquiring file lock for {}", path_str)))
            }
        };

        // Serialize with retries
        let json = match serde_json::to_string_pretty(data) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize data for {}: {}", path_str, e);
                return Err(PulserError::StorageError(format!("Serialization error: {}", e)));
            }
        };

        // Write to temp file with retries
        for attempt in 0..FILE_OPERATION_RETRIES {
            match write_file_safely(&temp_path, &json) {
                Ok(_) => break,
                Err(e) => {
                    if attempt == FILE_OPERATION_RETRIES - 1 {
                        error!("Failed to write to temp file {} after {} attempts: {}", 
                               temp_path.display(), FILE_OPERATION_RETRIES, e);
                        return Err(PulserError::StorageError(
                            format!("Failed to write to temp file: {}", e)));
                    }
                    warn!("Retry {} writing to temp file {}: {}", 
                          attempt + 1, temp_path.display(), e);
                    tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                }
            }
        }

        // Rename temp file to final file with retries
        for attempt in 0..FILE_OPERATION_RETRIES {
            match fs::rename(&temp_path, &full_path) {
                Ok(_) => break,
                Err(e) => {
                    if attempt == FILE_OPERATION_RETRIES - 1 {
                        error!("Failed to rename temp file to {} after {} attempts: {}", 
                               full_path.display(), FILE_OPERATION_RETRIES, e);
                        return Err(PulserError::StorageError(
                            format!("Failed to rename temp file: {}", e)));
                    }
                    warn!("Retry {} renaming temp file to {}: {}", 
                          attempt + 1, full_path.display(), e);
                    tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                }
            }
        }

        // Set file permissions
        #[cfg(unix)]
        {
            if let Err(e) = fs::set_permissions(&full_path, fs::Permissions::from_mode(0o600)) {
                warn!("Failed to set permissions on {}: {}", full_path.display(), e);
            }
        }
        #[cfg(not(unix))]
        {
            if let Err(e) = fs::set_permissions(&full_path, fs::Permissions::from_mode(0o644)) {
                warn!("Failed to set permissions on {}: {}", full_path.display(), e);
            }
        }

        debug!("Saved {} in {}ms", path_str, start_time.elapsed().as_millis());
        
        // If this is a StableChain, validate it after save
        if path_str.contains("stable_chain") {
            if let Ok(chain) = serde_json::from_str::<StableChain>(&json) {
                if let Err(e) = utils::validate_stablechain(&chain) {
                    warn!("Saved StableChain validation warning: {}", e);
                    // Continue anyway since we want to be lenient with existing data
                }
            }
        }
        
        Ok(())
    }

  pub async fn load<'a, T: for<'de> Deserialize<'de> + 'static>(&'a self, file_path: &'a Path) -> Result<T, PulserError> {

        let start_time = Instant::now();
        let full_path = if file_path.is_absolute() || file_path.starts_with(&self.data_dir) {
            file_path.to_path_buf()
        } else {
            self.data_dir.join(file_path)
        };
        
        let path_str = full_path.to_str().unwrap_or("unknown");
        trace!("Loading from path: {}", path_str);
        
        if !full_path.exists() {
            return Err(PulserError::StorageError(format!("File not found: {}", full_path.display())));
        }
        
        // Get file lock with timeout
        let lock = self.get_file_lock(path_str).await?;
        
        // Acquire the file lock with timeout
        let _guard = match tokio::time::timeout(
            Duration::from_secs(MUTEX_TIMEOUT_SECS), 
            lock.lock()
        ).await {
            Ok(guard) => guard,
            Err(_) => {
                warn!("Timeout acquiring file lock for reading {}", path_str);
                return Err(PulserError::StorageError(
                    format!("Timeout acquiring file lock for reading {}", path_str)))
            }
        };
        
        // Read file with retries
        let mut content = String::new();
        for attempt in 0..FILE_OPERATION_RETRIES {
            match fs::read_to_string(&full_path) {
                Ok(file_content) => {
                    content = file_content;
                    break;
                },
                Err(e) => {
                    if attempt == FILE_OPERATION_RETRIES - 1 {
                        error!("Failed to read file {} after {} attempts: {}", 
                               full_path.display(), FILE_OPERATION_RETRIES, e);
                        return Err(PulserError::StorageError(format!("Failed to read file: {}", e)));
                    }
                    warn!("Retry {} reading file {}: {}", 
                          attempt + 1, full_path.display(), e);
                    tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                }
            }
        }
        
        // Deserialize
        let result: T = match serde_json::from_str(&content) {
            Ok(result) => result,
            Err(e) => {
                error!("Failed to deserialize data from {}: {}", path_str, e);
                return Err(PulserError::StorageError(format!("Deserialization error: {}", e)));
            }
        };
            
        // Optionally validate StableChain if that's what we loaded
        if path_str.contains("stable_chain") {
            if let Some(chain) = (&result as &dyn std::any::Any).downcast_ref::<StableChain>() {
                if let Err(e) = utils::validate_stablechain(chain) {
                    warn!("Loaded potentially invalid StableChain: {}", e);
                    // Continue anyway since we want to be lenient with existing data
                }
            }
        }
        
        debug!("Loaded {} in {}ms", path_str, start_time.elapsed().as_millis());
        Ok(result)
    }

    pub async fn save_stable_chain(&self, user_id: &str, stable_chain: &StableChain) -> Result<(), PulserError> {
        let sc_path = utils::get_stablechain_path(self.data_dir.to_str().unwrap_or("data"), user_id);
        
        info!("Saving StableChain for user {}: {} BTC (${:.2}), {} history entries", 
            user_id, stable_chain.accumulated_btc.to_btc(), stable_chain.stabilized_usd.0, stable_chain.history.len());
        
        self.save(&sc_path, stable_chain).await
    }

    pub async fn load_stable_chain(&self, user_id: &str) -> Result<StableChain, PulserError> {
        let sc_path = utils::get_stablechain_path(self.data_dir.to_str().unwrap_or("data"), user_id);
        
        match self.load(&sc_path).await {
            Ok(chain) => {
                debug!("Loaded StableChain for user {}", user_id);
                Ok(chain)
            },
            Err(e) => {
                debug!("Failed to load StableChain for user {}: {}", user_id, e);
                Err(e)
            }
        }
    }

    pub async fn load_or_init_stable_chain(&self, user_id: &str, sc_dir: &str, multisig_addr: String) -> Result<StableChain, PulserError> {
        let sc_path = utils::get_stablechain_path(self.data_dir.to_str().unwrap_or("data"), user_id);
        
        // First try to load existing chain
        if let Ok(chain) = self.load(&sc_path).await {
            return Ok(chain);
        }
        
        // If loading failed, create a new chain
        let now = Utc::now();
        let stable_chain = StableChain {
            user_id: user_id.parse::<u32>().map_err(|e| PulserError::WalletError(format!("Invalid user_id: {}", e)))?,
            is_stable_receiver: false,
            counterparty: "unknown".to_string(),
            accumulated_btc: crate::types::Bitcoin::from_sats(0),
            stabilized_usd: crate::types::USD(0.0),
            timestamp: now.timestamp(),
            formatted_datetime: now.to_rfc3339(),
            sc_dir: sc_dir.to_string(),
            raw_btc_usd: 0.0,
            prices: HashMap::new(),
            multisig_addr,
            utxos: Vec::new(),
            pending_sweep_txid: None,
            events: Vec::new(),
            total_withdrawn_usd: 0.0,
            expected_usd: crate::types::USD(0.0),
            hedge_position_id: None,
            pending_channel_id: None,
            shorts: Vec::new(),
            hedge_ready: false,
            last_hedge_time: 0,
            short_reduction_amount: None,
            old_addresses: Vec::new(),
            history: Vec::new(),
        };
        
        debug!("Initialized new StableChain for user {}", user_id);
        
        // Try to save the new chain
        self.save(&sc_path, &stable_chain).await?;
        
        Ok(stable_chain)
    }

    pub async fn save_changeset(&self, user_id: &str, changeset: &ChangeSet) -> Result<(), PulserError> {
        let path = utils::get_changeset_path(self.data_dir.to_str().unwrap_or("data"), user_id);
        let full_path = self.data_dir.join(&path);
        let path_str = full_path.to_str().unwrap_or("unknown");
        let temp_path = full_path.with_extension("temp");

        trace!("Saving changeset to: {}", path_str);

        // Create parent directories if they don't exist
        if let Some(parent) = full_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(|e| {
                    error!("Failed to create directory for changeset {}: {}", parent.display(), e);
                    PulserError::StorageError(format!("Failed to create directory: {}", e))
                })?;
            }
        }

        // Get file lock with timeout
        let lock = self.get_file_lock(path_str).await?;

        // Acquire the file lock with timeout
        let _guard = match tokio::time::timeout(
            Duration::from_secs(MUTEX_TIMEOUT_SECS), 
            lock.lock()
        ).await {
            Ok(guard) => guard,
            Err(_) => {
                warn!("Timeout acquiring file lock for changeset {}", path_str);
                return Err(PulserError::StorageError(
                    format!("Timeout acquiring file lock for changeset {}", path_str)))
            }
        };

        // Serialize with bincode
        let data = match bincode::serialize(changeset) {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to serialize changeset for {}: {}", path_str, e);
                return Err(PulserError::StorageError(format!("Changeset serialization error: {}", e)));
            }
        };

        // Write to temp file with retries
        for attempt in 0..FILE_OPERATION_RETRIES {
            match fs::write(&temp_path, &data) {
                Ok(_) => break,
                Err(e) => {
                    if attempt == FILE_OPERATION_RETRIES - 1 {
                        error!("Failed to write changeset to temp file {} after {} attempts: {}", 
                               temp_path.display(), FILE_OPERATION_RETRIES, e);
                        return Err(PulserError::StorageError(
                            format!("Failed to write changeset to temp file: {}", e)));
                    }
                    warn!("Retry {} writing changeset to temp file {}: {}", 
                          attempt + 1, temp_path.display(), e);
                    tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                }
            }
        }

        // Rename temp file to final file with retries
        for attempt in 0..FILE_OPERATION_RETRIES {
            match fs::rename(&temp_path, &full_path) {
                Ok(_) => break,
                Err(e) => {
                    if attempt == FILE_OPERATION_RETRIES - 1 {
                        error!("Failed to rename changeset temp file to {} after {} attempts: {}", 
                               full_path.display(), FILE_OPERATION_RETRIES, e);
                        return Err(PulserError::StorageError(
                            format!("Failed to rename changeset temp file: {}", e)));
                    }
                    warn!("Retry {} renaming changeset temp file to {}: {}", 
                          attempt + 1, full_path.display(), e);
                    tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                }
            }
        }

        // Set file permissions
        #[cfg(unix)]
        {
            if let Err(e) = fs::set_permissions(&full_path, fs::Permissions::from_mode(0o600)) {
                warn!("Failed to set permissions on changeset file {}: {}", full_path.display(), e);
            }
        }
        #[cfg(not(unix))]
        {
            if let Err(e) = fs::set_permissions(&full_path, fs::Permissions::from_mode(0o644)) {
                warn!("Failed to set permissions on changeset file {}: {}", full_path.display(), e);
            }
        }

        debug!("Saved ChangeSet for user {} to {}", user_id, full_path.display());
        Ok(())
    }

    pub async fn load_changeset(&self, user_id: &str) -> Result<ChangeSet, PulserError> {
        let path = utils::get_changeset_path(self.data_dir.to_str().unwrap_or("data"), user_id);
        let full_path = self.data_dir.join(&path);
        let path_str = full_path.to_str().unwrap_or("unknown");
        
        trace!("Loading changeset from: {}", path_str);
        
        if !full_path.exists() {
            return Err(PulserError::StorageError("ChangeSet not found".into()));
        }

        // Get file lock with timeout
        let lock = self.get_file_lock(path_str).await?;
        
        // Acquire the file lock with timeout
        let _guard = match tokio::time::timeout(
            Duration::from_secs(MUTEX_TIMEOUT_SECS), 
            lock.lock()
        ).await {
            Ok(guard) => guard,
            Err(_) => {
                warn!("Timeout acquiring file lock for reading changeset {}", path_str);
                return Err(PulserError::StorageError(
                    format!("Timeout acquiring file lock for reading changeset {}", path_str)))
            }
        };

        // Read file with retries
        let mut data = Vec::new();
        for attempt in 0..FILE_OPERATION_RETRIES {
            match fs::read(&full_path) {
                Ok(file_data) => {
                    data = file_data;
                    break;
                },
                Err(e) => {
                    if attempt == FILE_OPERATION_RETRIES - 1 {
                        error!("Failed to read changeset file {} after {} attempts: {}", 
                               full_path.display(), FILE_OPERATION_RETRIES, e);
                        return Err(PulserError::StorageError(format!("Failed to read changeset file: {}", e)));
                    }
                    warn!("Retry {} reading changeset file {}: {}", 
                          attempt + 1, full_path.display(), e);
                    tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                }
            }
        }

        // Deserialize with bincode
        match bincode::deserialize(&data) {
            Ok(changeset) => {
                debug!("Loaded changeset for user {}", user_id);
                Ok(changeset)
            },
            Err(e) => {
                error!("Failed to deserialize changeset for user {}: {}", user_id, e);
                Err(PulserError::StorageError(e.to_string()))
            }
        }
    }
    
    // Method to update the price cache with RwLock for efficient access
    pub async fn update_price_cache(&self, price: f64, timestamp: i64) -> Result<(), PulserError> {
        match tokio::time::timeout(
            Duration::from_secs(RWLOCK_TIMEOUT_SECS),
            self.price_cache.write()
        ).await {
            Ok(mut cache) => {
                *cache = (price, timestamp);
                Ok(())
            },
            Err(_) => {
                warn!("Timeout acquiring write lock for price cache");
                Err(PulserError::StorageError("Timeout acquiring write lock for price cache".to_string()))
            }
        }
    }
    
    pub async fn get_price_cache(&self) -> Result<(f64, i64), PulserError> {
        match tokio::time::timeout(
            Duration::from_secs(RWLOCK_TIMEOUT_SECS),
            self.price_cache.read()
        ).await {
            Ok(cache) => {
 Ok((cache.0, cache.1))
        },
        Err(_) => {
            warn!("Timeout acquiring read lock for price cache");
            Err(PulserError::StorageError("Timeout acquiring read lock for price cache".to_string()))
        }
    }
}
}

// Helper function for safe file writes
fn write_file_safely(path: &Path, content: &str) -> io::Result<()> {
    let mut file = match fs::File::create(path) {
        Ok(file) => file,
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                    fs::File::create(path)?
                } else {
                    return Err(e);
                }
            } else {
                return Err(e);
            }
        }
    };
    
    // Write and ensure data is flushed to disk
    file.write_all(content.as_bytes())?;
    file.flush()?;
    file.sync_all()?;
    
    Ok(())
}


