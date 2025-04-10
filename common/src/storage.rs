// common/src/storage.rs
// common/src/storage.rs
use serde::{Serialize, Deserialize};
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crate::error::PulserError;
use log::{error, debug, info, warn, trace};
use chrono::Utc;
use bdk_wallet::ChangeSet;
use crate::types::{StableChain, ServiceStatus}; // Added ServiceStatus
use bincode;
use crate::utils;
use std::collections::{HashMap, HashSet};
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use tokio::fs as tokio_fs;  // Use a different name to avoid conflicts
use std::fs;  // Standard library fs
use crate::types::UtxoOrigin;

// Constants
const MUTEX_TIMEOUT_SECS: u64 = 5;
const RWLOCK_TIMEOUT_SECS: u64 = 3;
const FILE_OPERATION_RETRIES: u32 = 3;

#[derive(Debug, Clone)]
pub struct StateManager {
    pub data_dir: PathBuf,
    file_locks: Arc<RwLock<HashMap<String, Arc<Mutex<()>>>>>,
    price_cache: Arc<RwLock<(f64, i64)>>,
    // Add in-memory cache for StableChain data
    chain_cache: Arc<RwLock<HashMap<String, (StableChain, Instant, bool)>>>, // (data, last_access, dirty)
    // Add cache for service status
    status_cache: Arc<RwLock<Option<(ServiceStatus, Instant, bool)>>>, // (data, last_update, dirty)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawalDetails {
    pub withdrawal_id: String,
    pub amount_usd: f64,
    pub remaining_usd: Option<f64>,
    pub executed_timestamp: u64,
    pub change_address: Option<String>,
}

struct TxLockGuard {
    file: std::fs::File,
    path: PathBuf,
}

impl Drop for TxLockGuard {
    fn drop(&mut self) {
        // When the guard is dropped, delete the lock file to release the lock
        if let Err(e) = std::fs::remove_file(&self.path) {
            // Just log errors, don't panic
            warn!("Failed to remove lock file {}: {}", self.path.display(), e);
        }
    }
}

impl StateManager {
pub fn new(data_dir: impl Into<PathBuf>) -> Self {
    Self {
        data_dir: data_dir.into(),
        file_locks: Arc::new(RwLock::new(HashMap::new())),
        price_cache: Arc::new(RwLock::new((0.0, 0))),
        chain_cache: Arc::new(RwLock::new(HashMap::new())),
        status_cache: Arc::new(RwLock::new(None)),
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
    
async fn get_transaction_lock(&self, user_id: &str) -> Result<TxLockGuard, PulserError> {
    let lock_path = self.data_dir.join("locks").join(format!("tx-{}.lock", user_id));
    
    // Create lock directory if it doesn't exist
    if let Some(parent) = lock_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }
    
    // Try to create the lock file - if it already exists, fail
    let file = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path) 
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(PulserError::StorageError(format!(
                "Transaction lock already held for user {}", user_id
            )));
        },
        Err(e) => return Err(PulserError::StorageError(format!(
            "Failed to create lock file: {}", e
        ))),
    };
    
    // Return the lock guard
    Ok(TxLockGuard { file, path: lock_path })
}
    
    pub async fn save<T: Serialize>(&self, file_path: &Path, data: &T) -> Result<(), PulserError> {
        let start_time = Instant::now();
        let full_path = self.data_dir.join(file_path);
        let path_str = full_path.to_str().unwrap_or("unknown");
        let temp_path = full_path.with_extension("temp");

        trace!("Saving to path: {}", path_str);

        if let Some(parent) = full_path.parent() {
            if !parent.exists() {
                tokio_fs::create_dir_all(parent).await?;
            }
        }

        let lock = self.get_file_lock(path_str).await?;
        let _guard = tokio::time::timeout(Duration::from_secs(MUTEX_TIMEOUT_SECS), lock.lock()).await
            .map_err(|_| PulserError::StorageError(format!("Timeout acquiring file lock for {}", path_str)))?;

        let json = serde_json::to_string_pretty(data)?;
        for attempt in 0..FILE_OPERATION_RETRIES {
            match write_file_safely(&temp_path, &json) {
                Ok(_) => break,
                Err(e) => {
                    if attempt == FILE_OPERATION_RETRIES - 1 {
                        error!("Failed to write to temp file {} after {} attempts: {}", 
                               temp_path.display(), FILE_OPERATION_RETRIES, e);
                        return Err(PulserError::StorageError(format!("Failed to write to temp file: {}", e)));
                    }
                    warn!("Retry {} writing to temp file {}: {}", 
                          attempt + 1, temp_path.display(), e);
                    tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                }
            }
        }

        for attempt in 0..FILE_OPERATION_RETRIES {
            match tokio_fs::rename(&temp_path, &full_path).await {
                Ok(_) => break,
                Err(e) => {
                    if attempt == FILE_OPERATION_RETRIES - 1 {
                        error!("Failed to rename temp file to {} after {} attempts: {}", 
                               full_path.display(), FILE_OPERATION_RETRIES, e);
                        return Err(PulserError::StorageError(format!("Failed to rename temp file: {}", e)));
                    }
                    warn!("Retry {} renaming temp file to {}: {}", 
                          attempt + 1, full_path.display(), e);
                    tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                }
            }
        }

        #[cfg(unix)]
        if let Err(e) = tokio_fs::set_permissions(&full_path, Permissions::from_mode(0o600)).await {
            warn!("Failed to set permissions on {}: {}", full_path.display(), e);
        }
        
        #[cfg(not(unix))]
        if let Err(e) = tokio_fs::set_permissions(&full_path, Permissions::from_mode(0o644)).await {
            warn!("Failed to set permissions on {}: {}", full_path.display(), e);
        }

        debug!("Saved {} in {}ms", path_str, start_time.elapsed().as_millis());

        if path_str.contains("stable_chain") {
            if let Ok(chain_data) = serde_json::from_str::<StableChain>(&json) {
                if let Err(e) = utils::validate_stablechain(&chain_data) {
                    warn!("Saved StableChain validation warning: {}", e);
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

        let lock = self.get_file_lock(path_str).await?;
        let _guard = tokio::time::timeout(Duration::from_secs(MUTEX_TIMEOUT_SECS), lock.lock()).await
            .map_err(|_| PulserError::StorageError(format!("Timeout acquiring file lock for reading {}", path_str)))?;

        let mut content = String::new();
        for attempt in 0..FILE_OPERATION_RETRIES {
            match tokio_fs::read_to_string(&full_path).await {
                Ok(file_content) => {
                    content = file_content;
                    break;
                }
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

        let result: T = serde_json::from_str(&content)?;
        
        if path_str.contains("stable_chain") {
            if let Some(chain) = (&result as &dyn std::any::Any).downcast_ref::<StableChain>() {
                if let Err(e) = utils::validate_stablechain(chain) {
                    warn!("Loaded potentially invalid StableChain: {}", e);
                }
            }
        }

        debug!("Loaded {} in {}ms", path_str, start_time.elapsed().as_millis());
        Ok(result)
    }

pub async fn save_stable_chain(&self, user_id: &str, stable_chain: &StableChain) -> Result<(), PulserError> {
    // Validation logic you already have
    if let Err(e) = utils::validate_stablechain(stable_chain) {
        warn!("Validation warning before saving StableChain for user {}: {}", user_id, e);
    }
    
    // Update cache first
    {
        let mut cache = match tokio::time::timeout(
            Duration::from_secs(RWLOCK_TIMEOUT_SECS), 
            self.chain_cache.write()
        ).await {
            Ok(cache) => cache,
            Err(_) => {
                warn!("Timeout acquiring cache write lock for user {}", user_id);
                // Continue with disk save even if we can't update cache
                let sc_path = utils::get_stablechain_path(self.data_dir.to_str().unwrap_or("data"), user_id);
                return self.save(&sc_path, stable_chain).await;
            }
        };
        
        // Add or update cache entry
        cache.insert(user_id.to_string(), (stable_chain.clone(), Instant::now(), true));
    }
    
    // Check if this requires immediate persistence
    let needs_immediate_save = self.needs_immediate_save(stable_chain);
    
    if needs_immediate_save {
        let sc_path = utils::get_stablechain_path(self.data_dir.to_str().unwrap_or("data"), user_id);
        info!("Saving critical StableChain for user {}: {} BTC (${:.2}), {} history entries", 
              user_id, stable_chain.accumulated_btc.to_btc(), stable_chain.stabilized_usd.0, stable_chain.history.len());
        
        let result = self.save(&sc_path, stable_chain).await;
        
        // If save was successful, mark cache entry as clean
        if result.is_ok() {
            if let Ok(mut cache) = self.chain_cache.try_write() {
                if let Some((_, last_access, dirty)) = cache.get_mut(user_id) {
                    *dirty = false;
                }
            }
        }
        
        return result;
    }
    
    // If not immediate save, just return success (will be flushed later)
    Ok(())
}

// Helper method to determine if a change needs immediate persistence
fn needs_immediate_save(&self, chain: &StableChain) -> bool {
    // Check for critical changes that require immediate saving
    if chain.events.iter().any(|e| e.kind == "deposit" || e.kind == "withdrawal" || 
                               e.kind == "reorg" || e.kind == "channel_open") {
        return true;
    }
    
    // Check change log for critical changes
    if let Some(last_change) = chain.change_log.last() {
        match last_change.change_type.as_str() {
            "deposit" | "withdrawal" | "blockchain_reorg" | "force_resync" | "channel_open" => return true,
            _ => {}
        }
    }
    
    false
}

pub async fn load_stable_chain(&self, user_id: &str) -> Result<StableChain, PulserError> {
    // Try to get from cache first
    {
        let mut cache = match tokio::time::timeout(
            Duration::from_secs(RWLOCK_TIMEOUT_SECS), 
            self.chain_cache.write() // Write lock because we update last_access
        ).await {
            Ok(cache) => cache,
            Err(_) => {
                warn!("Timeout acquiring cache write lock for user {}", user_id);
                // Fall back to disk load
                let sc_path = utils::get_stablechain_path(self.data_dir.to_str().unwrap_or("data"), user_id);
                return self.load(&sc_path).await;
            }
        };
        
        if let Some((chain, last_access, _)) = cache.get_mut(user_id) {
            *last_access = Instant::now(); // Update access time
            return Ok(chain.clone());
        }
    }
    
    // Not in cache, load from disk
    let sc_path = utils::get_stablechain_path(self.data_dir.to_str().unwrap_or("data"), user_id);
    let chain: StableChain = self.load(&sc_path).await?;
    
    // Add to cache
    if let Ok(mut cache) = self.chain_cache.try_write() {
        cache.insert(user_id.to_string(), (chain.clone(), Instant::now(), false));
    }
    
    Ok(chain)
}

    pub async fn load_or_init_stable_chain(&self, user_id: &str, sc_dir: &str, multisig_addr: String) -> Result<StableChain, PulserError> {
        let sc_path = utils::get_stablechain_path(self.data_dir.to_str().unwrap_or("data"), user_id);
        if let Ok(chain) = self.load(&sc_path).await {
            return Ok(chain);
        }

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
            change_log: Vec::new(),
            // Initialize new fields
            regular_utxos: Vec::new(),
            change_utxos: Vec::new(),
            change_address_mappings: HashMap::new(),
            stable_value_by_origin: HashMap::new(),
            transaction_history: Vec::new(),
            trusted_service_addresses: HashSet::new(),
        };
        
        debug!("Initialized new StableChain for user {}", user_id);
        utils::validate_stablechain(&stable_chain)?;
        self.save(&sc_path, &stable_chain).await?;
        Ok(stable_chain)
    }

    pub async fn save_changeset(&self, user_id: &str, changeset: &ChangeSet) -> Result<(), PulserError> {
        let path = utils::get_changeset_path(self.data_dir.to_str().unwrap_or("data"), user_id);
        let full_path = self.data_dir.join(&path);
        let temp_path = full_path.with_extension("temp");

        let path_str = full_path.to_str().ok_or_else(|| PulserError::StorageError("Invalid UTF-8 path".to_string()))?;

        trace!("Saving changeset to: {}", path_str);

        if let Some(parent) = full_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        let lock = self.get_file_lock(path_str).await?;
        let _guard = tokio::time::timeout(Duration::from_secs(MUTEX_TIMEOUT_SECS), lock.lock()).await
            .map_err(|_| PulserError::StorageError(format!("Timeout acquiring file lock for changeset {}", path_str)))?;

        let data = bincode::serialize(changeset)?;
        for attempt in 0..FILE_OPERATION_RETRIES {
            match tokio_fs::write(&temp_path, &data).await {
                Ok(_) => break,
                Err(e) => {
                    if attempt == FILE_OPERATION_RETRIES - 1 {
                        error!("Failed to write changeset to temp file {} after {} attempts: {}", 
                               temp_path.display(), FILE_OPERATION_RETRIES, e);
                        return Err(PulserError::StorageError(format!("Failed to write changeset to temp file: {}", e)));
                    }
                    warn!("Retry {} writing changeset to temp file {}: {}", 
                          attempt + 1, temp_path.display(), e);
                    tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                }
            }
        }

        for attempt in 0..FILE_OPERATION_RETRIES {
            match tokio_fs::rename(&temp_path, &full_path).await {
                Ok(_) => break,
                Err(e) => {
                    if attempt == FILE_OPERATION_RETRIES - 1 {
                        error!("Failed to rename changeset temp file to {} after {} attempts: {}", 
                               full_path.display(), FILE_OPERATION_RETRIES, e);
                        return Err(PulserError::StorageError(format!("Failed to rename changeset temp file: {}", e)));
                    }
                    warn!("Retry {} renaming changeset temp file to {}: {}", 
                          attempt + 1, full_path.display(), e);
                    tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                }
            }
        }

        #[cfg(unix)]
        if let Err(e) = tokio_fs::set_permissions(&full_path, Permissions::from_mode(0o600)).await {
            warn!("Failed to set permissions on changeset file {}: {}", full_path.display(), e);
        }
        
        #[cfg(not(unix))]
        if let Err(e) = tokio_fs::set_permissions(&full_path, Permissions::from_mode(0o644)).await {
            warn!("Failed to set permissions on changeset file {}: {}", full_path.display(), e);
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

        let lock = self.get_file_lock(path_str).await?;
        let _guard = tokio::time::timeout(Duration::from_secs(MUTEX_TIMEOUT_SECS), lock.lock()).await
            .map_err(|_| PulserError::StorageError(format!("Timeout acquiring file lock for reading changeset {}", path_str)))?;

        let mut data = Vec::new();
        for attempt in 0..FILE_OPERATION_RETRIES {
            match tokio_fs::read(&full_path).await {
                Ok(file_data) => {
                    data = file_data;
                    break;
                }
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

        let changeset = bincode::deserialize(&data)?;
        debug!("Loaded changeset for user {}", user_id);
        Ok(changeset)
    }
    
    pub async fn flush_dirty_chains(&self) -> Result<usize, PulserError> {
    let mut flushed = 0;
    let dirty_chains = {
        let cache = match tokio::time::timeout(
            Duration::from_secs(RWLOCK_TIMEOUT_SECS), 
            self.chain_cache.read()
        ).await {
            Ok(cache) => cache,
            Err(_) => {
                warn!("Timeout acquiring cache read lock for flush");
                return Ok(0);
            }
        };
        
        // Collect dirty chains
        let mut dirty = Vec::new();
        for (user_id, (chain, _, is_dirty)) in cache.iter() {
            if *is_dirty {
                dirty.push((user_id.clone(), chain.clone()));
            }
        }
        dirty
    };
    
    // Process each dirty chain outside the lock
    for (user_id, chain) in dirty_chains {
        let sc_path = utils::get_stablechain_path(self.data_dir.to_str().unwrap_or("data"), &user_id);
        if let Err(e) = self.save(&sc_path, &chain).await {
            warn!("Failed to flush dirty chain for user {}: {}", user_id, e);
            continue;
        }
        
        // Mark as clean
        if let Ok(mut cache) = self.chain_cache.try_write() {
            if let Some((_, _, dirty)) = cache.get_mut(&user_id) {
                *dirty = false;
                flushed += 1;
            }
        }
    }
    
    Ok(flushed)
}

    pub async fn update_price_cache(&self, price: f64, timestamp: i64) -> Result<(), PulserError> {
        let mut price_cache = tokio::time::timeout(Duration::from_secs(RWLOCK_TIMEOUT_SECS), self.price_cache.write()).await
            .map_err(|_| PulserError::StorageError("Timeout acquiring write lock for price cache".to_string()))?;
        *price_cache = (price, timestamp);

        Ok(())
    }

    pub async fn get_price_cache(&self) -> Result<(f64, i64), PulserError> {
        let cache = tokio::time::timeout(Duration::from_secs(RWLOCK_TIMEOUT_SECS), self.price_cache.read()).await
            .map_err(|_| PulserError::StorageError("Timeout acquiring read lock for price cache".to_string()))?;
        Ok((*cache).clone())
    }

    // Added: Save ServiceStatus to service_status.json
    pub async fn save_service_status(&self, status: &ServiceStatus) -> Result<(), PulserError> {
        let path = PathBuf::from("service_status.json");
        debug!("Updating ServiceStatus: {} users, {} BTC, ${:.8} USD", 
               status.users_monitored, status.total_value_btc, status.total_value_usd);
        info!("Saving ServiceStatus: {} users, {} BTC, ${} USD", 
             status.users_monitored, status.total_value_btc, status.total_value_usd);
        self.save(&path, status).await
    }

    pub async fn load_service_status(&self) -> Result<ServiceStatus, PulserError> {
        let path = PathBuf::from("service_status.json");
        if !self.data_dir.join(&path).exists() {
            let default_status = ServiceStatus {
                up_since: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                last_update: 0,
                version: "unknown".to_string(),
                users_monitored: 0,
                total_utxos: 0,
                total_value_btc: 0.0,
                total_value_usd: 0.0,
                health: "starting".to_string(),
                last_price: 0.0,
                price_update_count: 0,
                active_syncs: 0,
                websocket_active: false,
                utxo_stats: None,
                unusual_activity_detected: false,
                unusual_activity_details: Vec::new(),
                resource_usage: None,  // Added
            performance: None,     // Added
            };
            self.save(&path, &default_status).await?;
            return Ok(default_status);
        }
        match self.load(&path).await {
            Ok(status) => {
                debug!("Loaded ServiceStatus");
                Ok(status)
            }
            Err(e) => {
                warn!("Failed to load ServiceStatus: {}", e);
                Err(e)
            }
        }
    }
    
    pub async fn save_withdrawal_details(&self, user_id: &str, details: &WithdrawalDetails) -> Result<(), PulserError> {
        let path = PathBuf::from(format!("user_{}/withdrawal_{}.json", user_id, details.withdrawal_id));
        self.save(&path, details).await
    }
    
    pub async fn load_withdrawal_details(&self, user_id: &str, withdrawal_id: &str) -> Result<WithdrawalDetails, PulserError> {
        let path = PathBuf::from(format!("user_{}/withdrawal_{}.json", user_id, withdrawal_id));
        self.load(&path).await
    }
    
    pub async fn cleanup_failed_withdrawal(&self, user_id: &str, withdrawal_id: &str) -> Result<(), PulserError> {
        // Implementation that removes orphaned mappings
        let mut chain = self.load_stable_chain(user_id).await?;
        
        // Find and remove any change addresses associated with this withdrawal
        let addresses_to_remove: Vec<String> = chain.change_address_mappings
            .iter()
            .filter(|(_, id)| id == &withdrawal_id)
            .map(|(addr, _)| addr.clone())
            .collect();
        
        for addr in addresses_to_remove {
            chain.change_address_mappings.remove(&addr);
        }
        
        // Save the updated chain
        self.save_stable_chain(user_id, &chain).await?;
        
        // Try to remove the withdrawal details file
        let path = PathBuf::from(format!("user_{}/withdrawal_{}.json", user_id, withdrawal_id));
        let full_path = self.data_dir.join(&path);
        if full_path.exists() {
            if let Err(e) = std::fs::remove_file(&full_path) {
                warn!("Failed to remove withdrawal details file: {}", e);
            }
        }
        
        Ok(())
    }
    
    pub async fn migrate_utxo_info(&self) -> Result<(), PulserError> {
        // Implementation with detailed field migration and error handling
        let users_dir = self.data_dir.join("users");
        if !users_dir.exists() {
            return Ok(());
        }
        
        let entries = std::fs::read_dir(users_dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                if let Some(user_id) = path.file_name().and_then(|n| n.to_str()).and_then(|s| s.strip_prefix("user_")) {
                    if let Ok(mut chain) = self.load_stable_chain(user_id).await {
                        // Perform migration
                        let mut updated = false;
                        
                        // Initialize new fields if needed
                        if chain.regular_utxos.is_empty() && !chain.utxos.is_empty() {
                            chain.regular_utxos = chain.utxos.clone();
                            updated = true;
                        }
                        
                        if chain.change_address_mappings.is_empty() {
                            chain.change_address_mappings = HashMap::new();
                            updated = true;
                        }
                        
                        if chain.stable_value_by_origin.is_empty() {
                            // Initialize with total from stabilized_usd
                            let mut map = HashMap::new();
                            map.insert("Legacy".to_string(), chain.stabilized_usd.clone());
                            chain.stable_value_by_origin = map;
                            updated = true;
                        }
                        
                        if chain.trusted_service_addresses.is_empty() {
                            chain.trusted_service_addresses = HashSet::new();
                            updated = true;
                        }
                        
                       if updated {
                            if let Err(e) = self.save_stable_chain(user_id, &chain).await {
                                warn!("Failed to save migrated chain for user {}: {}", user_id, e);
                            } else {
                                info!("Successfully migrated user {}", user_id);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    pub async fn add_trusted_service_address(&self, user_id: &str, address: &str) -> Result<(), PulserError> {
        let mut chain = self.load_stable_chain(user_id).await?;
        
        // Initialize trusted_service_addresses if it doesn't exist
        if chain.trusted_service_addresses.is_empty() {
            chain.trusted_service_addresses = HashSet::new();
        }
        
        chain.trusted_service_addresses.insert(address.to_string());
        info!("Added trusted service address {} for user {}", address, user_id);
        
        self.save_stable_chain(user_id, &chain).await
    }
    
    pub async fn remove_trusted_service_address(&self, user_id: &str, address: &str) -> Result<(), PulserError> {
        let mut chain = self.load_stable_chain(user_id).await?;
        
        // Remove the address if it exists
        if chain.trusted_service_addresses.remove(address) {
            info!("Removed trusted service address {} for user {}", address, user_id);
            self.save_stable_chain(user_id, &chain).await?;
        }
        
        Ok(())
    }
    
    pub async fn register_withdrawal_change(&self, user_id: &str, 
        change_address: &str, withdrawal_id: &str, remaining_usd: f64
    ) -> Result<(), PulserError> {
        // Acquire transaction lock (implementation may vary)
        let _lock = self.get_transaction_lock(user_id).await?;
        
        // Load chain
        let mut chain = self.load_stable_chain(user_id).await?;
        
        // Register change address
        chain.change_address_mappings.insert(
            change_address.to_string(), 
            withdrawal_id.to_string()
        );
        
        // Save withdrawal details
        let withdrawal_details = WithdrawalDetails {
            withdrawal_id: withdrawal_id.to_string(),
            amount_usd: 0.0,  // Will be updated during actual withdrawal
            remaining_usd: Some(remaining_usd),
            executed_timestamp: chrono::Utc::now().timestamp() as u64,
            change_address: Some(change_address.to_string()),
        };
        
        self.save_withdrawal_details(user_id, &withdrawal_details).await?;
        
        // Save chain
        self.save_stable_chain(user_id, &chain).await?;
        
        Ok(())
    }
} // End of StateManager impl

// Helper function for safe file writes
fn write_file_safely(path: &Path, content: &str) -> io::Result<()> {
    let mut file = match std::fs::File::create(path) {
        Ok(file) => file,
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                    std::fs::File::create(path)?
                } else {
                    return Err(e);
                }
            } else {
                return Err(e);
            }
        }
    };
    file.write_all(content.as_bytes())?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}
