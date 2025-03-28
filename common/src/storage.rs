// common/src/storage.rs
use serde::{Serialize, Deserialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;
use crate::error::PulserError;
use log::{error, debug, info, warn};
use chrono::Utc;
use std::str::FromStr;
use crate::types::{StableChain, USD, Bitcoin, UtxoInfo as TypesUtxoInfo};



#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

/// StateManager handles persistent storage operations with atomic file guarantees
#[derive(Debug, Clone)]
pub struct StateManager {
    pub data_dir: PathBuf,
    file_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl StateManager {
    /// Creates a new StateManager with the specified data directory
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            file_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Gets or creates a lock for a specific file path
    async fn get_file_lock(&self, path: &str) -> Arc<Mutex<()>> {
        let mut locks = self.file_locks.lock().await;
        locks.entry(path.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Atomically saves state to JSON file using temp file pattern.
    pub async fn save<T: Serialize>(&self, file_path: &Path, data: &T) -> Result<(), PulserError> {
        let full_path = self.data_dir.join(file_path);
        let temp_path = full_path.with_extension("temp");

        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let lock = self.get_file_lock(full_path.to_str().unwrap_or("unknown")).await;
        let _guard = lock.lock().await;

        let json = serde_json::to_string_pretty(data)?;
        fs::write(&temp_path, json).map_err(|e| {
            warn!("Save failed for {}: {}", full_path.display(), e);
            PulserError::StorageError(e.to_string())
        })?;
        fs::rename(&temp_path, &full_path)?;

        #[cfg(unix)]
        fs::set_permissions(&full_path, fs::Permissions::from_mode(0o600))?;
        #[cfg(not(unix))]
        fs::set_permissions(&full_path, fs::Permissions::from_mode(0o644))?;

        Ok(())
    }

    /// Loads state from JSON file with file locking.
    pub async fn load<T: for<'de> Deserialize<'de>>(&self, file_path: &Path) -> Result<T, PulserError> {
        let full_path = if file_path.is_absolute() || file_path.starts_with(&self.data_dir) {
            file_path.to_path_buf()
        } else {
            self.data_dir.join(file_path)
        };
        
        if !full_path.exists() {
            return Err(PulserError::StorageError(format!("File not found: {}", full_path.display())));
        }
        
        let lock = self.get_file_lock(full_path.to_str().unwrap_or("unknown")).await;
        let _guard = lock.lock().await;
        
        let content = fs::read_to_string(&full_path)?;
        serde_json::from_str(&content).map_err(|e| PulserError::StorageError(e.to_string()))
    }

    /// Saves StableChain data for a specific user with additional debug logging
// In common/src/storage.rs:

pub async fn save_stable_chain(&self, user_id: &str, stable_chain: &StableChain) -> Result<(), PulserError> {
    let sc_path = PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id));
    
    info!("Saving StableChain for user {}: {} BTC (${:.2}), {} history entries", 
        user_id, stable_chain.accumulated_btc.to_btc(), stable_chain.stabilized_usd.0, stable_chain.history.len());
    
    // Ensure directory exists
    let full_path = self.data_dir.join(&sc_path);
    if let Some(parent) = full_path.parent() {
        if !parent.exists() {
            debug!("Creating parent directory for StableChain: {}", parent.display());
            std::fs::create_dir_all(parent)?;
        }
    }
    
    // Save with comprehensive error handling
    match self.save(&sc_path, stable_chain).await {
        Ok(_) => {
            debug!("Successfully saved StableChain to {}", full_path.display());
            Ok(())
        }
        Err(e) => {
            error!("Failed to save StableChain for user {} to {}: {}", 
                user_id, full_path.display(), e);
            Err(e)
        }
    }
}

    /// Loads StableChain data for a specific user
    pub async fn load_stable_chain(&self, user_id: &str) -> Result<StableChain, PulserError> {
        let sc_path = PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id));
        let chain = self.load(&sc_path).await?;
        debug!("Loaded StableChain for user {}", user_id);
        Ok(chain)
    }

    /// Loads or initializes StableChain data for a user
    pub async fn load_or_init_stable_chain(&self, user_id: &str, sc_dir: &str, multisig_addr: String) -> Result<StableChain, PulserError> {
        let sc_path = PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id));
        if sc_path.exists() {
            self.load(&sc_path).await
        } else {
            let now = Utc::now();
            let stable_chain = StableChain {
                user_id: user_id.parse::<u32>().map_err(|e| PulserError::WalletError(format!("Invalid user_id: {}", e)))?,
                is_stable_receiver: false,
                counterparty: "unknown".to_string(),
                accumulated_btc: Bitcoin::from_sats(0),
                stabilized_usd: USD(0.0),
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
                expected_usd: USD(0.0),
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
            self.save(&sc_path, &stable_chain).await?;
            Ok(stable_chain)
        }
    }
}

/// Represents information about a UTXO with metadata for tracking and stabilization
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UtxoInfo {
    pub txid: String,
    pub amount_sat: u64,
    pub address: String,
    pub keychain: String,
    pub timestamp: u64,
    pub confirmations: u32,
    pub participants: Vec<String>,
    pub stable_value_usd: f64,
    pub spendable: bool,
    pub derivation_path: String,
    pub vout: u32,
    pub spent: bool,
}
