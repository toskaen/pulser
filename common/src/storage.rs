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
use bdk_wallet::ChangeSet;
use crate::types::StableChain;
use bincode; // Add this

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[derive(Debug, Clone)]
pub struct StateManager {
    pub data_dir: PathBuf,
    file_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl StateManager {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            file_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn get_file_lock(&self, path: &str) -> Arc<Mutex<()>> {
        let mut locks = self.file_locks.lock().await;
        locks.entry(path.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub async fn save<T: Serialize>(&self, file_path: &Path, data: &T) -> Result<(), PulserError> {
        let full_path = self.data_dir.join(file_path);
        let temp_path = full_path.with_extension("temp");

        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let lock = self.get_file_lock(full_path.to_str().unwrap_or("unknown")).await;
        let _guard = lock.lock().await;

        let json = serde_json::to_string_pretty(data)?;
        fs::write(&temp_path, json)?;
        fs::rename(&temp_path, &full_path)?;

        #[cfg(unix)]
        fs::set_permissions(&full_path, fs::Permissions::from_mode(0o600))?;
        #[cfg(not(unix))]
        fs::set_permissions(&full_path, fs::Permissions::from_mode(0o644))?;

        Ok(())
    }

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

    pub async fn save_stable_chain(&self, user_id: &str, stable_chain: &StableChain) -> Result<(), PulserError> {
        let sc_path = PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id));
        info!("Saving StableChain for user {}: {} BTC (${:.2}), {} history entries", 
            user_id, stable_chain.accumulated_btc.to_btc(), stable_chain.stabilized_usd.0, stable_chain.history.len());
        
        let full_path = self.data_dir.join(&sc_path);
        if let Some(parent) = full_path.parent() {
            if !parent.exists() {
                debug!("Creating parent directory: {}", parent.display());
                std::fs::create_dir_all(parent)?;
            }
        }
        
        match self.save(&sc_path, stable_chain).await {
            Ok(_) => {
                debug!("Successfully saved StableChain to {}", full_path.display());
                Ok(())
            }
            Err(e) => {
                error!("Failed to save StableChain for user {}: {}", user_id, e);
                Err(e)
            }
        }
    }

    pub async fn load_stable_chain(&self, user_id: &str) -> Result<StableChain, PulserError> {
        let sc_path = PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id));
        let chain = self.load(&sc_path).await?;
        debug!("Loaded StableChain for user {}", user_id);
        Ok(chain)
    }

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
            self.save(&sc_path, &stable_chain).await?;
            Ok(stable_chain)
        }
    }

pub async fn save_changeset(&self, user_id: &str, changeset: &ChangeSet) -> Result<(), PulserError> {
    let path = PathBuf::from(format!("user_{}/changeset.bin", user_id));
    let full_path = self.data_dir.join(&path);
    let temp_path = full_path.with_extension("temp");

    if let Some(parent) = full_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let lock = self.get_file_lock(full_path.to_str().unwrap_or("unknown")).await;
    let _guard = lock.lock().await;

    let data = bincode::serialize(changeset)?;
    fs::write(&temp_path, &data)?;
    fs::rename(&temp_path, &full_path)?;

    #[cfg(unix)]
    fs::set_permissions(&full_path, fs::Permissions::from_mode(0o600))?;
    #[cfg(not(unix))]
    fs::set_permissions(&full_path, fs::Permissions::from_mode(0o644))?;

    debug!("Saved ChangeSet for user {} to {}", user_id, full_path.display());
    Ok(())
}

pub async fn load_changeset(&self, user_id: &str) -> Result<ChangeSet, PulserError> {
    let path = PathBuf::from(format!("user_{}/changeset.bin", user_id));
    let full_path = self.data_dir.join(&path);
    if !full_path.exists() {
        return Err(PulserError::StorageError("ChangeSet not found".into()));
    }

    let lock = self.get_file_lock(full_path.to_str().unwrap_or("unknown")).await;
    let _guard = lock.lock().await;

    let data = fs::read(&full_path)?;
    bincode::deserialize(&data).map_err(|e| PulserError::StorageError(e.to_string()))
}
}
