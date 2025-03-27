// deposit-service/src/storage.rs
use serde::{Serialize, Deserialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;
use common::error::PulserError;
use crate::types::{StableChain, UtxoInfo as TypesUtxoInfo, Bitcoin};
use log::debug;
use chrono::Utc;
use std::str::FromStr;
use common::types::USD;


#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[derive(Debug, Clone)]
pub struct StateManager {
    // fields...

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
    // Check if file_path is already an absolute path or starts with the data directory
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
    let data = serde_json::from_str(&content)?;
    
    Ok(data)
}

    pub async fn prune_activity_log(&self, activity_path: &Path, max_entries: usize) -> Result<(), PulserError> {
        let full_path = self.data_dir.join(activity_path);
        if !full_path.exists() {
            return Ok(());
        }

        let lock = self.get_file_lock(full_path.to_str().unwrap_or("unknown")).await;
        let _guard = lock.lock().await;

        let content = fs::read_to_string(&full_path)?;
        let mut utxos: Vec<UtxoInfo> = serde_json::from_str(&content)?;
        utxos.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        if utxos.len() > max_entries {
            utxos.truncate(max_entries);
            self.save(activity_path, &utxos).await?;
            debug!("Pruned activity log at {} to {} entries", full_path.display(), max_entries);
        }
        Ok(())
    }

    pub async fn save_stable_chain(&self, user_id: &str, stable_chain: &StableChain) -> Result<(), PulserError> {
        let sc_path = PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id));
        self.save(&sc_path, stable_chain).await?;
        debug!("Saved StableChain for user {} to {}", user_id, sc_path.display());
        Ok(())
    }

    pub async fn load_stable_chain(&self, user_id: &str) -> Result<StableChain, PulserError> {
        let sc_path = PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id));
        self.load(&sc_path).await
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
                accumulated_btc: Bitcoin::from_sats(0),
                stabilized_usd: USD(0.0),
                timestamp: now.timestamp(),
                formatted_datetime: now.to_rfc3339(),
                sc_dir: sc_dir.to_string(),
                raw_btc_usd: 0.0,
                prices: HashMap::new(),
                multisig_addr: multisig_addr,
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
            self.save(&sc_path, &stable_chain).await?;
            Ok(stable_chain)
        }
    }
}

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
