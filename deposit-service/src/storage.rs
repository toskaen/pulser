// deposit-service/src/storage.rs
use serde::{Serialize, Deserialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;
use common::error::PulserError;
use log::debug;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

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
        let full_path = self.data_dir.join(file_path);
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
            self.save(&activity_path, &utxos).await?;
            debug!("Pruned activity log at {} to {} entries", full_path.display(), max_entries);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone)]
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
