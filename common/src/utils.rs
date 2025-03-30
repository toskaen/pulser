// common/src/utils.rs
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Utc};
use sha2::{Sha256, Digest};
use std::path::{Path, PathBuf};
use log::warn;
use crate::StableChain;
use crate::error::PulserError;
use crate::types::Amount;
use std::fs;


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
    PathBuf::from(format!("{}/user_{}/stable_chain_{}.json", data_dir, user_id, user_id))
}

/// Returns the standardized path for a user's status file
pub fn get_user_status_path(data_dir: &str, user_id: &str) -> PathBuf {
    PathBuf::from(format!("{}/user_{}/status_{}.json", data_dir, user_id, user_id))
}

/// Returns the standardized path for a user's public wallet info
pub fn get_user_public_path(data_dir: &str, user_id: &str) -> PathBuf {
    PathBuf::from(format!("{}/user_{}/user_{}_public.json", data_dir, user_id, user_id))
}

/// Returns the standardized path for a user's changeset file
pub fn get_changeset_path(data_dir: &str, user_id: &str) -> PathBuf {
    PathBuf::from(format!("{}/user_{}/changeset.bin", data_dir, user_id))
}

/// Returns the standardized path for a user's activity log
pub fn get_activity_path(data_dir: &str, user_id: &str) -> PathBuf {
    PathBuf::from(format!("{}/user_{}/activity_{}.json", data_dir, user_id, user_id))
}

/// Returns the standardized path for a user's multisig directory
pub fn get_multisig_dir(data_dir: &str, user_id: &str) -> PathBuf {
    PathBuf::from(format!("{}/user_{}/multisig", data_dir, user_id))
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
