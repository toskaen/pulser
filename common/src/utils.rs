// common/src/utils.rs
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Utc};
use sha2::{Sha256, Digest};

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
