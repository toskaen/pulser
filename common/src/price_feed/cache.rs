use crate::error::PulserError;
use log::{trace, warn};
use std::fs;
use std::io::Write;
use std::path::Path;
use tokio::time::timeout;
use crate::price_feed::{HISTORY_LOCK, PRICE_CACHE, PriceHistory, MAX_HISTORY_ENTRIES, DEFAULT_CACHE_DURATION_SECS};
use crate::utils::now_timestamp;

pub async fn save_price_history(entries: Vec<PriceHistory>) -> Result<(), PulserError> {
    if entries.is_empty() { return Ok(()); }
    
    let _lock = HISTORY_LOCK.lock().await; // Fixed syntax error with *lock
    let path = std::path::Path::new("data/price_history.json");
    
    // Ensure the data directory exists
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent).map_err(|e| PulserError::StorageError(format!("Failed to create data directory: {}", e)))?;
        }
    }
    
    // Open file in append mode, create it if it doesn't exist
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .map_err(|e| PulserError::StorageError(format!("Failed to open price_history.json: {}", e)))?;
    
    // Append each entry as a JSON line
    for entry in &entries {
        let json_line = serde_json::to_string(entry)? + "\n"; // Add newline for JSONL format
        file.write_all(json_line.as_bytes())
            .map_err(|e| PulserError::StorageError(format!("Failed to write price history: {}", e)))?;
    }
    
    file.flush().map_err(|e| PulserError::StorageError(format!("Failed to flush file: {}", e)))?; // Fixed error handling
    trace!("Appended {} price history entries", entries.len());
    Ok(())
}

pub async fn load_price_history() -> Result<Vec<PriceHistory>, PulserError> {
    let path = std::path::Path::new("data/price_history.json");
    if !path.exists() { return Ok(Vec::new()); }
    
    let content = fs::read_to_string(path)?;
    if content.trim().is_empty() { return Ok(Vec::new()); }
    
    let mut history: Vec<PriceHistory> = content
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok()) // Parse each line
        .collect();
    
    history.sort_by(|a, b| b.timestamp.cmp(&a.timestamp)); // Newest first
    if history.len() > MAX_HISTORY_ENTRIES {
        history.truncate(MAX_HISTORY_ENTRIES);
    }
    
    Ok(history)
}

pub async fn is_price_cache_stale() -> bool {
    let cache = PRICE_CACHE.read().await;
    let now = now_timestamp();
    cache.0 <= 0.0 || (now - cache.1) > DEFAULT_CACHE_DURATION_SECS as i64
}

pub async fn get_cached_price() -> Option<f64> {
    let cache = PRICE_CACHE.read().await;
    if cache.0 > 0.0 { Some(cache.0) } else { None }
}
