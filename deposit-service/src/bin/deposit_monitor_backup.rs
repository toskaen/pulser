// deposit-service/src/bin/deposit_monitor.rs
use bdk_wallet::{Wallet, KeychainKind, Update};
use bdk_esplora::{esplora_client, EsploraAsyncExt};
use bdk_chain::{local_chain::LocalChain, BlockId, spk_client::SyncRequest};
use bitcoin::{Network, BlockHash};
use common::error::PulserError;
use common::types::{USD, Event, PriceInfo, TaprootKeyMaterial};
use common::utils::now_timestamp;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use tokio::sync::{mpsc, broadcast};
use tokio::time::{sleep, timeout};
use log::{info, warn, error, debug, trace};
use warp::Filter;
use reqwest::Client;
use deposit_service::wallet::DepositWallet;
use deposit_service::types::{StableChain, Utxo, Bitcoin};

// File lock singleton for concurrency control
lazy_static::lazy_static! {
    static ref FILE_LOCKS: Arc<Mutex<std::collections::HashMap<String, Arc<Mutex<()>>>>> = 
        Arc::new(Mutex::new(std::collections::HashMap::new()));
    
    // Cache of active monitoring tasks - avoid running the same user sync twice concurrently
    static ref ACTIVE_TASKS: Arc<Mutex<HashMap<String, bool>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

// Constants
const MAX_UTXO_ACTIVITY_ENTRIES: usize = 100;
const DEFAULT_CACHE_DURATION_SECS: u64 = 120; // Harmonized with wallet.rs (120s)
const MAX_WEBHOOK_RETRIES: u32 = 3;
const MAX_CONCURRENT_USERS: usize = 10; // Process this many users at once
const WEBHOOK_TIMEOUT_SECS: u64 = 5;
const DEFAULT_RETRY_MAX: u32 = 3;
const DEFAULT_MAX_RETRY_TIME_SECS: u64 = 120;
const DEFAULT_PRICE_FALLBACK: f64 = 0.0;


/// UTXO information for monitoring and reporting
#[derive(Serialize, Deserialize, Clone)]
struct UtxoInfo {
    txid: String,
    amount_sat: u64,
    address: String,
    keychain: String,
    timestamp: u64,
    confirmations: u32,
    participants: Vec<String>,
    stable_value_usd: f64,
    spendable: bool,
    derivation_path: String,
    vout: u32,
}

/// Service status structure for API reporting
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ServiceStatus {
    up_since: u64,
    last_update: u64,
    version: String,
    users_monitored: u32,
    total_utxos: u32,
    total_value_btc: f64,
    total_value_usd: f64,
    health: String,
    api_status: HashMap<String, bool>,
    last_price: f64,
    price_update_count: u32,
    active_syncs: u32,
    price_cache_staleness_secs: u64,
}

/// User status structure for API reporting
#[derive(Debug, Serialize, Deserialize, Clone)]
struct UserStatus {
    user_id: String,
    last_sync: u64,
    sync_status: String,
    utxo_count: u32,
    total_value_btc: f64,
    total_value_usd: f64,
    confirmations_pending: bool,
    last_update_message: String,
    sync_duration_ms: u64,
    last_error: Option<String>,
    last_success: u64,
    pruned_utxo_count: u32,
}

/// Get file lock for concurrency protection
fn get_file_lock(path: &str) -> Arc<Mutex<()>> {
    let mut locks = FILE_LOCKS.lock().unwrap();
    locks.entry(path.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// Save to file with atomic write operations and retries
fn save_to_file<T: Serialize>(data: &T, file_path: &Path) -> Result<(), PulserError> {
    let temp_path = file_path.with_extension("temp");

    // Create parent directory if it doesn't exist
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| PulserError::StorageError(format!("Failed to create directory: {}", e)))?;
    }

    // Acquire file lock
    let lock_key = file_path.to_string_lossy().to_string();
    let _guard = get_file_lock(&lock_key).lock().unwrap();

    // Retry file operations
    let max_retries = 3;
    let mut last_error = None;

    for retry in 0..max_retries {
        if retry > 0 {
            debug!("Retrying file save ({}/{})", retry + 1, max_retries);
            std::thread::sleep(Duration::from_millis(50 * 2u64.pow(retry as u32)));
        }
        
        // Serialize data
        let json = match serde_json::to_string_pretty(data) {
            Ok(json) => json,
            Err(e) => {
                last_error = Some(PulserError::StorageError(format!("Failed to serialize data: {}", e)));
                continue;
            }
        };
        
        // Write to temp file first
        let mut file = match OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            #[cfg(unix)]
            .mode(0o600) // Secure file permissions on Unix
            .open(&temp_path) {
                Ok(file) => file,
                Err(e) => {
                    last_error = Some(PulserError::StorageError(format!("Failed to create temp file: {}", e)));
                    continue;
                }
            };
        
        if let Err(e) = file.write_all(json.as_bytes()) {
            last_error = Some(PulserError::StorageError(format!("Failed to write to temp file: {}", e)));
            continue;
        }
        
        // Ensure data is written to disk
        if let Err(e) = file.sync_all() {
            last_error = Some(PulserError::StorageError(format!("Failed to sync file: {}", e)));
            continue;
        }
        
        // Rename from temp to final
        if let Err(e) = fs::rename(&temp_path, file_path) {
            last_error = Some(PulserError::StorageError(format!("Failed to rename temp file: {}", e)));
            continue;
        }
        
        return Ok(());
    }

    // All retries failed
    Err(last_error.unwrap_or_else(|| 
        PulserError::StorageError("Failed to save file after retries".to_string())
    ))
}

/// Prune old UTXOs from activity log (keep only last entries)
fn prune_activity_log(activity_path: &Path, max_entries: usize) -> Result<(), PulserError> {
    if !activity_path.exists() {
        return Ok(());
    }

    // Acquire lock
    let lock_key = activity_path.to_string_lossy().to_string();
    let _guard = get_file_lock(&lock_key).lock().unwrap();

    // Read existing activities
    let contents = fs::read_to_string(activity_path)
        .map_err(|e| PulserError::StorageError(format!("Failed to read activity log: {}", e)))?;

    let mut utxos: Vec<UtxoInfo> = serde_json::from_str(&contents)
        .map_err(|e| PulserError::StorageError(format!("Invalid activity log JSON: {}", e)))?;

    // Sort by timestamp (newest first) and keep only max_entries
    utxos.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    if utxos.len() > max_entries {
        utxos.truncate(max_entries);
        
        // Save pruned log
        save_to_file(&utxos, activity_path)?;
        debug!("Pruned activity log to {} entries", max_entries);
    }

    Ok(())
}

/// Check if user is already being processed
fn is_user_active(user_id: &str) -> bool {
    ACTIVE_TASKS.lock().unwrap().contains_key(user_id)
}

/// Mark user as active
fn mark_user_active(user_id: &str) {
    ACTIVE_TASKS.lock().unwrap().insert(user_id.to_string(), true);
}

/// Mark user as inactive
fn mark_user_inactive(user_id: &str) {
    ACTIVE_TASKS.lock().unwrap().remove(user_id);
}

/// Notify external services about new UTXOs with retries
async fn notify_new_utxos(
    client: &Client,
    user_id: &str, 
    new_utxos: &[UtxoInfo],
    webhook_url: &str
) -> Result<(), PulserError> {
    if new_utxos.is_empty() || webhook_url.is_empty() {
        return Ok(());
    }

    let payload = serde_json::json!({
        "event": "new_utxos",
        "user_id": user_id,
        "utxos": new_utxos,
        "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    });

    // Track total retry time
    let start_time = Instant::now();
    let max_duration = Duration::from_secs(DEFAULT_MAX_RETRY_TIME_SECS);

    for retry in 0..MAX_WEBHOOK_RETRIES {
        // Check if we've exceeded max time
        if start_time.elapsed() >= max_duration {
            warn!("Webhook notification exceeded maximum retry time of {}s", DEFAULT_MAX_RETRY_TIME_SECS);
            break;
        }
        
        if retry > 0 {
            let backoff = Duration::from_millis(500 * 2u64.pow(retry));
            debug!("Retrying webhook notification (attempt {})", retry + 1);
            
            sleep(backoff).await;
            
            // Check again after sleep
            if start_time.elapsed() >= max_duration {
                break;
            }
        }
        
        match timeout(
            Duration::from_secs(WEBHOOK_TIMEOUT_SECS),
            client.post(webhook_url).json(&payload).send()
        ).await {
            Ok(result) => match result {
                Ok(response) if response.status().is_success() => {
                    info!("Webhook notification sent for user {}: {} new UTXOs", user_id, new_utxos.len());
                    return Ok(());
                },
                Ok(response) => {
                    warn!("Webhook notification failed with status: {}", response.status());
                    // Continue to retry
                },
                Err(e) => {
                    warn!("Webhook notification error: {}", e);
                    // Continue to retry
                }
            },
            Err(_) => {
                warn!("Webhook notification timed out");
                // Continue to retry
            }
        }
    }

    // All retries failed
    Err(PulserError::NetworkError("All webhook notification attempts failed".to_string()))
}

/// Sync a single user's deposits
async fn sync_user(
    user_id: &str,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_info: PriceInfo,
    esplora_url: String,
    fallback_url: String,
    data_dir: String,
    webhook_url: String,
    client: Client,
) -> Result<(), PulserError> {
    let start_time = Instant::now();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    
    // Update user status to "syncing"
    {
        let mut statuses = user_statuses.lock().unwrap();
        if let Some(status) = statuses.get_mut(user_id) {
            status.sync_status = "syncing".to_string();
            status.last_update_message = format!("Starting sync at {}", now);
            
            // Save updated status
            let status_path = PathBuf::from(format!("{}/status_{}.json", &data_dir, user_id));
            if let Err(e) = save_to_file(status, &status_path) {
                warn!("Failed to save status for user {}: {}", user_id, e);
            }
        } else {
            // Initialize status if not exists
            let new_status = UserStatus {
                user_id: user_id.to_string(),
                last_sync: 0,
                sync_status: "syncing".to_string(),
                utxo_count: 0,
                total_value_btc: 0.0,
                total_value_usd: 0.0,
                confirmations_pending: false,
                last_update_message: format!("Starting sync at {}", now),
                sync_duration_ms: 0,
                last_error: None,
                last_success: 0,
                pruned_utxo_count: 0,
            };
            statuses.insert(user_id.to_string(), new_status.clone());
            
            // Save new status
            let status_path = PathBuf::from(format!("{}/status_{}.json", &data_dir, user_id));
            if let Err(e) = save_to_file(&new_status, &status_path) {
                warn!("Failed to save status for user {}: {}", user_id, e);
            }
        }
    }
    
    debug!("Starting sync for user {} with price ${:.2}", user_id, price_info.raw_btc_usd);
    
    // Check if the wallet exists in the shared map
    let wallet_exists = {
        let wallets_lock = wallets.lock().unwrap();
        wallets_lock.contains_key(user_id)
    };
    
    // If wallet doesn't exist, try to create it
    if !wallet_exists {
        debug!("Wallet for user {} not found, trying to create it", user_id);
        
        // Check if the key file exists
        let key_path = format!("{}/secrets/user_{}_key.json", &data_dir, user_id);
        if !Path::new(&key_path).exists() {
            let error_msg = format!("Key file not found for user {}", user_id);
            error!("{}", error_msg);
            
            // Update user status with error
            let mut statuses = user_statuses.lock().unwrap();
            if let Some(status) = statuses.get_mut(user_id) {
                status.sync_status = "error".to_string();
                status.last_update_message = error_msg.clone();
                status.last_error = Some(error_msg.clone());
                status.sync_duration_ms = start_time.elapsed().as_millis() as u64;
                
                // Save updated status
                let status_path = PathBuf::from(format!("{}/status_{}.json", &data_dir, user_id));
                if let Err(e) = save_to_file(status, &status_path) {
                    warn!("Failed to save status for user {}: {}", user_id, e);
                }
            }
            
            return Err(PulserError::UserNotFound(format!("Key file not found for user {}", user_id)));
        }
        
        // Create a new wallet from config
        let config_path = format!("{}/config/service_config.toml", &data_dir);
        match DepositWallet::from_config(&config_path, user_id) {
            Ok((wallet, _deposit_info, chain)) => {
                // Add to wallets map
                let mut wallets_lock = wallets.lock().unwrap();
                wallets_lock.insert(user_id.to_string(), (wallet, chain));
                
                debug!("Created new wallet for user {}", user_id);
            },
            Err(e) => {
                let error_msg = format!("Failed to create wallet for user {}: {}", user_id, e);
                error!("{}", error_msg);
                
                // Update user status with error
                let mut statuses = user_statuses.lock().unwrap();
                if let Some(status) = statuses.get_mut(user_id) {
                    status.sync_status = "error".to_string();
                    status.last_update_message = error_msg.clone();
                    status.last_error = Some(error_msg);
                    status.sync_duration_ms = start_time.elapsed().as_millis() as u64;
                    
                    // Save updated status
                    let status_path = PathBuf::from(format!("{}/status_{}.json", &data_dir, user_id));
                    if let Err(e) = save_to_file(status, &status_path) {
                        warn!("Failed to save status for user {}: {}", user_id, e);
                    }
                }
                
                return Err(e);
            }
        }
    }
    
    // Get wallet and chain from shared storage
    let (mut wallet, mut chain) = {
        let wallets_lock = wallets.lock().unwrap();
        if let Some((wallet, chain)) = wallets_lock.get(user_id) {
            (wallet.clone(), chain.clone())
        } else {
            let error_msg = format!("Wallet for user {} not found in store", user_id);
            error!("{}", error_msg);
            
            // Update user status with error
            let mut statuses = user_statuses.lock().unwrap();
            if let Some(status) = statuses.get_mut(user_id) {
                status.sync_status = "error".to_string();
                status.last_update_message = error_msg.clone();
                status.last_error = Some(error_msg.clone());
                status.sync_duration_ms = start_time.elapsed().as_millis() as u64;
                
                // Save updated status
                let status_path = PathBuf::from(format!("{}/status_{}.json", &data_dir, user_id));
                if let Err(e) = save_to_file(status, &status_path) {
                    warn!("Failed to save status for user {}: {}", user_id, e);
                }
            }
            
            return Err(PulserError::UserNotFound(error_msg));
        }
    };
    
    // Update wallet's price information
    wallet.last_price = Some(price_info.raw_btc_usd);
    
    // Get previous UTXOs for comparison
    let previous_utxos = chain.utxos.clone();
    
    // Perform the sync
    let sync_result = wallet.monitor_deposits(user_id, &mut chain).await;
    
    match sync_result {
        Ok(()) => {
            debug!("Sync successful for user {}", user_id);
            
            // Check for new UTXOs
            let mut new_utxos = Vec::new();
            for utxo in &chain.utxos {
                if !previous_utxos.iter().any(|u| u.txid == utxo.txid && u.vout == utxo.vout) {
                    let utxo_info = UtxoInfo {
                        txid: utxo.txid.clone(),
                        amount_sat: utxo.amount,
                        address: chain.multisig_addr.clone(),
                        keychain: "External".to_string(),
                        timestamp: now,
                        confirmations: utxo.confirmations,
                        participants: vec![
                            "user".to_string(),
                            "lsp".to_string(),
                            "trustee".to_string(),
                        ],
                        stable_value_usd: utxo.usd_value.as_ref().map_or(0.0, |usd| usd.0),
                        spendable: utxo.confirmations >= 1,
                        derivation_path: "m/84'/1'/0'/0/0".to_string(),
                        vout: utxo.vout,
                    };
                    new_utxos.push(utxo_info);
                }
            }
            
            // Update activity log
            if !new_utxos.is_empty() {
                let activity_path = PathBuf::from(format!("{}/activity_{}.json", &data_dir, user_id));
                
                // Read existing activity log or create new one
                let mut activity = if activity_path.exists() {
                    match fs::read_to_string(&activity_path) {
                        Ok(content) => match serde_json::from_str::<Vec<UtxoInfo>>(&content) {
                            Ok(mut existing) => {
                                // Add new UTXOs to existing activity
                                existing.extend(new_utxos.clone());
                                existing
                            },
                            Err(e) => {
                                warn!("Failed to parse activity log for user {}: {}", user_id, e);
                                new_utxos.clone()
                            }
                        },
                        Err(e) => {
                            warn!("Failed to read activity log for user {}: {}", user_id, e);
                            new_utxos.clone()
                        }
                    }
                } else {
                    new_utxos.clone()
                };
                
                // Sort by timestamp (newest first)
                activity.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
                
                // Prune if needed
                if activity.len() > MAX_UTXO_ACTIVITY_ENTRIES {
                    activity.truncate(MAX_UTXO_ACTIVITY_ENTRIES);
                    debug!("Pruned activity log for user {} to {} entries", user_id, MAX_UTXO_ACTIVITY_ENTRIES);
                }
                
                // Save activity log
                if let Err(e) = save_to_file(&activity, &activity_path) {
                    warn!("Failed to save activity log for user {}: {}", user_id, e);
                }
                
                // Send webhook notification if configured
                if !webhook_url.is_empty() {
                    if let Err(e) = notify_new_utxos(&client, user_id, &new_utxos, &webhook_url).await {
                        warn!("Failed to send webhook notification for user {}: {}", user_id, e);
                    }
                }
            }
            
            // Check for pending confirmations
            let confirmations_pending = chain.utxos.iter().any(|u| u.confirmations == 0);
            
            // Update wallet in storage
            {
                let mut wallets_lock = wallets.lock().unwrap();
                wallets_lock.insert(user_id.to_string(), (wallet, chain.clone()));
            }
            
            // Update user status
            {
                let mut statuses = user_statuses.lock().unwrap();
                if let Some(status) = statuses.get_mut(user_id) {
                    status.sync_status = "success".to_string();
                    status.last_sync = now;
                    status.utxo_count = chain.utxos.len() as u32;
                    status.total_value_btc = chain.accumulated_btc.to_btc();
                    status.total_value_usd = chain.stabilized_usd.0;
                    status.confirmations_pending = confirmations_pending;
                    status.last_update_message = format!("Sync completed successfully at {}", now);
                    status.sync_duration_ms = start_time.elapsed().as_millis() as u64;
                    status.last_error = None;
                    status.last_success = now;
                    
                    // Save updated status
                    let status_path = PathBuf::from(format!("{}/status_{}.json", &data_dir, user_id));
                    if let Err(e) = save_to_file(status, &status_path) {
                        warn!("Failed to save status for user {}: {}", user_id, e);
                    }
                }
            }
            
            info!("User {} sync complete: {} UTXOs, {} BTC (${:.2})", 
                 user_id, chain.utxos.len(), chain.accumulated_btc.to_btc(), chain.stabilized_usd.0);
            Ok(())
        },
        Err(e) => {
            let error_msg = format!("Sync failed for user {}: {}", user_id, e);
            error!("{}", error_msg);
            
            // Update user status with error
            {
                let mut statuses = user_statuses.lock().unwrap();
                if let Some(status) = statuses.get_mut(user_id) {
                    status.sync_status = "error".to_string();
                    status.last_update_message = error_msg.clone();
                    status.last_error = Some(error_msg);
                    status.sync_duration_ms = start_time.elapsed().as_millis() as u64;
                    
                    // Save updated status
                    let status_path = PathBuf::from(format!("{}/status_{}.json", &data_dir, user_id));
                    if let Err(e) = save_to_file(status, &status_path) {
                        warn!("Failed to save status for user {}: {}", user_id, e);
                    }
                }
            }
            
            Err(e)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    // Initialize HTTP client with proper timeouts
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .pool_idle_timeout(Some(Duration::from_secs(30)))
        .build()?;

    // Record start time
    let start_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    // Initialize service status
    let service_status = Arc::new(Mutex::new(ServiceStatus {
        up_since: start_time,
        last_update: 0,
        version: env!("CARGO_PKG_VERSION").to_string(),
        users_monitored: 0,
        total_utxos: 0,
        total_value_btc: 0.0,
        total_value_usd: 0.0,
        health: "starting".to_string(),
        api_status: HashMap::new(),
        last_price: DEFAULT_PRICE_FALLBACK,
        price_update_count: 0,
        active_syncs: 0,
        price_cache_staleness_secs: 0,
    }));

    // Read configuration
    let config_str = fs::read_to_string("config/service_config.toml")
        .map_err(|e| PulserError::ConfigError(format!("Failed to read config file: {}", e)))?;

    let config: toml::Value = toml::from_str(&config_str)
        .map_err(|e| PulserError::ConfigError(format!("Invalid config format: {}", e)))?;

    let esplora_url = config["esplora_url"].as_str()
        .unwrap_or("https://blockstream.info/testnet/api");

    let fallback_url = config["fallback_esplora_url"].as_str()
        .unwrap_or("https://mempool.space/testnet/api");

    let webhook_url = config["webhook_url"].as_str().unwrap_or("");
    let http_port = config["listening_port"].as_integer().unwrap_or(8081) as u16;
    let data_dir = config["data_dir"].as_str().unwrap_or("data").to_string();
    let sync_interval_secs = config["sync_interval_secs"].as_integer().unwrap_or(30) as u64;
    let stagger_delay_secs = config["stagger_delay_secs"].as_integer().unwrap_or(5) as u64;
    let user_scan_interval_secs = config["user_scan_interval_secs"].as_integer().unwrap_or(300) as u64;
    let batch_users = config["batch_users"].as_bool().unwrap_or(false);
    let max_concurrent_users = config["max_concurrent_users"].as_integer().unwrap_or(MAX_CONCURRENT_USERS as i64) as usize;

    info!("Starting deposit monitor service");
    info!("Primary API: {}", esplora_url);
    info!("Fallback API: {}", fallback_url);

    // Create data directory
    fs::create_dir_all(&data_dir)?;

    // Initialize wallets and user statuses
    let wallets = Arc::new(Mutex::new(HashMap::<String, (DepositWallet, StableChain)>::new()));
    let user_statuses = Arc::new(Mutex::new(HashMap::<String, UserStatus>::new()));

    // Setup sync channel to handle events like new users (capacity 100)
    let (sync_tx, mut sync_rx) = mpsc::channel::<String>(100);

    // Start HTTP server
    let user_statuses_clone = user_statuses.clone();
    let service_status_clone = service_status.clone();
    let data_dir_clone = data_dir.clone();
    let sync_tx_clone = sync_tx.clone();

    tokio::spawn(async move {
        // Health endpoint
        let health = warp::path("health").map(|| "OK");
        
        // Service status endpoint
        let status_data = service_status_clone.clone();
        let status = warp::path("status")
            .and(warp::get())
            .and_then(move || {
                let status_clone = status_data.clone();
                async move {
                    let status = status_clone.lock().unwrap().clone();
                    Ok::<_, warp::Rejection>(warp::reply::json(&status))
                }
            });
        
        // User status endpoint
        let user_data = user_statuses_clone.clone();
        let user_status = warp::path("user")
            .and(warp::path::param::<String>())
            .and(warp::get())
            .and_then(move |user_id: String| {
                let user_data_clone = user_data.clone();
                async move {
                    let statuses = user_data_clone.lock().unwrap();
                    match statuses.get(&user_id) {
                        Some(status) => Ok(warp::reply::json(&status)),
                        None => Ok(warp::reply::json(&serde_json::json!({"error": "User not found"}))),
                    }
                }
            });
        
        // Activity log endpoint
        let activity_path = data_dir_clone.clone();
        let activity = warp::path("activity")
            .and(warp::path::param::<String>())
            .and(warp::get())
            .map(move |user_id: String| {
                let file_path = format!("{}/activity_{}.json", activity_path, user_id);
                match fs::read_to_string(&file_path) {
                    Ok(content) => match serde_json::from_str::<Vec<UtxoInfo>>(&content) {
                        Ok(utxos) => warp::reply::json(&utxos),
                        Err(_) => warp::reply::json(&serde_json::json!({"error": "Invalid activity data"})),
                    },
                    Err(_) => warp::reply::json(&serde_json::json!({"error": "Activity not found"})),
                }
            });
            
        // Force sync endpoint (for manual triggering)
        let sync_sender = sync_tx_clone.clone();
        let force_sync = warp::path("force_sync")
            .and(warp::path::param::<String>())
            .and(warp::post())
            .and_then(move |user_id: String| {
                let tx = sync_sender.clone();
                async move {
                    // Check if user is already being processed
                    if is_user_active(&user_id) {
                        return Ok(warp::reply::json(&serde_json::json!({
                            "status": "error", 
                            "message": "User is already being synced"
                        })));
                    }
                    
                    match tx.send(user_id.clone()).await {
                        Ok(_) => Ok(warp::reply::json(&serde_json::json!({
                            "status": "ok", 
                            "message": "Sync triggered"
                        }))),
                        Err(_) => Ok(warp::reply::json(&serde_json::json!({
                            "error": "Failed to trigger sync"
                        }))),
                    }
                }
            });
        
        // Combine routes and start server
        let routes = health.or(status).or(user_status).or(activity).or(force_sync);
        info!("Starting HTTP server on port {}", http_port);
        warp::serve(routes).run(([0, 0, 0, 0], http_port)).await;
    });

    // Initialize last user scan time
    let mut last_user_scan = start_time;

    // Create a channel for shutdown signal
    let (shutdown_tx, mut shutdown_rx) = broadcast::channel::<()>(1);

    // Handle termination signals
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).unwrap();
            let mut sigint = signal(SignalKind::interrupt()).unwrap();
            
            tokio::select! {
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, shutting down gracefully");
                },
                _ = sigint.recv() => {
                    info!("Received SIGINT, shutting down gracefully");
                }
            }
        }
        
        #[cfg(windows)]
        {
            use tokio::signal::windows;
            let mut ctrl_c = windows::ctrl_c().unwrap();
            let mut ctrl_break = windows::ctrl_break().unwrap();
            
            tokio::select! {
                _ = ctrl_c.recv() => {
                    info!("Received Ctrl+C, shutting down gracefully");
                },
                _ = ctrl_break.recv() => {
                    info!("Received Ctrl+Break, shutting down gracefully");
                }
            }
        }
        
        let _ = shutdown_tx.send(());
    });

    // Main monitoring loop
    loop {
        // Check for shutdown signal
        if let Ok(()) = shutdown_rx.try_recv() {
            info!("Shutdown signal received, exiting");
            break;
        }
        
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        
        // Fetch latest price once per monitoring cycle
let price_info = match common::price_feed::fetch_btc_usd_price(&client).await {
    Ok(info) => {
                // Update service status with latest price
        {
            let mut status = service_status.lock().unwrap();
            status.last_price = info.raw_btc_usd;
            status.synthetic_price = info.raw_btc_usd; // No discount - use same price
            status.api_status.insert("price_feed".to_string(), true);
            status.last_update = now;
            status.price_update_count += 1; // Simple counter increment
            status.price_cache_staleness_secs = if common::price_feed::is_price_cache_stale() { 
                common::price_feed::DEFAULT_CACHE_DURATION_SECS  
            } else { 
                0 // Not stale
            };
        }
        info
    },
    Err(e) => {
                error!("Failed to fetch price: {}, using cached value", e);
                // Mark price feed as failed in status
                {
                    let mut status = service_status.lock().unwrap();
                    status.api_status.insert("price_feed".to_string(), false);
                    status.last_update = now;
                    status.price_cache_staleness_secs = price_manager.cache_staleness_secs();
                }
                
                // Use cached value
                price_manager.cache.lock().unwrap().clone()
            }
        };
        
        // Also check for forced sync requests
        let mut forced_users = Vec::new();
        while let Ok(user_id) = sync_rx.try_recv() {
            info!("Forced sync requested for user {}", user_id);
            forced_users.push(user_id);
        }
        
        // Process forced syncs first
        for user_id in forced_users {
            // Skip if already active
            if is_user_active(&user_id) {
                warn!("Skipping forced sync for user {} - already being processed", user_id);
                continue;
            }
            
            // Mark user as active
            mark_user_active(&user_id);
            
            // Update active sync count
            {
                let mut status = service_status.lock().unwrap();
                status.active_syncs += 1;
            }
            
            let sync_result = sync_user(
                &user_id,
                wallets.clone(),
                user_statuses.clone(),
                price_info.clone(),
                esplora_url.to_string(),
                fallback_url.to_string(),
                data_dir.clone(),
                webhook_url.to_string(),
                client.clone(),
            ).await;
            
            // Update active sync count
            {
                let mut status = service_status.lock().unwrap();
                status.active_syncs = status.active_syncs.saturating_sub(1);
            }
            
            // Mark user as inactive
            mark_user_inactive(&user_id);
            
            if let Err(e) = sync_result {
                warn!("Forced sync failed for user {}: {}", user_id, e);
            }
        }
        
        // Scan for new users
        if now - last_user_scan >= user_scan_interval_secs {
            info!("Scanning for new users...");
            
            for entry in fs::read_dir(&data_dir)? {
                let path = entry?.path();
                if path.extension().map_or(false, |e| e == "json") && 
                   path.to_str().unwrap().contains("user_") && 
                   path.to_str().unwrap().contains("_key") {
                    let file_name = path.file_stem().unwrap().to_str().unwrap();
                    // Extracting user ID from file name pattern "user_1_key"
                    if let Some(user_id) = file_name.strip_prefix("user_").and_then(|s| s.split('_').next()) {
                        // Check if user already exists in wallet map
                        {
                            let wallets_lock = wallets.lock().unwrap();
                            if wallets_lock.contains_key(user_id) {
                                continue; // User already exists, skip
                            }
                        }
                        
                        info!("Found new user: {}", user_id);
                        
                        // Initialize with placeholder user status first
                        {
                            let mut user_statuses_lock = user_statuses.lock().unwrap();
                            user_statuses_lock.insert(user_id.to_string(), UserStatus {
                                user_id: user_id.to_string(),
                                last_sync: 0,
                                sync_status: "initializing".to_string(),
                                utxo_count: 0,
                                total_value_btc: 0.0,
                                total_value_usd: 0.0,
                                confirmations_pending: false,
                                last_update_message: "New user detected, initializing".to_string(),
                                sync_duration_ms: 0,
                                last_error: None,
                                last_success: 0,
                                pruned_utxo_count: 0,
                            });
                            
                            // Save user status
                            let status_path = PathBuf::from(format!("{}/status_{}.json", &data_dir, user_id));
                            if let Err(e) = save_to_file(&user_statuses_lock[user_id], &status_path) {
                                warn!("Failed to save initial status for user {}: {}", user_id, e);
                            }
                        }
                        
                        // Queue up for initialization
                        let _ = sync_tx.send(user_id.to_string()).await;
                    }
                }
            }
            
            // Update service status with user count
            {
                let mut service_status_lock = service_status.lock().unwrap();
                service_status_lock.users_monitored = wallets.lock().unwrap().len() as u32;
                service_status_lock.last_update = now;
            }
            
            last_user_scan = now;
            info!("User scan complete. Monitoring {} users.", wallets.lock().unwrap().len());
        }
        
        // Get user IDs to process
        let user_ids: Vec<String> = {
            let wallets_lock = wallets.lock().unwrap();
            wallets_lock.keys()
                .filter(|user_id| !is_user_active(user_id)) // Skip users already being processed
                .cloned()
                .collect()
        };
        
        // Process users
        if batch_users {
            // Batch processing - spawn a limited number of concurrent tasks
            for chunk in user_ids.chunks(max_concurrent_users) {
                let mut tasks = Vec::new();
                
                for user_id in chunk {
                    // Skip if already active
                    if is_user_active(user_id) {
                        continue;
                    }
                    
                    // Mark user as active
                    mark_user_active(user_id);
                    
                    // Update active sync count
                    {
                        let mut status = service_status.lock().unwrap();
                        status.active_syncs += 1;
                    }
                    
                    // Clone needed variables
                    let user_id = user_id.clone();
                    let wallets_clone = wallets.clone();
                    let user_statuses_clone = user_statuses.clone();
                    let price_info_clone = price_info.clone();
                    let esplora_url_clone = esplora_url.to_string();
                    let fallback_url_clone = fallback_url.to_string();
                    let data_dir_clone = data_dir.clone();
                    let webhook_url_clone = webhook_url.to_string();
                    let client_clone = client.clone();
                    let service_status_clone = service_status.clone();
                    
                    // Create task
                    let task = tokio::spawn(async move {
                        let result = sync_user(
                            &user_id,
                            wallets_clone,
                            user_statuses_clone,
                            price_info_clone,
                            esplora_url_clone,
                            fallback_url_clone,
                            data_dir_clone,
                            webhook_url_clone,
                            client_clone,
                        ).await;
                        
                        // Update active sync count
                        {
                            let mut status = service_status_clone.lock().unwrap();
                            status.active_syncs = status.active_syncs.saturating_sub(1);
                        }
                        
                        // Mark user as inactive
                        mark_user_inactive(&user_id);
                        
                        result
                    });
                    
                    tasks.push(task);
                }
                
                // Wait for all tasks in this chunk to complete
                for task in tasks {
                    let _ = task.await;
                }
            }
        } else {
            // Sequential processing with staggered delays
            for (index, user_id) in user_ids.iter().enumerate() {
                // Staggered delay
                if index > 0 && stagger_delay_secs > 0 {
                    sleep(Duration::from_secs(stagger_delay_secs)).await;
                }
                
                // Skip if already active
                if is_user_active(user_id) {
                    continue;
                }
                
                // Mark user as active
                mark_user_active(user_id);
                
                // Update active sync count
                {
                    let mut status = service_status.lock().unwrap();
                    status.active_syncs += 1;
                }
                
                let sync_result = sync_user(
                    user_id,
                    wallets.clone(),
                    user_statuses.clone(),
                    price_info.clone(),
                    esplora_url.to_string(),
                    fallback_url.to_string(),
                    data_dir.clone(),
                    webhook_url.to_string(),
                    client.clone(),
                ).await;
                
                // Update active sync count
                {
                    let mut status = service_status.lock().unwrap();
                    status.active_syncs = status.active_syncs.saturating_sub(1);
                }
                
                // Mark user as inactive
                mark_user_inactive(user_id);
                
                if let Err(e) = sync_result {
                    warn!("Sync failed for user {}: {}", user_id, e);
                }
            }
        }
        
        // Update service status with aggregated data
        {
            let wallets_lock = wallets.lock().unwrap();
            let mut total_utxos = 0;
            let mut total_value_btc = 0.0;
            let mut total_value_usd = 0.0;
            
            for (_, (_, chain)) in wallets_lock.iter() {
                total_utxos += chain.utxos.len() as u32;
                total_value_btc += chain.accumulated_btc.to_btc();
                total_value_usd += chain.stabilized_usd.0;
            }
            
            let mut status = service_status.lock().unwrap();
            status.total_utxos = total_utxos;
            status.total_value_btc = total_value_btc;
            status.total_value_usd = total_value_usd;
            status.last_update = now;
            status.health = "healthy".to_string();
        }
        
        // Sleep until next sync cycle
        debug!("Completed monitoring cycle, sleeping for {} seconds", sync_interval_secs);
        sleep(Duration::from_secs(sync_interval_secs)).await;
    }

    // Graceful shutdown
    info!("Service shutting down");
    Ok(())
}
