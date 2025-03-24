// deposit-service/src/bin/deposit_monitor.rs
use tokio::task;
use bdk_wallet::{Wallet, KeychainKind, Update};
use bdk_esplora::{esplora_client, EsploraAsyncExt};
use bdk_chain::{local_chain::LocalChain, BlockId, spk_client::SyncRequest};
use bitcoin::{Network, BlockHash};
use common::error::PulserError;
use common::types::{USD, Event, PriceInfo, TaprootKeyMaterial};
use common::utils::now_timestamp;
use common::price_feed::fetch_btc_usd_price;
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
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[derive(Debug)]
enum CustomError {
    Serde(serde_json::Error),
    Io(std::io::Error),
}

impl warp::reject::Reject for CustomError {}

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

/// Save to file with atomic write operations
fn save_to_file<T: Serialize>(data: &T, file_path: &Path) -> Result<(), PulserError> {
    let temp_path = file_path.with_extension("temp");
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent)?;
    }
    
    // Use the file_path parameter directly
    let lock_key = file_path.to_str().unwrap_or("unknown");
    let lock = get_file_lock(lock_key);
    let _guard = lock.lock().unwrap();
    
    let json = serde_json::to_string_pretty(data)?;
    fs::write(&temp_path, json)?;
    fs::rename(&temp_path, file_path)?;
    
    #[cfg(unix)]
    fs::set_permissions(file_path, fs::Permissions::from_mode(0o600))?;
    
    #[cfg(not(unix))]
    fs::set_permissions(file_path, fs::Permissions::from_mode(0o644))?;
    
    Ok(())
}

/// Prune old UTXOs from activity log (keep only last entries)
fn prune_activity_log(activity_path: &Path, max_entries: usize) -> Result<(), PulserError> {
    if !activity_path.exists() {
        return Ok(());
    }

    // Acquire lock using the activity_path
    let lock_key = activity_path.to_str().unwrap_or("unknown");
    let lock = get_file_lock(lock_key);
    let _guard = lock.lock().unwrap();

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
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    assert!(tokio::runtime::Handle::try_current().is_ok(), "Must run within Tokio runtime");

    // Update user status to "syncing"
    {
        let mut statuses = user_statuses.lock().unwrap();
        if let Some(status) = statuses.get_mut(user_id) {
            status.sync_status = "syncing".to_string();
            status.last_update_message = format!("Starting sync at {}", now);
            let status_path = PathBuf::from(format!("{}/status_{}.json", &data_dir, user_id));
            if let Err(e) = save_to_file(status, &status_path) {
                warn!("Failed to save status for user {}: {}", user_id, e);
            }
        } else {
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

    // If wallet doesn't exist, create it
    // Fixed type mismatch by making sure both blocks return the same types
    let (mut wallet, mut chain) = if !wallet_exists {
        let config_path = format!("{}/config/service_config.toml", &data_dir);
        match DepositWallet::from_config(&config_path, user_id).await {
            Ok((wallet, _deposit_info, chain)) => (wallet, chain),
            Err(e) => {
                let error_msg = format!("Failed to create wallet for user {}: {}", user_id, e);
                error!("{}", error_msg);
                return Err(PulserError::WalletError(error_msg));
            }
        }
    } else {
        // Clone the values from the reference
        let wallets_lock = wallets.lock().unwrap();
        let (wallet_ref, chain_ref) = wallets_lock.get(user_id).unwrap();
        (wallet_ref.clone(), chain_ref.clone())
    };

    // Get previous UTXOs for comparison
    let previous_utxos = chain.utxos.clone();

    // Perform the sync
    wallet.sync().await?;
    wallet.update_stable_chain()?;

    // Check for new UTXOs
    let mut new_utxos = Vec::new();
    for utxo in &chain.utxos {
        if !previous_utxos.iter().any(|u| u.txid == utxo.txid && u.vout == utxo.vout) {
            let utxo_info = UtxoInfo {
                txid: utxo.txid.clone(),
                amount_sat: utxo.amount,
                address: wallet.wallet.reveal_next_address(KeychainKind::External).address.to_string(), // Simplified
                keychain: "External".to_string(),
                timestamp: now,
                confirmations: utxo.confirmations,
                participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
                stable_value_usd: (utxo.amount as f64 / 100_000_000.0) * price_info.raw_btc_usd,
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
        let mut activity = if activity_path.exists() {
            let content = fs::read_to_string(&activity_path)?;
            serde_json::from_str(&content).unwrap_or_else(|e| {
                warn!("Failed to parse activity log for user {}: {}", user_id, e);
                Vec::new()
            })
        } else {
            Vec::new()
        };
        activity.extend(new_utxos.clone());
        activity.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        if activity.len() > MAX_UTXO_ACTIVITY_ENTRIES {
            activity.truncate(MAX_UTXO_ACTIVITY_ENTRIES);
        }
        save_to_file(&activity, &activity_path)?;

        if !webhook_url.is_empty() {
            notify_new_utxos(&client, user_id, &new_utxos, &webhook_url).await?;
        }
    }

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
            status.confirmations_pending = chain.utxos.iter().any(|u| u.confirmations == 0);
            status.last_update_message = format!("Sync completed successfully at {}", now);
            status.sync_duration_ms = start_time.elapsed().as_millis() as u64;
            status.last_error = None;
            status.last_success = now;
            let status_path = PathBuf::from(format!("{}/status_{}.json", &data_dir, user_id));
            save_to_file(status, &status_path)?;
        }
    }

    info!("User {} sync complete: {} UTXOs, {} BTC (${:.2})", 
         user_id, chain.utxos.len(), chain.accumulated_btc.to_btc(), chain.stabilized_usd.0);
    Ok(())
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

    // Start HTTP server - moved all route definitions inside the spawn
    let user_statuses_clone = user_statuses.clone();
    let service_status_clone = service_status.clone();
    let data_dir_clone = data_dir.clone();
    let sync_tx_clone = sync_tx.clone();

    tokio::task::spawn(async move {
        // Define routes inside the task
        let health = warp::path("health").map(|| "OK");

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

        let sync_sender = sync_tx_clone.clone();
        let force_sync = warp::path("force_sync")
            .and(warp::path::param::<String>())
            .and(warp::post())
            .and_then(move |user_id: String| {
                let tx = sync_sender.clone();
                async move {
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

        let data_dir_for_register = data_dir_clone.clone();
        let register = warp::path("register")
            .and(warp::post())
            .and(warp::body::json())
            .and_then(move |public_data: serde_json::Value| {
                let data_dir = data_dir_for_register.clone();
                async move {
                    let user_id = public_data["user_pubkey"]
                        .as_str()
                        .unwrap_or("unknown")
                        .split('/')
                        .last()
                        .unwrap_or("unknown");
                    
                    // Handle errors more gracefully
                    let json_result = serde_json::to_string_pretty(&public_data)
                        .map_err(|e| warp::reject::custom(CustomError::Serde(e)))?;
                    
                    fs::write(
                        format!("{}/user_{}_public.json", data_dir, user_id),
                        json_result
                    ).map_err(|e| warp::reject::custom(CustomError::Io(e)))?;
                    
                    Ok::<_, warp::Rejection>(warp::reply::json(&serde_json::json!({"status": "registered"})))
                }
            });

        // Combine all routes
        let routes = health
            .or(status)
            .or(user_status)
            .or(activity)
            .or(force_sync)
            .or(register);
            
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
        let price_info = fetch_btc_usd_price(&client).await.unwrap_or_else(|e| {
            error!("Failed to fetch price: {}, using default", e);
            let mut status = service_status.lock().unwrap();
            status.api_status.insert("price_feed".to_string(), false);
            status.last_update = now;
            status.price_cache_staleness_secs = DEFAULT_CACHE_DURATION_SECS;
            PriceInfo {
                raw_btc_usd: DEFAULT_PRICE_FALLBACK,
                timestamp: now_timestamp(),
                price_feeds: HashMap::new(),
            }
        });

        // Update service status with latest price
        {
            let mut status = service_status.lock().unwrap();
            status.last_price = price_info.raw_btc_usd;
            status.api_status.insert("price_feed".to_string(), true);
            status.last_update = now;
            status.price_update_count += 1;
            status.price_cache_staleness_secs = if common::price_feed::is_price_cache_stale() {
                DEFAULT_CACHE_DURATION_SECS
            } else {
                0 // Not stale
            };
        }

        // Check for forced sync requests
        let mut forced_users = Vec::new();
        while let Ok(user_id) = sync_rx.try_recv() {
            info!("Forced sync requested for user {}", user_id);
            forced_users.push(user_id);
        }

        // Process forced syncs first
        for user_id in forced_users {
            if is_user_active(&user_id) {
                warn!("Skipping forced sync for user {} - already being processed", user_id);
                continue;
            }

            mark_user_active(&user_id);
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

            {
                let mut status = service_status.lock().unwrap();
                status.active_syncs = status.active_syncs.saturating_sub(1);
            }
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
                if path.extension().map_or(false, |e| e == "json")
                    && path.to_str().unwrap().contains("user_")
                    && path.to_str().unwrap().contains("_public")
                {
                    let file_name = path.file_stem().unwrap().to_str().unwrap();
                    if let Some(user_id) = file_name.strip_prefix("user_").and_then(|s| s.split('_').next()) {
                        {
                            let wallets_lock = wallets.lock().unwrap();
                            if wallets_lock.contains_key(user_id) {
                                continue; // User already exists
                            }
                        }

                        info!("Found new user: {}", user_id);

                        {
                            let mut user_statuses_lock = user_statuses.lock().unwrap();
                            user_statuses_lock.insert(
                                user_id.to_string(),
                                UserStatus {
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
                                },
                            );

                            let status_path = PathBuf::from(format!("{}/status_{}.json", &data_dir, user_id));
                            if let Err(e) = save_to_file(&user_statuses_lock[user_id], &status_path) {
                                warn!("Failed to save initial status for user {}: {}", user_id, e);
                            }
                        }

                        let _ = sync_tx.send(user_id.to_string()).await;
                    }
                }
            }

            {
                let mut service_status_lock = service_status.lock().unwrap();
                service_status_lock.users_monitored = wallets.lock().unwrap().len() as u32;
                service_status_lock.last_update = now;
            }

            last_user_scan = now;
            info!("User scan complete. Monitoring {} users.", wallets.lock().unwrap().len());
        }

        // Verify tokio task spawn works
        let task = tokio::task::spawn(async {
            debug!("Task spawn verification successful");
            Ok::<(), PulserError>(())
        });
        let _ = task.await;

        // Get user IDs to process (only active users)
        let user_ids: Vec<String> = {
            let wallets_lock = wallets.lock().unwrap();
            wallets_lock
                .keys()
                .filter(|user_id| !is_user_active(user_id))
                .cloned()
                .collect()
        };

        // Process users based on configuration (batch or sequential)
        if batch_users {
            for chunk in user_ids.chunks(max_concurrent_users) {
                for user_id in chunk {
                    if is_user_active(user_id) {
                        continue;
                    }

                    mark_user_active(user_id);
                    {
                        let mut status = service_status.lock().unwrap();
                        status.active_syncs += 1;
                    }

                    let result = sync_user(
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

                    {
                        let mut status = service_status.lock().unwrap();
                        status.active_syncs = status.active_syncs.saturating_sub(1);
                    }
                    mark_user_inactive(user_id);

                    if let Err(e) = result {
                        warn!("Sync failed for user {}: {}", user_id, e);
                    }
                }
            }
        } else {
            for (index, user_id) in user_ids.iter().enumerate() {
                if index > 0 && stagger_delay_secs > 0 {
                    sleep(Duration::from_secs(stagger_delay_secs)).await;
                }

                if is_user_active(user_id) {
                    continue;
                }

                mark_user_active(user_id);
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

                {
                    let mut status = service_status.lock().unwrap();
                    status.active_syncs = status.active_syncs.saturating_sub(1);
                }
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
