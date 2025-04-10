// deposit-service/src/api/sync.rs - Improved implementation
use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::time::timeout;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, HashSet};
use log::{info, warn, debug, error, trace};
use serde_json::json;
use reqwest::Client;
use std::str::FromStr;
use bitcoin::Address;

use bdk_wallet::KeychainKind;
use bdk_esplora::EsploraAsyncExt;
use bdk_esplora::esplora_client::AsyncClient;

use common::wallet_sync;
use common::error::PulserError;
use common::types::{PriceInfo, StableChain, UserStatus, UtxoInfo, ServiceStatus, ChangeEntry};
use common::price_feed::PriceFeed;
use common::StateManager;
use common::task_manager::{UserTaskLock, ScopedTask};

use crate::wallet::DepositWallet;
use common::webhook::{WebhookManager, WebhookPayload};
use crate::config::Config;
use crate::api::status;
use common::wallet_sync::Config as WalletSyncConfig;

// Constants for timeouts and concurrency control
const DEFAULT_LOCK_TIMEOUT_MS: u64 = 1000;
const SYNC_OPERATION_TIMEOUT_SECS: u64 = 120;
const SYNC_LOCK_TIMEOUT_SECS: u64 = 5;
const WEBHOOK_TIMEOUT_SECS: u64 = 10;
const MAX_RETRY_ATTEMPTS: usize = 3;
const RETRY_DELAY_BASE_MS: u64 = 250;

// Routes configuration
pub fn sync_route(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_info: Arc<Mutex<PriceInfo>>,
    esplora_urls: Arc<Mutex<Vec<(String, u64)>>>,
    esplora: Arc<AsyncClient>,
    state_manager: Arc<StateManager>,
    webhook_url: String,
    webhook_manager: WebhookManager,
    client: Client,
    active_tasks_manager: Arc<UserTaskLock>,
    price_feed: Arc<PriceFeed>,
    service_status: Arc<Mutex<ServiceStatus>>,
    config: Arc<Config>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::post()
        .and(warp::path!("sync" / String))
        .and(super::with_wallets(wallets))
        .and(super::with_statuses(user_statuses))
        .and(super::with_price(price_info))
        .and(super::with_esplora_urls(esplora_urls))
        .and(warp::any().map(move || Arc::clone(&esplora)))
        .and(super::with_state_manager(state_manager))
        .and(warp::any().map(move || webhook_manager.clone()))
        .and(warp::any().map(move || webhook_url.clone()))
        .and(warp::any().map(move || client.clone()))
        .and(super::with_active_tasks_manager(active_tasks_manager))
        .and(warp::any().map(move || price_feed.clone()))
        .and(super::with_service_status(service_status))
        .and(super::with_config(config))
        .and_then(sync_user_handler)
}

// Force sync endpoint - last resort full resync
pub fn force_sync(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    active_tasks_manager: Arc<UserTaskLock>,
    price_info: Arc<Mutex<PriceInfo>>,
    esplora: Arc<AsyncClient>,
    state_manager: Arc<StateManager>,
    config: Arc<Config>
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path!("force_sync" / String)
        .and(warp::post())
        .and(super::with_wallets(wallets))
        .and(super::with_active_tasks_manager(active_tasks_manager))
        .and(super::with_price(price_info))
        .and(warp::any().map(move || Arc::clone(&esplora)))
        .and(super::with_state_manager(state_manager))
        .and(super::with_config(config))
        .and_then(force_sync_handler)
}

// Helper function to acquire a lock with timeout and backoff
async fn acquire_lock_with_retry<'a, T>(
    mutex: &'a Arc<Mutex<T>>,
    user_id: &str,
    operation: &str,
    timeout_ms: u64,
    max_retries: usize,
) -> Result<tokio::sync::MutexGuard<'a, T>, PulserError> {
    let mut attempts = 0;
    
    loop {
        match timeout(Duration::from_millis(timeout_ms), mutex.lock()).await {
            Ok(guard) => return Ok(guard),
            Err(_) => {
                attempts += 1;
                if attempts > max_retries {
                    return Err(PulserError::InternalError(format!(
                        "Timeout acquiring {} lock after {} attempts", operation, attempts
                    )));
                }
                
                // Exponential backoff
                let delay = RETRY_DELAY_BASE_MS * (1 << attempts);
                warn!("Timeout acquiring {} lock for user {}, retrying in {}ms (attempt {}/{})", 
                      operation, user_id, delay, attempts, max_retries + 1);
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        }
    }
}

// Main sync user handler for HTTP API
async fn sync_user_handler(
    user_id: String,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price: Arc<Mutex<PriceInfo>>,
    esplora_urls: Arc<Mutex<Vec<(String, u64)>>>,
    esplora: Arc<AsyncClient>,
    state_manager: Arc<StateManager>,
    webhook_manager: WebhookManager,
    webhook_url: String,
    client: Client,
    active_tasks_manager: Arc<UserTaskLock>,
    price_feed: Arc<PriceFeed>,
    service_status: Arc<Mutex<ServiceStatus>>,
    config: Arc<Config>,
) -> Result<impl warp::Reply, warp::Rejection> {
    debug!("Received sync request for user {}", user_id);
    let start_time = Instant::now();
    
    // Check if user is already being synced
    if active_tasks_manager.is_user_active(&user_id).await {
        debug!("User {} is already being synced", user_id);
        return Ok(warp::reply::json(&json!({
            "status": "error", 
            "message": "User already syncing"
        })));
    }

    // Use ScopedTask for automatic cleanup on drop
    let task = match ScopedTask::new(&active_tasks_manager, &user_id, "sync").await {
        Some(task) => task,
        None => {
            debug!("Failed to mark user {} as active, likely already syncing", user_id);
            return Ok(warp::reply::json(&json!({
                "status": "error", 
                "message": "User already syncing or unable to begin sync"
            })));
        }
    };

    // Update service status to increment active syncs
let mut status = service_status.lock().await;
status.active_syncs += 1;

    // Update user status to indicate sync is in progress
    status::update_user_status_simple(&user_id, "syncing", "Sync in progress", &user_statuses).await;

    // Execute sync operation with timeout
    let sync_result = match timeout(
        Duration::from_secs(SYNC_OPERATION_TIMEOUT_SECS),
        sync_user(
            &user_id,
            wallets.clone(),
            user_statuses.clone(),
            price.clone(),
            price_feed.clone(),
            esplora_urls.clone(),
            &esplora,
            state_manager.clone(),
            webhook_manager.clone(),
            &webhook_url,
            client.clone(),
            config.clone(),
        )
    ).await {
        Ok(Ok(new_funds)) => {
            let message = if new_funds { "Sync completed with new funds" } else { "Sync completed" };
            json!({
                "status": "ok", 
                "message": message
            })
        },
        Ok(Err(e)) => {
            error!("Sync for user {} failed: {}", user_id, e);
            // Update user status with error
            status::update_user_status_with_error(&user_id, "error", &e.to_string(), &user_statuses).await;
            json!({
                "status": "error",
                "message": format!("Sync failed: {}", e)
            })
        },
        Err(_) => {
            error!("Sync for user {} timed out after {}s", user_id, SYNC_OPERATION_TIMEOUT_SECS);
            status::update_user_status_with_error(&user_id, "error", "Sync operation timed out", &user_statuses).await;
            json!({
                "status": "error",
                "message": "Sync operation timed out"
            })
        }
    };

    let duration_ms = start_time.elapsed().as_millis() as u64;

    // Update user status with duration
let mut statuses = user_statuses.lock().await;
if let Some(status) = statuses.get_mut(&user_id) {
    status.sync_duration_ms = duration_ms;
    status.last_sync = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
}

    // Update service statistics
let mut status = service_status.lock().await;
status.active_syncs = status.active_syncs.saturating_sub(1);

            // Only update overall stats if we can get the wallets lock with a short timeout
            if let Ok(wallets_lock) = timeout(Duration::from_secs(2), wallets.lock()).await {
                // Update global statistics only when needed (use atomic fetch operations inside)
                let total_utxos = wallets_lock.values().map(|(_, c)| c.utxos.len() as u32).sum();
                let total_value_btc = wallets_lock.values().map(|(w, _)| 
                    w.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0).sum();
                let total_value_usd = wallets_lock.values().map(|(_, c)| c.stabilized_usd.0).sum();
                
                // Only update if values actually changed to avoid unnecessary writes
let btc_value1: f64 = status.total_value_btc;
let btc_value2: f64 = total_value_btc;
let btc_diff: f64 = (btc_value1 - btc_value2).abs();

let usd_value1: f64 = status.total_value_usd;
let usd_value2: f64 = total_value_usd;  
let usd_diff: f64 = (usd_value1 - usd_value2).abs();

if status.total_utxos != total_utxos || 
   btc_diff > 0.00001_f64 || 
   usd_diff > 0.01_f64 {
    status.total_utxos = total_utxos;
    status.total_value_btc = total_value_btc;
    status.total_value_usd = total_value_usd;
    status.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
}
            }
     
        Ok(warp::reply::json(&sync_result))
}

// Force sync handler - "last resort" complete wallet resync
async fn force_sync_handler(
    user_id: String,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    active_tasks_manager: Arc<UserTaskLock>,
    price_info: Arc<Mutex<PriceInfo>>,
    esplora: Arc<AsyncClient>,
    state_manager: Arc<StateManager>,
    config: Arc<Config>
) -> Result<impl Reply, Rejection> {
    debug!("Received force_sync request for user {}", user_id);
    
    // Check if user is already being synced
    if active_tasks_manager.is_user_active(&user_id).await {
        debug!("User {} already being synced", &user_id);
        return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
    }

    // Use ScopedTask for automatic cleanup on drop
    let _task = match ScopedTask::new(&active_tasks_manager, &user_id, "force_sync").await {
        Some(task) => task,
        None => {
            debug!("Failed to mark user {} as active for force_sync", user_id);
            return Ok(warp::reply::json(&json!({
                "status": "error", 
                "message": "Unable to begin force sync"
            })));
        }
    };
    
    // Initialize price feed (needed for resync)
    let price_feed = Arc::new(PriceFeed::new());
    
    // Retrieve current price info with timeout
    let price_info_data = match timeout(Duration::from_secs(5), price_info.lock()).await {
        Ok(lock) => lock.clone(),
        Err(_) => return Ok(warp::reply::json(&json!({
            "status": "error", 
            "message": "Timeout acquiring price info"
        }))),
    };
    
    // Attempt to acquire wallet lock with reasonable timeout
    let wallet_result = match timeout(Duration::from_secs(10), wallets.lock()).await {
        Ok(mut wallets_lock) => {
            if let Some((wallet, chain)) = wallets_lock.get_mut(&user_id) {
                // Save existing history and logs before syncing
                let existing_history = chain.history.clone();
                let existing_change_log = chain.change_log.clone();
                
                // Perform full resync through wallet_sync module
                match wallet_sync::resync_full_history(
                    &user_id,
                    &mut wallet.wallet,
                    &esplora,
                    chain,
                    price_feed.clone(),
                    &price_info_data,
                    &state_manager,
                    config.min_confirmations,
                ).await {
                    Ok(new_utxos) => {
                        // Use a HashMap to deduplicate and preserve most updated UTXOs
                        let mut txid_map = HashMap::new();

                        // First add all current history items 
                        for utxo in &chain.history {
                            txid_map.insert((utxo.txid.clone(), utxo.vout), utxo.clone());
                        }

                        // Then add back old history, only overwriting if it has MORE confirmations
                        for utxo in existing_history {
                            let key = (utxo.txid.clone(), utxo.vout);
                            
                            // Only insert if it doesn't exist or has more confirmations
                            if !txid_map.contains_key(&key) || 
                               utxo.confirmations > txid_map.get(&key).unwrap().confirmations {
                                txid_map.insert(key, utxo.clone());
                            }
                        }

                        // Replace history with deduplicated values
                        chain.history = txid_map.values().cloned().collect();
                        
                        // Add a resync log entry
                        chain.change_log = {
                            let mut logs = existing_change_log;
                            logs.push(ChangeEntry {
                                timestamp: chrono::Utc::now().timestamp(),
                                change_type: "force_resync".to_string(),
                                btc_delta: 0.0,
                                usd_delta: 0.0,
                                service: "deposit-service".to_string(),
                                details: Some(format!("Full history resync with {} new UTXOs", new_utxos.len())),
                            });
                            logs
                        };
                        
                        // Save the updated chain state
                        if let Err(e) = state_manager.save_stable_chain(&user_id, chain).await {
                            warn!("Failed to save StableChain after force_sync: {}", e);
                        }
                        
                        // Save changeset if available
                        if let Some(changeset) = wallet.wallet.take_staged() {
                            if let Err(e) = state_manager.save_changeset(&user_id, &changeset).await {
                                warn!("Failed to save changeset after force_sync: {}", e);
                            }
                        }
                        
                        json!({
                            "status": "ok",
                            "message": format!("Full resync completed, {} new UTXOs, history preserved", new_utxos.len())
                        })
                    },
                    Err(e) => {
                        json!({
                            "status": "error",
                            "message": format!("Force sync failed: {}", e)
                        })
                    },
                }
            } else {
                json!({"status": "error", "message": "User wallet not found"})
            }
        },
        Err(_) => {
            json!({"status": "error", "message": "Timeout acquiring wallet lock"})
        }
    };
    
    Ok(warp::reply::json(&wallet_result))
}

// Core sync user function - called by the HTTP handler
pub async fn sync_user(
    user_id: &str,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_info: Arc<Mutex<PriceInfo>>,
    price_feed: Arc<PriceFeed>,
    esplora_urls: Arc<Mutex<Vec<(String, u64)>>>,
    esplora: &Arc<AsyncClient>,
    state_manager: Arc<StateManager>,
    webhook_manager: WebhookManager,
    webhook_url: &str,
    client: Client,
    config: Arc<Config>,
) -> Result<bool, PulserError> {
    let start_time = Instant::now();
    debug!("Starting sync for user {}", user_id);

    // Track if new funds are discovered
    let mut new_funds_found = false;
    
    // Get current price data
    let current_price = match timeout(Duration::from_secs(5), price_info.lock()).await {
        Ok(guard) => guard.clone(),
        Err(_) => return Err(PulserError::InternalError("Timeout acquiring price info".to_string())),
    };

    // Perform the actual sync operation
    for attempt in 0..3 {
        if attempt > 0 {
            debug!("Retry attempt {}/3 for user {} sync", attempt + 1, user_id);
            tokio::time::sleep(Duration::from_millis(500 * (attempt + 1) as u64)).await;
        }
        
        // Get the current and old addresses with minimal wallet lock time
        let (deposit_address, old_addresses) = {
            let wallets_lock = match timeout(Duration::from_secs(5), wallets.lock()).await {
                Ok(lock) => lock,
                Err(_) => return Err(PulserError::InternalError("Timeout acquiring wallets lock".to_string())),
            };
            
            let (wallet, chain) = match wallets_lock.get(user_id) {
                Some(pair) => pair,
                None => return Err(PulserError::UserNotFound(format!("Wallet not found for user {}", user_id))),
            };
            
            // Clone the data we need to minimize lock time
            let deposit_addr = Address::from_str(&chain.multisig_addr)?.assume_checked();
            let old_addresses = chain.old_addresses.clone();
            
            (deposit_addr, old_addresses)
        };
        
        // Create a targeted sync request with current and all old addresses
        let mut spks = vec![deposit_address.script_pubkey()];
        for old_addr in &old_addresses {
            if let Ok(addr) = Address::from_str(old_addr) {
                spks.push(addr.assume_checked().script_pubkey());
            }
        }
        
        // Build a targeted sync request
        let sync_request = bdk_chain::spk_client::SyncRequest::builder()
            .spks(spks.into_iter())
            .build();
            
        // Perform sync with esplora API
        match esplora.sync(sync_request, 5).await {
            Ok(update) => {
                // Now update the wallet with minimal lock time
                let prev_balance = {
                    let mut wallets_lock = match timeout(Duration::from_secs(10), wallets.lock()).await {
                        Ok(lock) => lock,
                        Err(_) => return Err(PulserError::InternalError("Timeout acquiring wallets lock".to_string())),
                    };
                    
                    let (wallet, chain) = match wallets_lock.get_mut(user_id) {
                        Some(pair) => pair,
                        None => return Err(PulserError::UserNotFound(format!("Wallet disappeared for user {}", user_id))),
                    };
                    
                    // Apply the update to the wallet
                    wallet.wallet.apply_update(update)?;
                    
                    // Get the previous balance to check for new funds
                    let prev_balance = chain.stabilized_usd.0;
                    let prev_utxo_count = chain.utxos.len();
                    
                    // Get a change address for UTXO stabilization
                    let change_addr = wallet.wallet.reveal_next_address(KeychainKind::Internal).address;
                    
let mempool_timestamps = HashMap::new(); // Empty HashMap
let wallet_sync_config = WalletSyncConfig {
    min_confirmations: config.min_confirmations,
    service_min_confirmations: config.service_min_confirmations,
    external_min_confirmations: config.external_min_confirmations,
};

match wallet_sync::sync_and_stabilize_utxos(
    user_id,
    &mut wallet.wallet,
    esplora,
    chain,
    price_feed.clone(),
    &current_price,
    &deposit_address,
    &change_addr,
    &state_manager,
    config.min_confirmations,
    &mempool_timestamps,
    &wallet_sync_config
).await {
                        Ok(_) => {
                            // New funds check
                            new_funds_found = chain.stabilized_usd.0 > prev_balance || chain.utxos.len() > prev_utxo_count;
                            
                            if new_funds_found {
                                debug!(
                                    "New funds for user {}: ${:.2} -> ${:.2}, UTXOs: {} -> {}",
                                    user_id, prev_balance, chain.stabilized_usd.0, prev_utxo_count, chain.utxos.len()
                                );
                            }
                            
                            prev_balance
                        },
                        Err(e) => return Err(e),
                    }
                };
                
                // Handle webhook notification if new funds found
                if !webhook_url.is_empty() && new_funds_found && config.webhook_enabled {
                    let new_balance = {
                        let wallets_lock = match timeout(Duration::from_secs(3), wallets.lock()).await {
                            Ok(lock) => lock,
                            Err(_) => return Err(PulserError::InternalError("Timeout acquiring wallets lock".to_string())),
                        };
                        
                        match wallets_lock.get(user_id) {
                            Some((_, chain)) => chain.stabilized_usd.0,
                            None => prev_balance, // Fallback if wallet disappeared
                        }
                    };
                    
                    // Send webhook in the background
                    let webhook_data = json!({
                        "event": "deposit",
                        "user_id": user_id,
                        "amount_usd": new_balance - prev_balance,
                        "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
                    });
                    
                    let user_id_owned = user_id.to_string();
                    let webhook_url_owned = webhook_url.to_string();
                    let webhook_manager = webhook_manager.clone();
                    
                    tokio::spawn(async move {
                        let payload = WebhookPayload {
                            event: "deposit".to_string(),
                            user_id: user_id_owned.clone(),
                            data: webhook_data,
                            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                        };
                        
                        if let Err(e) = webhook_manager.send(&webhook_url_owned, payload).await {
                            warn!("Failed to send webhook for user {}: {}", user_id_owned, e);
                        }
                    });
                }
                
                // Success, no need for more retries
                break;
            },
            Err(e) => {
                warn!("Error syncing wallet for user {} (attempt {}/3): {}", user_id, attempt + 1, e);
                if attempt == 2 {
                    // All attempts failed
                    status::update_user_status_with_error(
                        user_id, "error", &format!("Sync failed: {}", e), &user_statuses
                    ).await;
                    return Err(e.into());
                }
            }
        }
    }

    // Update user status with final success state
    let success_message = if new_funds_found {
        "Sync completed with new funds"
    } else {
        "Sync completed successfully"
    };
    
    // Update wallet and user status with the latest info
   let (utxo_count, total_value_btc, total_value_usd) = match timeout(
    Duration::from_secs(3),
    wallets.lock()
).await {
    Ok(wallets_lock) => {
        match wallets_lock.get(user_id) {
            Some((wallet, chain)) => {
                let wallet_balance = wallet.wallet.balance();
                (
                    chain.utxos.len() as u32,
                    wallet_balance.confirmed.to_sat() as f64 / 100_000_000.0,
                    chain.stabilized_usd.0
                )
            },
            None => (0, 0.0, 0.0),
        }
    },
    Err(_) => (0, 0.0, 0.0), // Default values if we can't get the lock
};
    
    // Update user status with complete information
    let elapsed = start_time.elapsed().as_millis() as u64;
    status::update_user_status_full(
        user_id,
        "success",
        utxo_count,
        total_value_btc,
        total_value_usd,
        false,
        success_message,
        elapsed,
        None,
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        &user_statuses
    ).await;

    debug!(
        "Sync completed for user {} in {}ms, new_funds={}",
        user_id,
        elapsed,
        new_funds_found
    );
    
    Ok(new_funds_found)
}
