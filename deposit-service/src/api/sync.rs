// deposit-service/src/api/sync.rs - revised implementation
use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, Semaphore, mpsc};
use tokio::time::timeout;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, VecDeque, HashSet};
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

// Constants for timeouts and concurrency control
const DEFAULT_LOCK_TIMEOUT_MS: u64 = 1000;
const SYNC_OPERATION_TIMEOUT_SECS: u64 = 120;
const SYNC_LOCK_TIMEOUT_SECS: u64 = 5;
const WEBHOOK_TIMEOUT_SECS: u64 = 10;
const MAX_RETRY_ATTEMPTS: usize = 3;
const RETRY_DELAY_BASE_MS: u64 = 250;

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
        debug!("Attempting to acquire lock for {} operation on user {}, attempt {}/{}", 
               operation, user_id, attempts + 1, max_retries + 1);
        
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

// Force sync endpoint
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
    
    // Check if user is already being synced with timeout
    let is_active = match timeout(
        Duration::from_secs(SYNC_LOCK_TIMEOUT_SECS), 
        active_tasks_manager.is_user_active(&user_id)
    ).await {
        Ok(active) => active,
        Err(_) => {
            warn!("Timeout checking active status for user {}", user_id);
            return Ok(warp::reply::json(&json!({
                "status": "error", 
                "message": "Service busy, try again later"
            })));
        }
    };

    if is_active {
        debug!("User {} is already being synced", user_id);
        return Ok(warp::reply::json(&json!({
            "status": "error", 
            "message": "User already syncing"
        })));
    }

    // Use ScopedTask for automatic cleanup on drop
    let task = match timeout(
        Duration::from_secs(SYNC_LOCK_TIMEOUT_SECS),
        ScopedTask::new(&active_tasks_manager, &user_id, "sync")
    ).await {
        Ok(Some(task)) => task,
        Ok(None) => {
            debug!("Failed to mark user {} as active, likely already syncing", user_id);
            return Ok(warp::reply::json(&json!({
                "status": "error", 
                "message": "User already syncing or unable to begin sync"
            })));
        },
        Err(_) => {
            warn!("Timeout attempting to mark user {} as active", user_id);
            return Ok(warp::reply::json(&json!({
                "status": "error", 
                "message": "Service busy, try again later"
            })));
        }
    };

    // Update service status to increment active syncs
    match timeout(
        Duration::from_secs(SYNC_LOCK_TIMEOUT_SECS),
        acquire_lock_with_retry(
            &service_status,
            &user_id,
            "service_status",
            DEFAULT_LOCK_TIMEOUT_MS,
            MAX_RETRY_ATTEMPTS
        )
    ).await {
        Ok(Ok(mut status)) => status.active_syncs += 1,
        Ok(Err(e)) => warn!("Error acquiring service status lock for user {}: {}", user_id, e),
        Err(_) => warn!("Timeout acquiring service status lock for user {}", user_id),
    }

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
    match timeout(
        Duration::from_secs(SYNC_LOCK_TIMEOUT_SECS),
        user_statuses.lock()
    ).await {
        Ok(mut statuses) => {
            if let Some(status) = statuses.get_mut(&user_id) {
                status.sync_duration_ms = duration_ms;
                status.last_sync = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            }
        },
        Err(_) => warn!("Timeout updating user status duration for {}", user_id),
    }

    // Update service statistics
    match timeout(
        Duration::from_secs(SYNC_LOCK_TIMEOUT_SECS),
        service_status.lock()
    ).await {
        Ok(mut status) => {
            status.active_syncs = status.active_syncs.saturating_sub(1);
            if let Ok(wallets_lock) = timeout(Duration::from_secs(2), wallets.lock()).await {
                status.total_utxos = wallets_lock.values().map(|(_, c)| c.utxos.len() as u32).sum();
                status.total_value_btc = wallets_lock.values().map(|(w, _)| 
                    w.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0).sum();
                status.total_value_usd = wallets_lock.values().map(|(_, c)| c.stabilized_usd.0).sum();
            }
        },
        Err(_) => warn!("Timeout updating service status after sync for {}", user_id),
    }
    
    Ok(warp::reply::json(&sync_result))
}

// Force sync handler - unchanged except for updating the history preservation
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

    let price_feed = Arc::new(PriceFeed::new());

    // Check if user is already being synced
    if active_tasks_manager.is_user_active(&user_id).await {
        debug!("User {} already being synced", &user_id);
        return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
    }

    // Acquire task lock
    if !active_tasks_manager.mark_user_active(&user_id, "force_sync").await {
        return Ok(warp::reply::json(&json!({"status": "error", "message": "Failed to lock user for sync"})));
    }
    
    // IMPORTANT: We access the wallet without removing it from the HashMap
    {
        let mut wallets_lock = wallets.lock().await;
        if let Some((wallet, chain)) = wallets_lock.get_mut(&user_id) {
            // Save existing history before syncing
            let existing_history = chain.history.clone();
            let existing_change_log = chain.change_log.clone();
            
            // Get the current price information
            let price_info_data = match tokio::time::timeout(Duration::from_secs(3), price_info.lock()).await {
                Ok(lock) => lock.clone(),
                Err(_) => return Err(PulserError::InternalError("Timeout acquiring price info".to_string()))?,
            };
            
            // Perform the sync
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
                    // Create a set to detect duplicates
                    let mut txid_set = HashSet::new();
                    for utxo in &chain.history {
                        txid_set.insert((utxo.txid.clone(), utxo.vout));
                    }
                    
                    // Add back original history that doesn't conflict
                    for utxo in existing_history {
                        let key = (utxo.txid.clone(), utxo.vout);
                        if !txid_set.contains(&key) {
                            chain.history.push(utxo.clone());
                        }
                    }
                    
                    // Add resync log entry
                    let mut merged_logs = existing_change_log;
                    merged_logs.push(ChangeEntry {
                        timestamp: chrono::Utc::now().timestamp(),
                        change_type: "force_resync".to_string(),
                        btc_delta: 0.0,
                        usd_delta: 0.0,
                        service: "deposit-service".to_string(),
                        details: Some(format!("Full history resync with {} new UTXOs", new_utxos.len())),
                    });
                    chain.change_log = merged_logs;
                    
                    info!("Full resync completed for user {} with history preserved: {} UTXOs", &user_id, chain.history.len());
                    
                    // Make sure to save the updated chain and wallet state
                    state_manager.save_stable_chain(&user_id, chain).await?;
                    if let Some(changeset) = wallet.wallet.take_staged() {
                        state_manager.save_changeset(&user_id, &changeset).await?;
                    }
                    
                    active_tasks_manager.mark_user_inactive(&user_id).await;
                    Ok(warp::reply::json(&json!({
                        "status": "ok",
                        "message": format!("Full resync completed, {} new UTXOs, history preserved", new_utxos.len())
                    })))
                },
                Err(e) => {
                    active_tasks_manager.mark_user_inactive(&user_id).await;
                    Err(e)?
                },
            }
        } else {
            active_tasks_manager.mark_user_inactive(&user_id).await;
            Ok(warp::reply::json(&json!({"status": "error", "message": "User wallet disappeared"})))
        }
    }
}

// Main sync user function to work with monitor - Fixed the locking issues
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

    // Update user status to indicate sync is in progress
    status::update_user_status_simple(user_id, "syncing", "Sync in progress", &user_statuses).await;

    // Track if new funds are discovered
    let mut new_funds_found = false;

    // Get current price data
    let current_price = {
        let price_guard = acquire_lock_with_retry(
            &price_info,
            user_id,
            "price_info",
            DEFAULT_LOCK_TIMEOUT_MS,
            MAX_RETRY_ATTEMPTS,
        ).await?;
        price_guard.clone()
    };

    // CRITICAL CHANGE: Instead of removing the wallet from the HashMap,
    // we access it within the lock scope per operation
    for attempt in 0..3 {
        if attempt > 0 {
            debug!("Retry attempt {}/3 for user {} sync", attempt, user_id);
        }
        
        // Get deposit address and other data
        let (deposit_address, prev_balance, prev_utxo_count) = {
            let wallets_lock = acquire_lock_with_retry(
                &wallets, user_id, "wallets", DEFAULT_LOCK_TIMEOUT_MS, MAX_RETRY_ATTEMPTS
            ).await?;
            
            let (wallet, chain) = match wallets_lock.get(user_id) {
                Some(pair) => pair,
                None => return Err(PulserError::UserNotFound(format!("Wallet not found for user {}", user_id))),
            };
            
            let address = Address::from_str(&chain.multisig_addr)?;
            (
                address.assume_checked(),
                chain.stabilized_usd.0,
                chain.utxos.len()
            )
        };
        
        // Perform the sync within a new lock scope to minimize lock time
        let sync_result = {
            let mut wallets_lock = acquire_lock_with_retry(
                &wallets, user_id, "wallets", DEFAULT_LOCK_TIMEOUT_MS, MAX_RETRY_ATTEMPTS
            ).await?;
            
            let (wallet, chain) = match wallets_lock.get_mut(user_id) {
                Some(pair) => pair,
                None => return Err(PulserError::UserNotFound(format!("Wallet not found for user {}", user_id))),
            };
            
            let change_addr = wallet.wallet.reveal_next_address(KeychainKind::Internal).address;
            wallet_sync::sync_and_stabilize_utxos(
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
            ).await
        };
        
        match sync_result {
            Ok(_) => {
                // Now check if new funds were found
                let (new_balance, new_utxo_count, state_save_result) = {
                    let mut wallets_lock = acquire_lock_with_retry(
                        &wallets, user_id, "wallets", DEFAULT_LOCK_TIMEOUT_MS, MAX_RETRY_ATTEMPTS
                    ).await?;
                    
                    let (wallet, chain) = match wallets_lock.get_mut(user_id) {
                        Some(pair) => pair,
                        None => return Err(PulserError::UserNotFound(format!("Wallet not found for user {}", user_id))),
                    };
                    
                    // Save the state with retries in a separate task
                    let state_save_result = tokio::spawn({
                        let user_id = user_id.to_string();
                        let chain = chain.clone();
                        let changeset = wallet.wallet.staged().cloned();
                        let state_manager = state_manager.clone();
                        
                        async move {
                            if let Err(e) = state_manager.save_stable_chain(&user_id, &chain).await {
                                warn!("Error saving StableChain for user {}: {}", user_id, e);
                                return false;
                            }
                            
                            if let Some(changeset) = changeset {
                                if let Err(e) = state_manager.save_changeset(&user_id, &changeset).await {
                                    warn!("Error saving wallet changeset for user {}: {}", user_id, e);
                                    return false;
                                }
                            }
                            true
                        }
                    });
                    
                    (chain.stabilized_usd.0, chain.utxos.len(), state_save_result)
                };
                
                // Check if saving was successful
                if let Ok(save_ok) = timeout(Duration::from_secs(5), state_save_result).await {
                    if let Ok(false) = save_ok {
                        if attempt < 2 {
                            warn!("State save failed for user {}, retry attempt {}", user_id, attempt + 1);
                            continue; // Retry on state save failure
                        }
                    }
                }

                // Check if new funds were found
                if new_balance > prev_balance || new_utxo_count > prev_utxo_count {
                    new_funds_found = true;
                    debug!(
                        "New funds for user {}: ${:.2} -> ${:.2}, UTXOs: {} -> {}",
                        user_id, prev_balance, new_balance, prev_utxo_count, new_utxo_count
                    );

                    // Send webhook notification if new funds found
                    if !webhook_url.is_empty() && new_funds_found {
                        let webhook_data = json!({
                            "event": "deposit",
                            "user_id": user_id,
                            "amount_usd": new_balance - prev_balance,
                            "utxo_count": new_utxo_count,
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
                            
                            match timeout(
                                Duration::from_secs(WEBHOOK_TIMEOUT_SECS), 
                                webhook_manager.send(&webhook_url_owned, payload)
                            ).await {
                                Ok(Ok(_)) => debug!(
                                    "Webhook sent successfully for new deposit for user {}",
                                    user_id_owned
                                ),
                                Ok(Err(e)) => warn!(
                                    "Failed to send webhook for user {}: {}",
                                    user_id_owned, e
                                ),
                                Err(_) => warn!(
                                    "Webhook send timed out for user {}",
                                    user_id_owned
                                ),
                            }
                        });
                    }
                }
                
                // Success, no need for more retries
                break;
            },
            Err(e) => {
                warn!(
                    "Error syncing wallet for user {} (attempt {}/3): {}",
                    user_id,
                    attempt + 1,
                    e
                );
                if attempt < 2 {
                    // Exponential backoff
                    tokio::time::sleep(Duration::from_millis(500 * (attempt + 1) as u64)).await;
                    continue;
                } else {
                    // All attempts failed
                    status::update_user_status_with_error(
                        user_id, "error", &format!("Sync failed: {}", e), &user_statuses
                    ).await;
                    return Err(e);
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
    
    let elapsed = start_time.elapsed().as_millis() as u64;
    status::update_user_status_full(
        user_id,
        "success",
        0, // We don't have utxo count here, but it will be updated elsewhere
        0.0, // Same for BTC value
        0.0, // Same for USD value
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
