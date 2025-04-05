use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, Semaphore, mpsc};
use tokio::time::timeout;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, VecDeque};
use log::{info, warn, debug, error, trace};
use serde_json::json;
use reqwest::Client;
use futures_util::FutureExt;
use std::pin::Pin;
use bitcoin::Address;
use std::str::FromStr;
use futures::future::join_all;


use bdk_wallet::KeychainKind;
use bdk_chain::spk_client::SyncRequest;
use bdk_chain::ChainPosition;
use bdk_esplora::EsploraAsyncExt;
use bdk_esplora::esplora_client::AsyncClient;


use common::wallet_sync::{resync_full_history, sync_and_stabilize_utxos};
use common::error::PulserError;
use common::types::{PriceInfo, StableChain, UserStatus, UtxoInfo, ServiceStatus};
use common::price_feed::PriceFeed;
use common::StateManager;
use common::wallet_sync;
use common::task_manager::{UserTaskLock, ScopedTask};

use crate::wallet::DepositWallet;
use common::webhook::{WebhookManager, WebhookPayload};
use crate::config::Config;
use crate::monitor::check_mempool_for_address;
use crate::api::status;



// Constants for timeouts and concurrency control
const DEFAULT_LOCK_TIMEOUT_MS: u64 = 1000;
const SYNC_OPERATION_TIMEOUT_SECS: u64 = 120;
const SYNC_LOCK_TIMEOUT_SECS: u64 = 5;
const WEBHOOK_TIMEOUT_SECS: u64 = 10;
const MAX_RETRY_ATTEMPTS: usize = 3;
const RETRY_DELAY_BASE_MS: u64 = 250;

// ActivityGuard: Ensures user is marked as inactive when the guard is dropped
struct ActivityGuard<'a> {
    user_id: &'a str,
    manager: &'a Arc<UserTaskLock>,
    task_type: &'a str,
    completed: bool,
}

impl<'a> ActivityGuard<'a> {
    fn new(user_id: &'a str, manager: &'a Arc<UserTaskLock>, task_type: &'a str) -> Self {
        Self {
            user_id,
            manager,
            task_type,
            completed: false,
        }
    }
    
    fn mark_completed(&mut self) {
        self.completed = true;
    }
}

impl<'a> Drop for ActivityGuard<'a> {
    fn drop(&mut self) {
        if !self.completed {
            let user_id = self.user_id.to_string();
            let manager = self.manager.clone();
            let task_type = self.task_type.to_string();
            tokio::spawn(async move {
                manager.mark_user_inactive(&user_id).await;
                warn!(
                    "Failed to mark user {} as inactive for {}: task cleanup",
                    user_id, task_type
                );
            });
        }
    }
}

// SyncResult: Typed result of a sync operation
#[derive(Debug)]
enum SyncResult {
    Success(bool), // bool indicates if new funds were found
    Error(PulserError),
    Timeout,
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
        debug!("Attempting to acquire lock for {} operation on user {}, attempt {}/{}", operation, user_id, attempts + 1, max_retries + 1);
        
        match timeout(Duration::from_millis(timeout_ms), mutex.lock()).await {
            Ok(guard) => return Ok(guard),
            Err(_) => {
                attempts += 1;
                if attempts > max_retries {
                    return Err(PulserError::InternalError(format!("Timeout acquiring {} lock after {} attempts", operation, attempts)));
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

// Update user status with the sync result
async fn update_user_status(
    user_id: &str,
    user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>,
    result: &SyncResult,
    duration_ms: u64,
) -> Result<(), PulserError> {
    // Try to acquire the lock with retry
    let mut statuses = acquire_lock_with_retry(
        user_statuses,
        user_id,
        "user_statuses",
        DEFAULT_LOCK_TIMEOUT_MS,
        MAX_RETRY_ATTEMPTS
    ).await?;
    
    if let Some(status) = statuses.get_mut(user_id) {
        match result {
            SyncResult::Success(new_funds) => {
                status.sync_status = "success".to_string();
                status.last_success = now_timestamp();
                status.last_error = None;
                status.sync_duration_ms = duration_ms;
                status.last_update_message = if *new_funds {
                    "Sync completed with new funds".to_string()
                } else {
                    "Sync completed successfully".to_string()
                };
            },
            SyncResult::Error(err) => {
                status.sync_status = "error".to_string();
                status.last_error = Some(format!("{}", err));
                status.sync_duration_ms = duration_ms;
                status.last_update_message = format!("Sync failed: {}", err);
            },
            SyncResult::Timeout => {
                status.sync_status = "error".to_string();
                status.last_error = Some("Sync operation timed out".to_string());
                status.sync_duration_ms = duration_ms;
                status.last_update_message = "Sync timed out".to_string();
            }
        }
    } else {
        warn!("User status not found for user {} during status update", user_id);
    }
    
    Ok(())
}

// Update service statistics after sync
async fn update_service_stats(
    user_id: &str,
    service_status: &Arc<Mutex<ServiceStatus>>,
    wallets: &Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
) -> Result<(), PulserError> {
    // Update service stats with minimal lock times
    let mut stats = acquire_lock_with_retry(
        service_status,
        user_id,
        "service_status",
        DEFAULT_LOCK_TIMEOUT_MS,
        MAX_RETRY_ATTEMPTS
    ).await?;
    
    // Decrement active syncs safely
    stats.active_syncs = stats.active_syncs.saturating_sub(1);
    
    // Update total stats from wallets if we can acquire the lock quickly
    match timeout(Duration::from_secs(2), wallets.lock()).await {
        Ok(wallets_lock) => {
            stats.total_utxos = wallets_lock.values()
                .map(|(_, chain)| chain.utxos.len() as u32)
                .sum();
            
            stats.total_value_btc = wallets_lock.values()
                .map(|(wallet, _)| wallet.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0)
                .sum();
                
            stats.total_value_usd = wallets_lock.values()
                .map(|(_, chain)| chain.stabilized_usd.0)
                .sum();
        },
        Err(_) => {
            warn!("Timeout acquiring wallets lock during service stats update for user {}", user_id);
        }
    }
    
    Ok(())
}

// Helper function to get current Unix timestamp in seconds
fn now_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs()
}

// Sync endpoints
pub fn sync_route(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_info: Arc<Mutex<PriceInfo>>,
    esplora_urls: Arc<Mutex<Vec<(String, u64)>>>, // Match monitor.rs
    esplora: Arc<bdk_esplora::esplora_client::AsyncClient>,
    state_manager: Arc<StateManager>,
    webhook_url: String,
    webhook_manager: WebhookManager,
    client: Client,
    active_tasks_manager: Arc<common::task_manager::UserTaskLock>,
    price_feed: Arc<PriceFeed>,
    service_status: Arc<Mutex<common::types::ServiceStatus>>,
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

pub fn force_sync(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    active_tasks_manager: Arc<common::task_manager::UserTaskLock>,
    price_info: Arc<Mutex<PriceInfo>>,
    esplora: Arc<bdk_esplora::esplora_client::AsyncClient>,
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

async fn force_sync_handler(
    user_id: String,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    active_tasks_manager: Arc<common::task_manager::UserTaskLock>,
    price_info: Arc<Mutex<PriceInfo>>,
    esplora: Arc<bdk_esplora::esplora_client::AsyncClient>,
    state_manager: Arc<StateManager>,
    config: Arc<Config>
) -> Result<impl Reply, Rejection> {
    debug!("Received force_sync request for user {}", user_id);

    let price_feed = Arc::new(PriceFeed::new());

    let mut wallets_lock = match tokio::time::timeout(Duration::from_secs(3), wallets.lock()).await {
        Ok(lock) => lock,
        Err(_) => return Err(PulserError::InternalError("Timeout checking if user exists".to_string()))?,
    };

    let (wallet, chain) = if !wallets_lock.contains_key(&user_id) {
        debug!("User {} not found, initializing wallet", &user_id);
        match DepositWallet::from_config(&config, &user_id, &state_manager, price_feed.clone()).await {
            Ok((wallet, _, chain, _recovery_doc)) => {
                wallets_lock.insert(user_id.clone(), (wallet, chain));
                wallets_lock.get_mut(&user_id).expect("Just inserted")
            },
            Err(e) => return Err(e)?,
        }
    } else {
        match wallets_lock.get_mut(&user_id) {
            Some(entry) => entry,
            None => return Ok(warp::reply::json(&json!({"status": "error", "message": "User not found"})))
        }
    };

    if active_tasks_manager.is_user_active(&user_id).await {
        debug!("User {} already being synced", &user_id);
        return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
    }

    if !active_tasks_manager.mark_user_active(&user_id, "force_sync").await {
        return Ok(warp::reply::json(&json!({"status": "error", "message": "Failed to lock user for sync"})));
    }
    let _guard = super::ActivityGuard { user_id: &user_id, manager: &active_tasks_manager };

    let price_info = match tokio::time::timeout(Duration::from_secs(3), price_info.lock()).await {
        Ok(lock) => lock.clone(),
        Err(_) => return Err(PulserError::InternalError("Timeout acquiring price info".to_string()))?,
    };

    match wallet_sync::resync_full_history(
        &user_id,
        &mut wallet.wallet,
        &esplora,
        chain,
        price_feed.clone(),  // Add price_feed here
        &price_info,
        &state_manager,
        config.min_confirmations,
    ).await {
        Ok(new_utxos) => {
            info!("Full resync completed for user {}: {} new UTXOs", &user_id, new_utxos.len());
            Ok(warp::reply::json(&json!({"status": "ok", "message": format!("Full resync completed, {} new UTXOs", new_utxos.len())})))
        },
        Err(e) => Err(e)?,
    }
}

// Improved sync_user_handler with better concurrency and error handling
pub async fn sync_user_handler(
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
        active_tasks_manager.is_user_active(&user_id,)
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

    // Try to mark user as active with timeout
    let task_result = match timeout(
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

    // Create guard to ensure user is marked inactive if something fails
    let mut guard = ActivityGuard::new(&user_id, &active_tasks_manager, "sync");
    
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
    Ok(Ok(new_funds)) => SyncResult::Success(new_funds),
    Ok(Err(e)) => SyncResult::Error(e),
    Err(_) => SyncResult::Timeout,
};

let duration_ms = start_time.elapsed().as_millis() as u64;

// Update user status with sync result
if let Err(e) = update_user_status(&user_id, &user_statuses, &sync_result, duration_ms).await {
    warn!("Failed to update user status for {}: {}", user_id, e);
}

// Update service statistics
if let Err(e) = update_service_stats(&user_id, &service_status, &wallets).await {
    warn!("Failed to update service stats for {}: {}", user_id, e);
}

// No need for task_result.complete(); Drop handles it
guard.mark_completed();
    
    // Return appropriate response based on sync result
    match sync_result {
        SyncResult::Success(new_funds) => {
            let message = if new_funds { "Sync completed with new funds" } else { "Sync completed" };
            Ok(warp::reply::json(&json!({
                "status": "ok", 
                "message": message
            })))
        },
        SyncResult::Error(e) => {
            error!("Sync for user {} failed: {}", user_id, e);
            Err(warp::reject::custom(e))
        },
        SyncResult::Timeout => {
            error!("Sync for user {} timed out after {}s", user_id, SYNC_OPERATION_TIMEOUT_SECS);
            Err(warp::reject::custom(PulserError::InternalError("Sync operation timed out".to_string())))
        }
    }
}

// Improved sync_user implementation with better error handling and performance
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
    {
        let mut statuses = acquire_lock_with_retry(
            &user_statuses,
            user_id,
            "user_statuses",
            DEFAULT_LOCK_TIMEOUT_MS,
            MAX_RETRY_ATTEMPTS,
        )
        .await?;

        if let Some(status) = statuses.get_mut(user_id) {
            status.sync_status = "syncing".to_string();
            status.last_sync = now_timestamp();
            status.last_update_message = "Sync in progress".to_string();
        } else {
            warn!("User status not found for {} during sync start", user_id);
        }
    }

    // Extract wallet and chain with minimal lock time
    let (mut wallet, mut chain) = {
        let mut wallets_lock = acquire_lock_with_retry(
            &wallets,
            user_id,
            "wallets",
            DEFAULT_LOCK_TIMEOUT_MS,
            MAX_RETRY_ATTEMPTS,
        )
        .await?;

        match wallets_lock.remove(user_id) {
            Some((w, c)) => (w, c),
            None => {
                return Err(PulserError::UserNotFound(format!(
                    "Wallet not found for user {}",
                    user_id
                )))
            }
        }
    };

    // Track if new funds are discovered
    let mut new_funds_found = false;
    let mut error_occurred = false;

    // Get current price data
    let current_price = {
        let price_guard = acquire_lock_with_retry(
            &price_info,
            user_id,
            "price_info",
            DEFAULT_LOCK_TIMEOUT_MS,
            MAX_RETRY_ATTEMPTS,
        )
        .await?;

        price_guard.clone()
    };

    // Try to perform the sync operation with multiple error recovery attempts
    for attempt in 0..3 {
        if attempt > 0 {
            debug!("Retry attempt {}/3 for user {} sync", attempt, user_id);
        }
        
let deposit_addr = Address::from_str(&chain.multisig_addr)?.assume_checked();
let change_addr = wallet.wallet.reveal_next_address(KeychainKind::Internal).address;

match wallet_sync::sync_and_stabilize_utxos(
    user_id,
    &mut wallet.wallet,
    esplora,
    &mut chain,
    price_feed.clone(),
    &current_price, // Fixed
    &deposit_addr, // Use pre-computed value
    &change_addr,
    &state_manager,
    config.min_confirmations,
).await
        {
            Ok(_) => {
                // Check for new deposits
                let prev_balance = chain.stabilized_usd.0;
                let prev_utxo_count = chain.utxos.len();

                // Save the updated chain state
                if let Err(e) = state_manager.save_stable_chain(user_id, &chain).await {
                    warn!("Error saving StableChain for user {}: {}", user_id, e);
                    if attempt < 2 {
                        continue; // Try again
                    } else {
                        error_occurred = true;
                        break;
                    }
                }

                // Save wallet checkpoint
                if let Some(changeset) = wallet.wallet.take_staged() {
                    if let Err(e) = state_manager.save_changeset(user_id, &changeset).await {
                        warn!("Error saving wallet changeset for user {}: {}", user_id, e);
                    }
                }

                // Check if new funds were found
                let new_balance = chain.stabilized_usd.0;
                let new_utxo_count = chain.utxos.len();

if new_balance > prev_balance || new_utxo_count > prev_utxo_count {
    new_funds_found = true;
    debug!(
        "New funds for user {}: ${:.2} -> ${:.2}, UTXOs: {} -> {}",
        user_id, prev_balance, new_balance, prev_utxo_count, new_utxo_count
    );

    if !webhook_url.is_empty() && new_funds_found {
        let webhook_data = json!({
            "event": "deposit",
            "user_id": user_id,
            "amount_usd": new_balance - prev_balance,
            "amount_btc": wallet.wallet.balance().total().to_sat() as f64 / 100_000_000.0,
            "utxo_count": new_utxo_count,
            "timestamp": now_timestamp()
        });

        let user_id_owned = user_id.to_string();
        let webhook_url_owned = webhook_url.to_string();
        tokio::spawn(async move {
            let payload = WebhookPayload {
                event: "deposit".to_string(),
                user_id: user_id_owned.clone(), // Clone for logging if needed
                data: webhook_data.clone(),
                timestamp: common::utils::now_timestamp() as u64,
            };
            match timeout(Duration::from_secs(WEBHOOK_TIMEOUT_SECS), webhook_manager.send(&webhook_url_owned, payload)).await {
                Ok(Ok(_)) => debug!(
                    "Webhook sent successfully for new deposit for user {}",
                    user_id_owned // Use owned String
                ),
                Ok(Err(e)) => warn!(
                    "Failed to send webhook for user {}: {}",
                    user_id_owned, e // Use owned String
                ),
                Err(_) => warn!(
                    "Webhook send timed out for user {}",
                    user_id_owned // Use owned String
                ),
            }
        }); // Single closing brace for if block
    }

    // Sync was successful, break out of retry loop
    break;
}
            }
            Err(e) => {
                warn!(
                    "Error syncing wallet for user {} (attempt {}/3): {}",
                    user_id,
                    attempt + 1,
                    e
                );
                if attempt < 2 {
                    tokio::time::sleep(Duration::from_millis(500 * (attempt + 1) as u64)).await;
                    continue;
                } else {
                    error_occurred = true;
                    break;
                }
            }
        }
    }

    // Always put the wallet back, even if there was an error
    {
        let mut wallets_lock = acquire_lock_with_retry(
            &wallets,
            user_id,
            "wallets",
            DEFAULT_LOCK_TIMEOUT_MS,
            MAX_RETRY_ATTEMPTS,
        )
        .await?;

        wallets_lock.insert(user_id.to_string(), (wallet, chain));
    }

    // Update user status with final state
    {
        let mut statuses = acquire_lock_with_retry(
            &user_statuses,
            user_id,
            "user_statuses",
            DEFAULT_LOCK_TIMEOUT_MS,
            MAX_RETRY_ATTEMPTS,
        )
        .await?;

        if let Some(status) = statuses.get_mut(user_id) {
            if error_occurred {
                status.sync_status = "error".to_string();
                status.last_error = Some("Sync operation failed after multiple attempts".to_string());
            } else {
                status.sync_status = "success".to_string();
                status.last_success = now_timestamp();
                status.last_error = None;

                if new_funds_found {
                    status.last_deposit_time = Some(now_timestamp());
                }
            }

            status.sync_duration_ms = start_time.elapsed().as_millis() as u64;
        }
    }

    // Report error if one occurred
    if error_occurred {
        return Err(PulserError::SyncError(format!(
            "Sync failed for user {} after multiple attempts",
            user_id
        )));
    }

    debug!(
        "Sync completed for user {} in {}ms",
        user_id,
        start_time.elapsed().as_millis()
    );
    Ok(new_funds_found)
}

/// Sends a webhook notification for new UTXOs with enhanced summary data
///
/// This function creates a rich payload with summary information about the new UTXOs
/// and sends it asynchronously via the WebhookManager, properly handling retries
/// and logging.
async fn spawn_webhook_notification(
    user_id: &str,
    new_utxos: &[UtxoInfo],
    chain: &StableChain,
    webhook_manager: WebhookManager,
    webhook_url: &str,
) {
    // Clone what we need
    let user_id_str = user_id.to_string();
    let webhook_manager = webhook_manager.clone();
    let webhook_url = webhook_url.to_string();
    let new_utxos_cloned = new_utxos.to_vec(); // Clone the utxos
    let chain_clone = chain.clone();
    
    // Calculate summary information
    let total_btc: f64 = new_utxos.iter()
        .map(|u| u.amount_sat as f64 / 100_000_000.0)
        .sum();
    let total_stable_usd: f64 = new_utxos.iter()
        .map(|u| u.stable_value_usd)
        .sum();
    
    // Create payload with richer information
    let payload = WebhookPayload {
        event: "new_funds".to_string(),
        user_id: user_id_str.clone(),
        data: serde_json::json!({
            "utxos": new_utxos_cloned, // Use the cloned data
            "chain": chain_clone,      // Use the cloned chain
            "summary": {
                "total_btc": total_btc,
                "total_stable_usd": total_stable_usd,
                "utxo_count": new_utxos.len(),
                "current_price": chain.raw_btc_usd,
                "timestamp": common::utils::now_timestamp(), // Use utils version
            }
        }),
        timestamp: common::utils::now_timestamp() as u64, // Use utils version
    };
    
    // Launch notification as non-blocking task
    tokio::spawn(async move {
        match webhook_manager.send(&webhook_url, payload).await {
            Ok(_) => {
                debug!("Webhook sent for user {}: {} new UTXOs (${:.2})", 
                      user_id_str, new_utxos_cloned.len(), total_stable_usd);
            },
            Err(e) => {
                warn!("Webhook failed for user {}: {}. Queued for retry.", 
                     user_id_str, e);
            }
        }
    });
}
