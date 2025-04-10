// deposit-service/src/api/user.rs - Enhanced implementation
use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::timeout;
use std::collections::HashMap;
use serde_json::json;
use log::{info, warn, debug, error};
use std::time::Duration;
use common::error::PulserError;
use common::types::{StableChain, UserStatus};
use crate::wallet::DepositWallet;
use bitcoin::Address;
use std::str::FromStr;

// Constants for timeouts
const DEFAULT_LOCK_TIMEOUT_MS: u64 = 1000;
const OPERATION_TIMEOUT_SECS: u64 = 5;
const MAX_RETRY_ATTEMPTS: usize = 3;

// Type for transaction info
#[derive(serde::Serialize)]
struct TxInfo {
    txid: String,
    confirmation_time: Option<u32>, // Block height if confirmed
    amount: u64,                    // Total output value in satoshis
    is_spent: bool,                 // Are outputs spent?
    timestamp: Option<u64>,         // Unix timestamp of confirmation
}

// User status endpoint
pub fn user_status(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path("user")
        .and(warp::path::param::<String>())
        .and(warp::get())
        .and(super::with_wallets(wallets))
        .and(super::with_statuses(statuses))
        .and_then(user_status_handler)
}

// Helper function to acquire a lock with timeout and backoff
async fn acquire_lock_with_retry<'a, T>(
    mutex: &'a Arc<Mutex<T>>,
    user_id: &str,
    operation: &str,
    timeout_ms: u64,
) -> Result<tokio::sync::MutexGuard<'a, T>, PulserError> {
    let mut attempts = 0;
    let max_retries = MAX_RETRY_ATTEMPTS;
    
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
                let delay = 250 * (1 << attempts);
                warn!("Timeout acquiring {} lock for user {}, retrying in {}ms (attempt {}/{})", 
                      operation, user_id, delay, attempts, max_retries + 1);
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        }
    }
}

async fn user_status_handler(
    user_id: String,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
) -> Result<impl Reply, Rejection> {
    // Validate user ID
    if user_id.is_empty() || !user_id.chars().all(|c| c.is_ascii_digit()) {
        return Err(PulserError::InvalidRequest("Invalid user ID format".to_string()))?;
    }

    // Get user status with improved error handling
    let status = match timeout(
        Duration::from_secs(OPERATION_TIMEOUT_SECS), 
        acquire_lock_with_retry(&statuses, &user_id, "statuses", DEFAULT_LOCK_TIMEOUT_MS)
    ).await {
        Ok(Ok(statuses_guard)) => {
            match statuses_guard.get(&user_id) {
                Some(status) => status.clone(),
                None => return Err(PulserError::UserNotFound(format!("User {} not found", user_id)))?
            }
        },
        Ok(Err(e)) => {
            error!("Error acquiring statuses lock for user {}: {}", user_id, e);
            return Err(e)?;
        },
        Err(_) => {
            error!("Timeout waiting for status lock acquisition for user {}", user_id);
            return Err(PulserError::Timeout(format!("Timeout retrieving status for user {}", user_id)))?;
        }
    };
        
    // Fetch balance and wallet data with minimal lock time
    let balance_data = match timeout(
        Duration::from_secs(OPERATION_TIMEOUT_SECS),
        acquire_lock_with_retry(&wallets, &user_id, "wallets", DEFAULT_LOCK_TIMEOUT_MS)
    ).await {
        Ok(Ok(wallets_guard)) => {
            match wallets_guard.get(&user_id) {
                Some((wallet, chain)) => {
                    // Get just the data we need while holding lock
                    let balance = wallet.wallet.balance();
                    let btc_confirmed = balance.confirmed.to_sat() as f64 / 100_000_000.0;
                    let btc_unconfirmed = balance.untrusted_pending.to_sat() as f64 / 100_000_000.0;
                    let btc_immature = balance.immature.to_sat() as f64 / 100_000_000.0;
                    let usd_value = chain.stabilized_usd.0;
                    let utxo_count = chain.utxos.len();
                
                    // Collect other relevant data
                    (btc_confirmed, btc_unconfirmed, btc_immature, usd_value, utxo_count)
                },
                None => return Err(PulserError::UserNotFound(format!("User {} wallet not found", user_id)))?
            }
        },
        Ok(Err(e)) => {
            error!("Error acquiring wallets lock for user {}: {}", user_id, e);
            return Err(e)?;
        },
        Err(_) => {
            error!("Timeout waiting for wallets lock acquisition for user {}", user_id);
            return Err(PulserError::Timeout(format!("Timeout retrieving wallet for user {}", user_id)))?;
        }
    };
    
    let (confirmed_btc, unconfirmed_btc, immature_btc, stable_value_usd, utxo_count) = balance_data;
    
    debug!("Retrieved status for user {}: ${:.2}, {} utxos", user_id, stable_value_usd, utxo_count);

    // Return comprehensive user information
    Ok(warp::reply::json(&json!({
        "user_id": user_id,
        "status": {
            "sync_status": status.sync_status,
            "last_sync": status.last_sync,
            "last_update_message": status.last_update_message,
            "confirmations_pending": status.confirmations_pending,
            "last_error": status.last_error,
            "last_success": status.last_success,
            "sync_duration_ms": status.sync_duration_ms
        },
        "balance": {
            "confirmed_btc": confirmed_btc,
            "unconfirmed_btc": unconfirmed_btc,
            "immature_btc": immature_btc,
            "stable_value_usd": stable_value_usd
        },
        "deposit": {
            "address": status.current_deposit_address,
            "utxo_count": utxo_count,
            "last_deposit_time": status.last_deposit_time
        }
    })))
}

// User transactions endpoint
pub fn user_txs(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path("user")
        .and(warp::path::param::<String>())
        .and(warp::path("txs"))
        .and(warp::get())
        .and(super::with_wallets(wallets))
        .and_then(user_txs_handler)
}

async fn user_txs_handler(
    user_id: String,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
) -> Result<impl Reply, Rejection> {
    // Validate user ID
    if user_id.is_empty() || !user_id.chars().all(|c| c.is_ascii_digit()) {
        return Err(PulserError::InvalidRequest("Invalid user ID format".to_string()))?;
    }

    // Get transactions with improved error handling
    let transactions = match timeout(
        Duration::from_secs(OPERATION_TIMEOUT_SECS),
        acquire_lock_with_retry(&wallets, &user_id, "wallets", DEFAULT_LOCK_TIMEOUT_MS)
    ).await {
        Ok(Ok(wallets_guard)) => {
            match wallets_guard.get(&user_id) {
                Some((wallet, chain)) => {
                    // Process transactions with chain context
                    let txs: Vec<TxInfo> = wallet.wallet.transactions()
                        .map(|tx| {
                            let tx_node = &*tx.tx_node;
                            let amount = tx_node.output.iter().map(|o| o.value.to_sat()).sum();
                            
                            // Check if this transaction has any UTXOs in this wallet
                            let is_spent = wallet.wallet.list_unspent()
                                .filter(|u| u.outpoint.txid == tx_node.compute_txid())
                                .all(|u| u.is_spent);
                                
                            let (height, timestamp) = match tx.chain_position {
                                bdk_chain::ChainPosition::Confirmed { anchor, .. } => {
                                    (Some(anchor.block_id.height), Some(anchor.confirmation_time))
                                }
                                bdk_chain::ChainPosition::Unconfirmed { .. } => (None, None),
                            };
                            
                            // Find USD value in StableChain if available
                            let usd_value = chain.utxos.iter()
                                .find(|utxo| utxo.txid == tx_node.compute_txid().to_string())
                                .and_then(|utxo| utxo.usd_value.as_ref().map(|usd| usd.0));
                                
                            TxInfo {
                                txid: tx_node.compute_txid().to_string(),
                                confirmation_time: height,
                                amount,
                                is_spent,
                                timestamp,
                            }
                        })
                        .collect();
                    txs
                },
                None => return Err(PulserError::UserNotFound(format!("User {} wallet not found", user_id)))?
            }
        },
        Ok(Err(e)) => {
            error!("Error acquiring wallets lock for user {} transactions: {}", user_id, e);
            return Err(e)?;
        },
        Err(_) => {
            error!("Timeout waiting for wallets lock for user {} transactions", user_id);
            return Err(PulserError::Timeout(format!("Timeout retrieving transactions for user {}", user_id)))?;
        }
    };
    
    debug!("Retrieved {} transactions for user {}", transactions.len(), user_id);
    Ok(warp::reply::json(&transactions))
}

// User address endpoint
pub fn user_address(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path!("user" / String / "address")
        .and(warp::get())
        .and(super::with_wallets(wallets))
        .and(super::with_statuses(user_statuses))
        .and_then(user_address_handler)
}

async fn user_address_handler(
    user_id: String,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
) -> Result<impl Reply, Rejection> {
    // Validate user ID
    if user_id.is_empty() || !user_id.chars().all(|c| c.is_ascii_digit()) {
        return Err(PulserError::InvalidRequest("Invalid user ID format".to_string()))?;
    }

    // Get address with improved error handling
    let address_data = match timeout(
        Duration::from_secs(OPERATION_TIMEOUT_SECS),
        async {
            // First get status for last deposit address
            let status_guard = acquire_lock_with_retry(&user_statuses, &user_id, "statuses", DEFAULT_LOCK_TIMEOUT_MS).await?;
            let status = status_guard.get(&user_id)
                .ok_or_else(|| PulserError::UserNotFound(format!("User {} not found", user_id)))?;
                
            let address = status.current_deposit_address.clone();
            let sync_status = status.sync_status.clone();
            
            // Release status lock before getting wallet
            drop(status_guard);
            
            // Now get wallet to check if it exists and has an address
            let wallets_guard = acquire_lock_with_retry(&wallets, &user_id, "wallets", DEFAULT_LOCK_TIMEOUT_MS).await?;
            let wallet_exists = wallets_guard.contains_key(&user_id);
            
            // Determine wallet readiness
            let ready = wallet_exists && 
                       sync_status != "error" && 
                       sync_status != "initializing" &&
                       !address.is_empty();
                       
            Ok::<_, PulserError>((address, ready))
        }
    ).await {
        Ok(Ok(data)) => data,
        Ok(Err(e)) => {
            error!("Error getting address for user {}: {}", user_id, e);
            return Err(e)?;
        },
        Err(_) => {
            error!("Timeout getting address for user {}", user_id);
            return Err(PulserError::Timeout(format!("Timeout retrieving address for user {}", user_id)))?;
        }
    };
    
    let (address, ready) = address_data;
    
    debug!("Retrieved address for user {}: {}, ready: {}", user_id, address, ready);
    Ok(warp::reply::json(&json!({
        "address": address,
        "ready": ready
    })))
}
