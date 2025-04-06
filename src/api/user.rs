use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;
use serde_json::json;
use log::warn;
use std::time::Duration;
use common::error::PulserError;
use common::types::{StableChain, UserStatus};
use crate::wallet::DepositWallet;
use bitcoin::Address;
use std::str::FromStr;

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

async fn user_status_handler(
    user_id: String,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
) -> Result<impl Reply, Rejection> {
    let wallets_lock = tokio::time::timeout(Duration::from_secs(5), wallets.lock())
        .await
        .map_err(|_| PulserError::InternalError("Timeout acquiring wallets lock".to_string()))?;
    
    let statuses_lock = tokio::time::timeout(Duration::from_secs(5), statuses.lock())
        .await
        .map_err(|_| PulserError::InternalError("Timeout acquiring statuses lock".to_string()))?;
    
    let status = statuses_lock
        .get(&user_id)
        .ok_or_else(|| PulserError::UserNotFound("User not found".to_string()))?;
        
    let (wallet, _) = wallets_lock
        .get(&user_id)
        .ok_or_else(|| PulserError::UserNotFound("User not found".to_string()))?;

    let balance = wallet.wallet.balance();
    Ok(warp::reply::json(&json!({
        "user_id": user_id,
        "status": status,
        "balance": {
            "confirmed_btc": balance.confirmed.to_sat() as f64 / 100_000_000.0,
            "unconfirmed_btc": balance.untrusted_pending.to_sat() as f64 / 100_000_000.0,
            "immature_btc": balance.immature.to_sat() as f64 / 100_000_000.0
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
    let wallets_lock = match tokio::time::timeout(Duration::from_secs(5), wallets.lock()).await {
        Ok(lock) => lock,
        Err(_) => {
            warn!("Timeout acquiring wallets lock for user {} transactions", user_id);
            return Err(PulserError::InternalError("Timeout acquiring wallets lock".to_string()))?;
        }
    };
    
    match wallets_lock.get(&user_id) {
        Some((wallet, _)) => {
            let txs: Vec<TxInfo> = wallet.wallet.transactions()
                .map(|tx| {
                    let tx_node = &*tx.tx_node;
                    let amount = tx_node.output.iter().map(|o| o.value.to_sat()).sum();
                    let is_spent = wallet.wallet.list_unspent()
                        .filter(|u| u.outpoint.txid == tx_node.compute_txid())
                        .all(|u| u.is_spent);
                    let (height, timestamp) = match tx.chain_position {
                        bdk_chain::ChainPosition::Confirmed { anchor, .. } => {
                            (Some(anchor.block_id.height), Some(anchor.confirmation_time))
                        }
                        bdk_chain::ChainPosition::Unconfirmed { .. } => (None, None),
                    };
                    TxInfo {
                        txid: tx_node.compute_txid().to_string(),
                        confirmation_time: height,
                        amount,
                        is_spent,
                        timestamp,
                    }
                })
                .collect();
            Ok::<_, Rejection>(warp::reply::json(&txs))
        }
        None => Ok::<_, Rejection>(warp::reply::json(&json!({"error": "User not found"}))),
    }
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
        return Err(PulserError::InvalidRequest("Invalid user ID".to_string()))?;
    }

    // First, check user status
    let status_lock = match tokio::time::timeout(Duration::from_secs(3), user_statuses.lock()).await {
        Ok(lock) => lock,
        Err(_) => return Err(PulserError::InternalError("Timeout acquiring user statuses".to_string()))?,
    };

    // Verify user exists in statuses
    let status = status_lock.get(&user_id)
        .ok_or_else(|| PulserError::UserNotFound("User not found".to_string()))?;

    // Check if wallet is loaded
    let wallets_lock = match tokio::time::timeout(Duration::from_secs(3), wallets.lock()).await {
        Ok(lock) => lock,
        Err(_) => return Err(PulserError::InternalError("Timeout acquiring wallets".to_string()))?,
    };

    // Ensure wallet is loaded
    let (wallet, _) = wallets_lock.get(&user_id)
        .ok_or_else(|| PulserError::WalletError("Wallet not loaded".to_string()))?;

    // Determine wallet readiness
    let ready = status.sync_status != "error" 
                && status.sync_status != "initializing"
                && !status.current_deposit_address.is_empty();

    Ok(warp::reply::json(&json!({
        "address": status.current_deposit_address,
        "ready": ready
    })))
}
