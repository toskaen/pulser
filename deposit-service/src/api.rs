use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use serde_json::json;
use log::{info, warn, debug, error};
use bdk_wallet::{KeychainKind, bitcoin::{Address, Network}};
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use reqwest::Client;
use common::error::PulserError; // Use PulserError exclusively
use common::types::PriceInfo;
use crate::wallet::DepositWallet;
use common::{ServiceStatus, UserStatus, StableChain, WebhookRetry};
use crate::webhook::{WebhookConfig, notify_new_utxos};
use tokio::time::sleep;
use crate::init_pulser_wallet::init_pulser_wallet;
use common::task_manager::UserTaskLock;
use common::price_feed::PriceFeed;
use common::StateManager;
use common::wallet_sync;
use serde::Serialize;
use common::types::UtxoInfo;
use common::wallet_sync::{resync_full_history, sync_and_stabilize_utxos};
use futures_util::FutureExt;
use std::pin::Pin;
use warp::reply::Json;
use crate::config::Config;
use bdk_chain::spk_client::SyncRequest;
use bdk_esplora::EsploraAsyncExt;
use bdk_chain::ChainPosition;
use crate::monitor::check_mempool_for_address;
use bdk_wallet::keys::DerivableKey;

#[derive(Serialize)]
struct TxInfo {
    txid: String,
    confirmation_time: Option<u32>, // Block height if confirmed
    amount: u64,                    // Total output value in satoshis
    is_spent: bool,                 // Are outputs spent?
    timestamp: Option<u64>,         // Unix timestamp of confirmation
}

// ActivityGuard for managing user task lifecycle
struct ActivityGuard<'a> {
    user_id: &'a str,
    manager: &'a Arc<UserTaskLock>,
}

impl<'a> Drop for ActivityGuard<'a> {
    fn drop(&mut self) {
        let user_id = self.user_id.to_string();
        let manager = self.manager.clone();
        tokio::spawn(async move {
            manager.mark_user_inactive(&user_id).await;
        });
    }
}

pub fn routes(
    service_status: Arc<Mutex<ServiceStatus>>,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_info: Arc<Mutex<PriceInfo>>,
    esplora_urls: Arc<Mutex<Vec<(String, u32)>>>,
    state_manager: Arc<StateManager>,
    sync_tx: mpsc::Sender<String>,
    retry_queue: Arc<Mutex<VecDeque<WebhookRetry>>>,
    webhook_url: String,
    webhook_config: WebhookConfig,
    client: Client,
    active_tasks_manager: Arc<UserTaskLock>,
    price_feed: Arc<PriceFeed>,
    esplora: Arc<bdk_esplora::esplora_client::AsyncClient>,
    config: Arc<Config>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let client_for_sync = client.clone();
    let price_feed_for_sync = price_feed.clone();
    let esplora_clone = Arc::clone(&esplora);

    let health = warp::path("health")
        .and(warp::get())
        .and(with_service_status(service_status.clone()))
        .and(with_esplora_urls(esplora_urls.clone()))
        .and(with_price(price_info.clone()))
        .and(with_active_tasks_manager(active_tasks_manager.clone()))
        .and_then({
            move |service_status: Arc<Mutex<ServiceStatus>>, 
                  esplora_urls: Arc<Mutex<Vec<(String, u32)>>>, 
                  price_info: Arc<Mutex<PriceInfo>>,
                  active_tasks_manager: Arc<UserTaskLock>| async move {
                let status = match tokio::time::timeout(Duration::from_secs(3), service_status.lock()).await {
                    Ok(status) => status,
                    Err(_) => return Err(PulserError::InternalError("Timeout acquiring service status lock".to_string()))?,
                };
                
                let esplora_urls_lock = match tokio::time::timeout(Duration::from_secs(3), esplora_urls.lock()).await {
                    Ok(lock) => lock,
                    Err(_) => return Err(PulserError::InternalError("Timeout acquiring esplora URLs lock".to_string()))?,
                };
                
                let price_info_lock = match tokio::time::timeout(Duration::from_secs(3), price_info.lock()).await {
                    Ok(lock) => lock,
                    Err(_) => return Err(PulserError::InternalError("Timeout acquiring price info lock".to_string()))?,
                };
                
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0)).as_secs();
                let websocket_active = status.websocket_active;
                
                let mut esplora_status = HashMap::new();
                let mut esplora_details = HashMap::new();
                let mut any_healthy_esplora = false;
                
                for (url, errors) in esplora_urls_lock.iter() {
                    let is_healthy = *errors < 10;
                    esplora_status.insert(url.clone(), is_healthy);
                    esplora_details.insert(url.clone(), json!({
                        "healthy": is_healthy,
                        "error_count": errors,
                        "last_used": now - status.last_update,
                        "primary": url == &esplora_urls_lock[0].0
                    }));
                    if is_healthy {
                        any_healthy_esplora = true;
                    }
                }
                
                let active_tasks = active_tasks_manager.get_active_tasks().await;
                let task_counts = active_tasks_manager.get_task_counts().await;
                
                let price_staleness_secs = now - price_info_lock.timestamp as u64;
                let price_healthy = price_staleness_secs < 120 && price_info_lock.raw_btc_usd > 1000.0;
                
                let overall_status = if websocket_active && price_healthy && any_healthy_esplora {
                    "healthy"
                } else if !any_healthy_esplora {
                    "critical"
                } else if !websocket_active || !price_healthy {
                    "degraded"
                } else {
                    "warning"
                };
                
                let websocket_action = if websocket_active { "WebSocket operational" } else { "Action: Check WebSocket connection to Deribit" };
                let price_action = if price_healthy { "Price feed current" } else if price_info_lock.raw_btc_usd <= 1000.0 { "Action: Invalid price detected, check price sources" } else { "Action: Price feed stale, check Deribit connectivity" };
                let esplora_action = if any_healthy_esplora { "At least one Esplora endpoint operational" } else { "CRITICAL: All Esplora endpoints failing, service severely degraded" };
                
                Ok::<_, Rejection>(warp::reply::json(&json!({
                    "status": overall_status,
                    "timestamp": now,
                    "health": status.health.clone(),
                    "active_syncs": status.active_syncs,
                    "websocket": { 
                        "active": websocket_active, 
                        "action": websocket_action,
                        "last_connected": status.last_update
                    },
                    "price": { 
                        "staleness_secs": price_staleness_secs, 
                        "last_price": price_info_lock.raw_btc_usd, 
                        "action": price_action,
                        "sources": price_info_lock.price_feeds.keys().collect::<Vec<_>>()
                    },
                    "esplora": { 
                        "status": if any_healthy_esplora { "operational" } else { "failing" },
                        "endpoints": esplora_status, 
                        "details": esplora_details,
                        "action": esplora_action
                    },
                    "tasks": {
                        "active_count": active_tasks.len(),
                        "by_type": task_counts,
                        "details": active_tasks
                    },
                    "system": {
                        "users_monitored": status.users_monitored,
                        "total_utxos": status.total_utxos,
                        "total_value_btc": status.total_value_btc,
                        "total_value_usd": status.total_value_usd,
                        "uptime_seconds": now - status.up_since
                    }
                })))
            }
        });

    let user_status = warp::path("user")
        .and(warp::path::param::<String>())
        .and(warp::get())
        .and(with_wallets(wallets.clone()))
        .and(with_statuses(user_statuses.clone()))
        .and_then({
            move |user_id, 
                  wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>, 
                  statuses: Arc<Mutex<HashMap<String, UserStatus>>>| {
                async move {
                    let wallets_lock = match tokio::time::timeout(Duration::from_secs(5), wallets.lock()).await {
                        Ok(lock) => lock,
                        Err(_) => return Err(PulserError::InternalError("Timeout acquiring wallets lock".to_string()))?,
                    };
                    
                    let statuses_lock = match tokio::time::timeout(Duration::from_secs(5), statuses.lock()).await {
                        Ok(lock) => lock,
                        Err(_) => return Err(PulserError::InternalError("Timeout acquiring statuses lock".to_string()))?,
                    };
                    
                    match (statuses_lock.get(&user_id), wallets_lock.get(&user_id)) {
                        (Some(status), Some((wallet, _))) => {
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
                        _ => Ok(warp::reply::json(&json!({"error": "User not found"}))),
                    }
                }
                .boxed() as Pin<Box<dyn futures_util::Future<Output = Result<Json, Rejection>> + Send>>
            }
        });

    let user_txs = warp::path("user")
        .and(warp::path::param::<String>())
        .and(warp::path("txs"))
        .and(warp::get())
        .and(with_wallets(wallets.clone()))
        .and_then({
            move |user_id, 
                  wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>| async move {
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
        });

    let sync_route = warp::post()
        .and(warp::path!("sync" / String))
        .and(with_wallets(wallets.clone()))
        .and(with_statuses(user_statuses.clone()))
        .and(with_price(price_info.clone()))
        .and(with_esplora_urls(esplora_urls.clone()))
        .and(warp::any().map({
            let esplora_clone = Arc::clone(&esplora_clone);
            move || Arc::clone(&esplora_clone)
        }))
        .and(with_state_manager(state_manager.clone()))
        .and(with_retry_queue(retry_queue.clone()))
        .and(warp::any().map(move || webhook_url.clone()))
        .and(warp::any().map(move || webhook_config.clone()))
        .and(warp::any().map(move || client_for_sync.clone()))
        .and(with_active_tasks_manager(active_tasks_manager.clone()))
        .and(warp::any().map(move || price_feed_for_sync.clone()))
        .and(with_service_status(service_status.clone()))
        .and(with_config(config.clone()))
        .and_then(sync_user_handler);

    let force_sync = warp::path!("force_sync" / String)
        .and(warp::post())
        .and(with_wallets(wallets.clone()))
        .and(with_active_tasks_manager(active_tasks_manager.clone()))
        .and(with_price(price_info.clone()))
        .and(warp::any().map({
            let esplora_clone = Arc::clone(&esplora);
            move || Arc::clone(&esplora_clone)
        }))
        .and(with_state_manager(state_manager.clone()))
        .and(with_config(config.clone()))
        .and_then(force_sync_handler);

let register = warp::path("register")
    .and(warp::post())
    .and(warp::body::json())
    .and(with_config(config.clone()))
    .and_then({
        let state_manager = state_manager.clone();
        let wallets = wallets.clone();
        let service_status = service_status.clone();
        let user_statuses = user_statuses.clone();
        let price_feed = price_feed.clone();
        move |public_data: serde_json::Value, config: Arc<Config>| {
            let state_manager = state_manager.clone();
            let wallets = wallets.clone();
            let service_status = service_status.clone();
            let user_statuses = user_statuses.clone();
            let price_feed = price_feed.clone();
            async move {
                let user_id = match public_data["user_id"].as_str() {
                    Some(id) if !id.is_empty() && id.chars().all(|c| c.is_ascii_digit()) => id.to_string(),
                    _ => return Err(PulserError::InvalidRequest("Invalid or missing user_id".to_string()))?,
                };
                let user_dir = PathBuf::from(format!("{}/user_{}", config.data_dir, user_id));
                let public_path = user_dir.join(format!("user_{}_public.json", user_id));
                let mut wallets_lock = match tokio::time::timeout(Duration::from_secs(5), wallets.lock()).await {
                    Ok(lock) => lock,
                    Err(_) => {
                        warn!("Timeout acquiring wallets lock for user {} registration", user_id);
                        return Ok(warp::reply::json(&json!({
                            "status": "registered",
                            "message": "User registered but wallet initialization delayed"
                        })));
                    }
                };
                if !wallets_lock.contains_key(&user_id) {
                    match DepositWallet::from_config(&config, &user_id, &state_manager, price_feed.clone()).await {
                        Ok((wallet, deposit_info, chain, recovery_doc)) => {
                            if !public_path.exists() {
                                // Use data from from_config instead of re-running init_pulser_wallet
                                let public_data = json!({
                                    "wallet_descriptor": deposit_info.descriptor,
"internal_descriptor": wallet.wallet.public_descriptor(KeychainKind::Internal).to_string(),
                                    "lsp_pubkey": config.lsp_pubkey,
                                    "trustee_pubkey": config.trustee_pubkey,
                                    "user_pubkey": deposit_info.user_pubkey,
                                    "user_id": user_id
                                });
                                match tokio::time::timeout(Duration::from_secs(5), state_manager.save(&public_path, &public_data)).await {
                                    Ok(Ok(_)) => info!("Public data saved to {}", public_path.display()),
                                    Ok(Err(e)) => {
                                        error!("Failed to save user {} public data: {}", user_id, e);
                                        return Err(e)?;
                                    }
                                    Err(_) => return Err(PulserError::InternalError("Timeout saving user data".to_string()))?,
                                }
                            }
                            let mut statuses = match tokio::time::timeout(Duration::from_secs(5), user_statuses.lock()).await {
                                Ok(lock) => lock,
                                Err(_) => {
                                    wallets_lock.insert(user_id.clone(), (wallet, chain));
                                    return Ok(warp::reply::json(&json!({
                                        "status": "registered",
                                        "message": "User registered but status initialization delayed"
                                    })));
                                }
                            };
                            statuses.insert(user_id.clone(), UserStatus {
                                user_id: user_id.clone(),
                                last_sync: 0,
                                sync_status: "initialized".to_string(),
                                utxo_count: 0,
                                total_value_btc: 0.0,
                                total_value_usd: 0.0,
                                confirmations_pending: false,
                                last_update_message: "User initialized".to_string(),
                                sync_duration_ms: 0,
                                last_error: None,
                                last_success: 0,
                                pruned_utxo_count: 0,
                                current_deposit_address: deposit_info.address.clone(),
                                last_deposit_time: None,
                            });
                            wallets_lock.insert(user_id.clone(), (wallet, chain));
                            match tokio::time::timeout(Duration::from_secs(5), service_status.lock()).await {
                                Ok(mut status) => status.users_monitored = wallets_lock.len() as u32,
                                Err(_) => warn!("Timeout updating service status for new user {}", user_id)
                            }
                            info!("Registered user {} with descriptors", user_id);
                            Ok::<_, Rejection>(warp::reply::json(&json!({
                                "status": "registered",
                                "deposit_address": deposit_info.address,
                                "user_xpub": deposit_info.user_pubkey,
                                "recovery_doc": recovery_doc
                            })))
                        },
                        Err(e) => Err(e)?,
                    }
                } else {
                    debug!("User {} already registered, skipping wallet init", user_id);
                    let (existing_wallet, existing_chain) = wallets_lock.get(&user_id).unwrap();
                    let deposit_address = existing_chain.multisig_addr.clone();
                    if !public_path.exists() {
                        let public_data = json!({
"wallet_descriptor": existing_wallet.wallet.public_descriptor(KeychainKind::External).to_string(),
                          "internal_descriptor": existing_wallet.wallet.public_descriptor(KeychainKind::Internal).to_string(),
                            "lsp_pubkey": config.lsp_pubkey,
                            "trustee_pubkey": config.trustee_pubkey,
"user_pubkey": existing_chain.multisig_addr.clone(), // or use deposit_info.user_pubkey if available
                            "user_id": user_id
                        });
                        match tokio::time::timeout(Duration::from_secs(5), state_manager.save(&public_path, &public_data)).await {
                            Ok(Ok(_)) => info!("Updated public data for existing user {}", user_id),
                            Ok(Err(e)) => error!("Failed to update public data for {}: {}", user_id, e),
                            Err(_) => error!("Timeout updating public data for {}", user_id),
                        }
                    }
                    Ok::<_, Rejection>(warp::reply::json(&json!({
                        "status": "registered",
                        "deposit_address": deposit_address
                    })))
                }
            }
        }
    });

let user_address = warp::path!("user" / String / "address")
        .and(warp::get())
        .and(with_wallets(wallets.clone()))
        .and(with_statuses(user_statuses.clone()))
        .and_then(move |user_id: String, wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>, user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>| async move {
            // Validate user ID (optional, but recommended)
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

            Ok::<_, Rejection>(warp::reply::json(&json!({
                "address": status.current_deposit_address,
                "ready": ready
            })))
        });

    health
        .or(user_status)
        .or(user_txs)
        .or(sync_route)
        .or(force_sync)
        .or(register)
        .or(user_address)
}
// Handler functions

async fn force_sync_handler(
    user_id: String,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    active_tasks_manager: Arc<UserTaskLock>,
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
    let _guard = ActivityGuard { user_id: &user_id, manager: &active_tasks_manager };

    let price_info = match tokio::time::timeout(Duration::from_secs(3), price_info.lock()).await {
        Ok(lock) => lock.clone(),
        Err(_) => return Err(PulserError::InternalError("Timeout acquiring price info".to_string()))?,
    };

    match resync_full_history(
        &user_id,
        &mut wallet.wallet,
        &esplora,
        chain,
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

async fn sync_user_handler(
    user_id: String,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price: Arc<Mutex<PriceInfo>>,
    esplora_urls: Arc<Mutex<Vec<(String, u32)>>>,
    esplora: Arc<bdk_esplora::esplora_client::AsyncClient>,
    state_manager: Arc<StateManager>,
    retry_queue: Arc<Mutex<VecDeque<WebhookRetry>>>,
    webhook_url: String,
    webhook_config: WebhookConfig,
    client: Client,
    active_tasks_manager: Arc<UserTaskLock>,
    price_feed: Arc<PriceFeed>,
    service_status: Arc<Mutex<ServiceStatus>>,
    config: Arc<Config>,
) -> Result<impl Reply, Rejection> {
    debug!("Received sync request for user {}", user_id);

    let is_active = match tokio::time::timeout(Duration::from_secs(5), active_tasks_manager.is_user_active(&user_id)).await {
        Ok(active) => active,
        Err(_) => return Err(PulserError::InternalError("Timeout checking user status".to_string()))?,
    };

    if is_active {
        debug!("User {} is already being synced", user_id);
        return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
    }

    match tokio::time::timeout(Duration::from_secs(5), active_tasks_manager.mark_user_active(&user_id, "sync")).await {
        Ok(result) => if !result {
            debug!("Failed to mark user {} as active", user_id);
            return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
        },
        Err(_) => return Err(PulserError::InternalError("Timeout acquiring lock".to_string()))?,
    };

    let _guard = ActivityGuard { user_id: &user_id, manager: &active_tasks_manager };

    {
        match tokio::time::timeout(Duration::from_secs(5), service_status.lock()).await {
            Ok(mut status) => status.active_syncs += 1,
            Err(_) => warn!("Timeout updating service status for user {}", user_id),
        }
    }

    let result = tokio::time::timeout(Duration::from_secs(120), sync_user(
        &user_id,
        wallets.clone(),
        user_statuses.clone(),
        price.clone(),
        price_feed.clone(),
        esplora_urls.clone(),
        &esplora,
        state_manager.clone(),
        retry_queue.clone(),
        &webhook_url,
        &webhook_config,
        client.clone(),
        config.clone(),
    )).await;

    {
        match tokio::time::timeout(Duration::from_secs(5), service_status.lock()).await {
            Ok(mut status) => {
                status.active_syncs = status.active_syncs.saturating_sub(1);
                if let Ok(wallets_lock) = tokio::time::timeout(Duration::from_secs(5), wallets.lock()).await {
                    status.total_utxos = wallets_lock.values().map(|(_, chain)| chain.utxos.len() as u32).sum();
                    status.total_value_btc = wallets_lock.values().map(|(wallet, _)| wallet.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0).sum();
                    status.total_value_usd = wallets_lock.values().map(|(_, chain)| chain.stabilized_usd.0).sum();
                }
            },
            Err(_) => warn!("Timeout updating service status after sync for user {}", user_id),
        }
    }

    match result {
        Ok(Ok(new_funds)) => {
            let message = if new_funds { "Sync completed with new funds" } else { "Sync completed" };
            Ok(warp::reply::json(&json!({"status": "ok", "message": message})))
        },
        Ok(Err(e)) => Err(e)?,
        Err(_) => Err(PulserError::InternalError("Sync timed out".to_string()))?,
    }
}

pub async fn sync_user(
    user_id: &str,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_info: Arc<Mutex<PriceInfo>>,
    price_feed: Arc<PriceFeed>,
    esplora_urls: Arc<Mutex<Vec<(String, u32)>>>,
    esplora: &bdk_esplora::esplora_client::AsyncClient,
    state_manager: Arc<StateManager>,
    retry_queue: Arc<Mutex<VecDeque<WebhookRetry>>>,
    webhook_url: &str,
    webhook_config: &WebhookConfig,
    client: Client,
    config: Arc<Config>,
) -> Result<bool, PulserError> {
    let start_time = Instant::now();
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    update_user_status(user_id, "syncing", "Starting sync", &user_statuses).await;

    let price_info_data = match tokio::time::timeout(Duration::from_secs(5), price_info.lock()).await {
        Ok(guard) => guard.clone(),
        Err(_) => {
            update_user_status(user_id, "error", "Price info timeout", &user_statuses).await;
            return Err(PulserError::InternalError("Price info timeout".to_string()));
        }
    };

    if price_info_data.raw_btc_usd <= 0.0 {
        update_user_status(user_id, "error", "Invalid price data", &user_statuses).await;
        return Err(PulserError::PriceFeedError("Invalid price data".to_string()));
    }

    let update_result = {
        let mut wallets_guard = match tokio::time::timeout(Duration::from_secs(10), wallets.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                update_user_status(user_id, "error", "Wallet lock timeout", &user_statuses).await;
                return Err(PulserError::InternalError("Wallet lock timeout".to_string()));
            }
        };

        if !wallets_guard.contains_key(user_id) {
            debug!("Initializing wallet for user {}", user_id);
            let (wallet, deposit_info, chain, _recovery_doc) = DepositWallet::from_config(&config, user_id, &state_manager, price_feed.clone()).await?;
            match tokio::time::timeout(Duration::from_secs(5), user_statuses.lock()).await {
                Ok(mut statuses) => {
                    let status_obj = statuses.entry(user_id.to_string()).or_insert_with(|| UserStatus::new(user_id));
                    status_obj.sync_status = "initializing".to_string();
                    status_obj.current_deposit_address = deposit_info.address.clone();
                },
                Err(_) => warn!("Timeout updating status with address for user {}", user_id),
            };
            wallets_guard.insert(user_id.to_string(), (wallet, chain));
            debug!("Wallet initialized for user {}", user_id);
        }

        if let Some((wallet, chain)) = wallets_guard.get_mut(user_id) {
            let mut spks = vec![Address::from_str(&chain.multisig_addr)?.assume_checked().script_pubkey()];
            for i in 0..=wallet.wallet.derivation_index(KeychainKind::External).unwrap_or(0) {
                spks.push(wallet.wallet.peek_address(KeychainKind::External, i).address.script_pubkey());
            }
            let request = SyncRequest::builder().spks(spks.into_iter()).build();
            wallet.blockchain.sync(request, 5).await?;

            match check_mempool_for_address(&client, &config.esplora_url, &chain.multisig_addr).await {
                Ok(mempool_txs) if !mempool_txs.is_empty() => {
                    wallet.wallet.apply_unconfirmed_txs(mempool_txs.iter().map(|tx| (Arc::new(tx.clone()), now as u64)));
                    debug!("Applied {} mempool transactions for user {}", mempool_txs.len(), user_id);
                },
                Ok(_) => {},
                Err(e) => warn!("Mempool check failed for user {}: {}", user_id, e),
            }

            let utxos = wallet.wallet.list_unspent().collect::<Vec<_>>();
            let has_unconfirmed = utxos.iter().any(|u| matches!(u.chain_position, ChainPosition::Unconfirmed { .. }));

            let deposit_addr = Address::from_str(&chain.multisig_addr)?.assume_checked();
            let change_addr = wallet.wallet.reveal_next_address(KeychainKind::Internal).address;
            let new_utxos = sync_and_stabilize_utxos(
                user_id,
                &mut wallet.wallet,
                &wallet.blockchain,
                chain,
                price_info_data.raw_btc_usd,
                &price_info_data,
                &deposit_addr,
                &change_addr,
                &state_manager,
                config.min_confirmations,
            ).await?;

            if let Some(changeset) = wallet.wallet.take_staged() {
                state_manager.save_changeset(user_id, &changeset).await?;
            }

            let new_funds_detected = !new_utxos.is_empty();
            let new_utxos_clone = new_utxos.clone();
            let chain_clone = chain.clone();
            let balance = wallet.wallet.balance();
            let confirmed_btc = balance.confirmed.to_sat() as f64 / 100_000_000.0;
            let unconfirmed_btc = balance.untrusted_pending.to_sat() as f64 / 100_000_000.0;
            let confirmations_pending = balance.untrusted_pending.to_sat() > 0;
            let utxo_count = chain.utxos.len() as u32;
            let stabilized_usd = chain.stabilized_usd.0;

            Ok((new_funds_detected, new_utxos_clone, chain_clone, confirmed_btc, unconfirmed_btc, confirmations_pending, utxo_count, stabilized_usd))
        } else {
            Err(PulserError::UserNotFound(user_id.to_string()))
        }
    };

    match update_result {
        Ok((new_funds_detected, new_utxos, chain, confirmed_btc, unconfirmed_btc, confirmations_pending, utxo_count, stabilized_usd)) => {
            if new_funds_detected && !webhook_url.is_empty() {
                spawn_webhook_notification(&client, user_id, &new_utxos, &chain, webhook_url, retry_queue.clone(), webhook_config).await;
            }
            update_user_status_full(
                user_id,
                "completed",
                utxo_count,
                confirmed_btc,
                stabilized_usd,
                confirmations_pending,
                if new_funds_detected { "Sync completed with new funds" } else { "Sync completed" },
                start_time.elapsed().as_millis() as u64,
                None,
                now,
                &user_statuses,
            ).await;
            if utxo_count > 0 {
                info!("User {} sync complete: {} UTXOs, {} BTC (${:.2})", user_id, utxo_count, confirmed_btc, stabilized_usd);
            }
            Ok(new_funds_detected)
        },
        Err(e) => {
            update_user_status_with_error(user_id, "error", &format!("Wallet update failed: {}", e), &user_statuses).await;
            Err(e)
        }
    }
}

// Helper functions

async fn spawn_webhook_notification(
    client: &Client,
    user_id: &str,
    new_utxos: &[UtxoInfo],
    chain: &StableChain,
    webhook_url: &str,
    retry_queue: Arc<Mutex<VecDeque<WebhookRetry>>>,
    webhook_config: &WebhookConfig,
) {
    let client = client.clone();
    let user_id = user_id.to_string();
    let new_utxos = new_utxos.to_vec();
    let chain = chain.clone();
    let webhook_url = webhook_url.to_string();
    let webhook_config = webhook_config.clone();
    
    tokio::spawn(async move {
        if let Err(e) = notify_new_utxos(&client, &user_id, &new_utxos, &chain, &webhook_url, retry_queue, &webhook_config).await {
            warn!("Webhook failed for user {}: {}. Queued for retry.", user_id, e);
        } else {
            debug!("Webhook sent for user {}: {} new UTXOs", user_id, new_utxos.len());
        }
    });
}

async fn update_user_status(user_id: &str, status: &str, message: &str, user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>) {
    match tokio::time::timeout(Duration::from_secs(5), user_statuses.lock()).await {
        Ok(mut statuses) => {
            if let Some(status_obj) = statuses.get_mut(user_id) {
                status_obj.sync_status = status.to_string();
                status_obj.last_update_message = message.to_string();
                if status == "error" {
                    status_obj.last_error = Some(message.to_string());
                }
            }
        },
        Err(_) => warn!("Timeout updating status for user {}", user_id),
    }
}

async fn update_user_status_with_address(user_id: &str, status: &str, address: &str, user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>) {
    match tokio::time::timeout(Duration::from_secs(5), user_statuses.lock()).await {
        Ok(mut statuses) => {
            let status_obj = statuses.entry(user_id.to_string()).or_insert_with(|| UserStatus::new(user_id));
            status_obj.sync_status = status.to_string();
            status_obj.current_deposit_address = address.to_string();
        },
        Err(_) => warn!("Timeout updating status with address for user {}", user_id),
    }
}

async fn update_user_status_with_error(user_id: &str, status: &str, error: &str, user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>) {
    match tokio::time::timeout(Duration::from_secs(5), user_statuses.lock()).await {
        Ok(mut statuses) => {
            if let Some(status_obj) = statuses.get_mut(user_id) {
                status_obj.sync_status = status.to_string();
                status_obj.last_update_message = format!("Error: {}", error);
                status_obj.last_error = Some(error.to_string());
            }
        },
        Err(_) => warn!("Timeout updating error status for user {}", user_id),
    }
}

async fn update_user_status_full(
    user_id: &str,
    sync_status: &str,
    utxo_count: u32,
    total_value_btc: f64,
    total_value_usd: f64,
    confirmations_pending: bool,
    update_message: &str,
    sync_duration_ms: u64,
    error: Option<String>,
    last_success: u64,
    user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>,
) {
    match tokio::time::timeout(Duration::from_secs(5), user_statuses.lock()).await {
        Ok(mut statuses) => {
            if let Some(status) = statuses.get_mut(user_id) {
                status.sync_status = sync_status.to_string();
                status.utxo_count = utxo_count;
                status.total_value_btc = total_value_btc;
                status.total_value_usd = total_value_usd;
                status.confirmations_pending = confirmations_pending;
                status.last_update_message = update_message.to_string();
                status.sync_duration_ms = sync_duration_ms;
                status.last_error = error;
                status.last_success = last_success;
            }
        },
        Err(_) => warn!("Timeout updating full status for user {}", user_id),
    }
}

// Filter helpers

fn with_wallets(wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>) -> impl Filter<Extract = (Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || wallets.clone())
}

fn with_statuses(statuses: Arc<Mutex<HashMap<String, UserStatus>>>) -> impl Filter<Extract = (Arc<Mutex<HashMap<String, UserStatus>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || statuses.clone())
}

fn with_client(client: Client) -> impl Filter<Extract = (Client,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || client.clone())
}

fn with_esplora_urls(urls: Arc<Mutex<Vec<(String, u32)>>>) -> impl Filter<Extract = (Arc<Mutex<Vec<(String, u32)>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || urls.clone())
}

fn with_state_manager(manager: Arc<StateManager>) -> impl Filter<Extract = (Arc<StateManager>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || manager.clone())
}

fn with_retry_queue(queue: Arc<Mutex<VecDeque<WebhookRetry>>>) -> impl Filter<Extract = (Arc<Mutex<VecDeque<WebhookRetry>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || queue.clone())
}

fn with_active_tasks_manager(manager: Arc<UserTaskLock>) -> impl Filter<Extract = (Arc<UserTaskLock>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || manager.clone())
}

fn with_service_status(status: Arc<Mutex<ServiceStatus>>) -> impl Filter<Extract = (Arc<Mutex<ServiceStatus>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || status.clone())
}

fn with_price(price: Arc<Mutex<PriceInfo>>) -> impl Filter<Extract = (Arc<Mutex<PriceInfo>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || price.clone())
}

fn with_price_feed(price_feed: Arc<PriceFeed>) -> impl Filter<Extract = (Arc<PriceFeed>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || price_feed.clone())
}

fn with_config(config: Arc<Config>) -> impl Filter<Extract = (Arc<Config>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || config.clone())
}
