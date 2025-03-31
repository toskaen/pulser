// deposit-service/src/api.rs
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
use common::error::PulserError;
use common::types::PriceInfo;
use crate::wallet::DepositWallet;
use common::{ServiceStatus, UserStatus, StableChain, WebhookRetry};
use crate::webhook::{WebhookConfig, notify_new_utxos};
use tokio::time::sleep;
use crate::wallet_init;
use common::task_manager::UserTaskLock; // Updated to match your task_manager.rs
use common::price_feed::PriceFeed;
use common::StateManager;
use common::wallet_sync;
use serde::Serialize;
use common::types::UtxoInfo;
use common::wallet_sync::resync_full_history; // Add this
use futures_util::FutureExt; // Add this to imports
use std::pin::Pin; // Add this to imports
use warp::reply::Json;
use crate::config::Config; // Add this
use crate::wallet_init::init_wallet;



#[derive(Serialize)]
struct TxInfo {
    txid: String,
    confirmation_time: Option<u32>, // Block height if confirmed
    amount: u64,                    // Total output value in satoshis
    is_spent: bool,                 // Are outputs spent?
    timestamp: Option<u64>,         // Unix timestamp of confirmation
}

#[derive(Debug)]
enum CustomError {
    Io(std::io::Error),
    Config(String),
    Pulser(PulserError),
    HttpError(reqwest::Error),
    AddressError(bitcoin::address::ParseError),
    NetworkError(bitcoin::network::ParseNetworkError),
}

// Near the top of api.rs, after imports
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

impl warp::reject::Reject for CustomError {}

fn to_rejection<E>(err: E) -> warp::Rejection where E: Into<CustomError> {
    warp::reject::custom(err.into())
}

impl From<std::io::Error> for CustomError { fn from(err: std::io::Error) -> Self { CustomError::Io(err) } }
impl From<toml::de::Error> for CustomError { fn from(err: toml::de::Error) -> Self { CustomError::Config(err.to_string()) } }
impl From<reqwest::Error> for CustomError { fn from(err: reqwest::Error) -> Self { CustomError::HttpError(err) } }
impl From<bitcoin::address::ParseError> for CustomError { fn from(err: bitcoin::address::ParseError) -> Self { CustomError::AddressError(err) } }
impl From<bitcoin::network::ParseNetworkError> for CustomError { fn from(err: bitcoin::network::ParseNetworkError) -> Self { CustomError::NetworkError(err) } }

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
    active_tasks_manager: Arc<UserTaskLock>, // Updated to UserTaskLock
    price_feed: Arc<PriceFeed>,
    esplora: Arc<bdk_esplora::esplora_client::AsyncClient>,
        config: Arc<Config>, // Add this

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
            
            // Get status with timeout protection
            let status = match tokio::time::timeout(Duration::from_secs(3), service_status.lock()).await {
                Ok(status) => status,
                Err(_) => return Ok::<_, Rejection>(warp::reply::json(&json!({
                    "status": "ERROR", 
                    "message": "Timeout acquiring service status lock"
                })))
            };
            
            // Get esplora status with timeout protection
            let esplora_urls_lock = match tokio::time::timeout(Duration::from_secs(3), esplora_urls.lock()).await {
                Ok(lock) => lock,
                Err(_) => return Ok::<_, Rejection>(warp::reply::json(&json!({
                    "status": "ERROR", 
                    "message": "Timeout acquiring esplora URLs lock"
                })))
            };
            
            // Get price info with timeout protection
            let price_info_lock = match tokio::time::timeout(Duration::from_secs(3), price_info.lock()).await {
                Ok(lock) => lock,
                Err(_) => return Ok::<_, Rejection>(warp::reply::json(&json!({
                    "status": "ERROR", 
                    "message": "Timeout acquiring price info lock"
                })))
            };
            
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0)).as_secs();
            let websocket_active = status.websocket_active;
            
            // Enhanced Esplora health check
            let mut esplora_status = HashMap::new();
            let mut esplora_details = HashMap::new();
            let mut any_healthy_esplora = false;
            
            for (url, errors) in esplora_urls_lock.iter() {
                let is_healthy = *errors < 10;
                esplora_status.insert(url.clone(), is_healthy);
                
                // Add more details about each endpoint
                esplora_details.insert(url.clone(), json!({
                    "healthy": is_healthy,
                    "error_count": errors,
                    "last_used": now - status.last_update, // Approximate
                    "primary": url == &esplora_urls_lock[0].0
                }));
                
                if is_healthy {
                    any_healthy_esplora = true;
                }
            }
            
            // Get active tasks
            let active_tasks = active_tasks_manager.get_active_tasks().await;
            let task_counts = active_tasks_manager.get_task_counts().await;
            
            // Compute price feed staleness
            let price_staleness_secs = now - price_info_lock.timestamp as u64;
            let price_healthy = price_staleness_secs < 120 && price_info_lock.raw_btc_usd > 1000.0;
            
            // Determine overall health 
            let overall_status = if websocket_active && price_healthy && any_healthy_esplora {
                "healthy"
            } else if !any_healthy_esplora {
                "critical" // No healthy Esplora endpoints is critical
            } else if !websocket_active || !price_healthy {
                "degraded" // Issues with WebSocket or price feed
            } else {
                "warning" // Other minor issues
            };
            
            // Action recommendations
            let websocket_action = if websocket_active { 
                "WebSocket operational" 
            } else { 
                "Action: Check WebSocket connection to Deribit" 
            };
            
            let price_action = if price_healthy { 
                "Price feed current" 
            } else if price_info_lock.raw_btc_usd <= 1000.0 { 
                "Action: Invalid price detected, check price sources" 
            } else { 
                "Action: Price feed stale, check Deribit connectivity" 
            };
            
            let esplora_action = if any_healthy_esplora { 
                "At least one Esplora endpoint operational" 
            } else { 
                "CRITICAL: All Esplora endpoints failing, service severely degraded" 
            };
            
            // Build the response
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
                // Lock wallets with timeout
                let wallets_lock = match tokio::time::timeout(Duration::from_secs(5), wallets.lock()).await {
                    Ok(lock) => Ok(lock),
                    Err(_) => Err(warp::reject::custom(CustomError::Pulser(PulserError::InternalError(
                        "Timeout acquiring wallets lock".to_string()
                    )))),
                }?;
                
                // Lock statuses with timeout
                let statuses_lock = match tokio::time::timeout(Duration::from_secs(5), statuses.lock()).await {
                    Ok(lock) => Ok(lock),
                    Err(_) => Err(warp::reject::custom(CustomError::Pulser(PulserError::InternalError(
                        "Timeout acquiring statuses lock".to_string()
                    )))),
                }?;
                
                // Use the locks
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
// In api.rs - user_txs endpoint
let user_txs = warp::path("user")
    .and(warp::path::param::<String>())
    .and(warp::path("txs"))
    .and(warp::get())
    .and(with_wallets(wallets.clone()))
    .and_then({
        move |user_id, 
              wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>| async move {
            let wallets_lock = match tokio::time::timeout(
                Duration::from_secs(5),
                wallets.lock()
            ).await {
                Ok(lock) => lock,
                Err(_) => {
                    warn!("Timeout acquiring wallets lock for user {} transactions", user_id);
                    return Ok::<_, Rejection>(warp::reply::json(&json!({
                        "error": "Service busy, try again"
                    })));
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
        let esplora_clone = Arc::clone(&esplora_clone); // Clone for this route
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
    .and(with_config(config.clone())) // Make sure we include config
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

// Define the handler function separately to avoid closure issues
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

    // Get price_feed from a dependency injection mechanism or create a new one
    let price_feed = Arc::new(PriceFeed::new());

    // Check user existence
    let mut wallets_lock = match tokio::time::timeout(Duration::from_secs(3), wallets.lock()).await {
        Ok(lock) => lock,
        Err(_) => {
            warn!("Timeout checking if user {} exists", &user_id);
            return Ok(warp::reply::json(&json!({"status": "error", "message": "Timeout checking if user exists"})));
        }
    };
    
    // If user doesn't exist, try to initialize with config
    if !wallets_lock.contains_key(&user_id) {
        debug!("User {} not found, attempting to initialize wallet", &user_id);
        match DepositWallet::from_config(&config, &user_id, &state_manager, price_feed.clone()).await {
            Ok((wallet, deposit_info, chain)) => {
                debug!("Initialized wallet for user {}", &user_id);
                wallets_lock.insert(user_id.clone(), (wallet, chain));
            }
            Err(e) => {
                error!("Failed to initialize wallet for user {}: {}", &user_id, e);
                return Ok(warp::reply::json(&json!({"status": "error", "message": format!("Failed to initialize wallet: {}", e)})));
            }
        }
    }
    
    let (wallet, chain) = match wallets_lock.get_mut(&user_id) {
        Some(entry) => entry,
        None => return Ok(warp::reply::json(&json!({"status": "error", "message": "User not found"})))
    };

    // Check if user is already syncing
    if active_tasks_manager.is_user_active(&user_id).await {
        debug!("User {} already being synced", &user_id);
        return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
    }

    // Mark user active
    if !active_tasks_manager.mark_user_active(&user_id, "force_sync").await {
        return Ok(warp::reply::json(&json!({"status": "error", "message": "Failed to lock user for sync"})));
    }
    let _guard = ActivityGuard { user_id: &user_id, manager: &active_tasks_manager };

    // Get price info
    let price_info = match tokio::time::timeout(Duration::from_secs(3), price_info.lock()).await {
        Ok(lock) => lock.clone(),
        Err(_) => {
            warn!("Timeout acquiring price info for user {}", &user_id);
            return Ok(warp::reply::json(&json!({"status": "error", "message": "Timeout acquiring price info"})));
        }
    };

    // Execute full resync
    match resync_full_history(
        &user_id,
        &mut wallet.wallet,
        &esplora,
        chain,
        &price_info,
        &state_manager,
        config.min_confirmations, // Use config value instead of hardcoded 1
    ).await {
        Ok(new_utxos) => {
            info!("Full resync completed for user {}: {} new UTXOs", &user_id, new_utxos.len());
            Ok(warp::reply::json(&json!({"status": "ok", "message": format!("Full resync completed, {} new UTXOs", new_utxos.len())})))
        },
        Err(e) => {
            error!("Full resync failed for user {}: {}", &user_id, e);
            Ok(warp::reply::json(&json!({"status": "error", "message": format!("Full resync failed: {}", e)})))
        }
    }
}

// Add this helper function for the price_feed filter
fn with_price_feed(
    price_feed: Arc<PriceFeed>
) -> impl Filter<Extract = (Arc<PriceFeed>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || price_feed.clone())
}
    
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
                // Validate user_id from request
                let user_id = match public_data["user_id"].as_str() {
                    Some(id) if !id.is_empty() && id.chars().all(|c| c.is_ascii_digit()) => id.to_string(),
                    _ => return Ok::<_, Rejection>(warp::reply::json(&json!({
                        "status": "error",
                        "message": "Invalid or missing user_id"
                    })))
                };
                
                // Create directory structure
                let user_dir = PathBuf::from(format!("user_{}", user_id));
                let public_path = user_dir.join(format!("user_{}_public.json", user_id));
                
                // Lock wallets early to check existence
                let mut wallets_lock = match tokio::time::timeout(Duration::from_secs(5), wallets.lock()).await {
                    Ok(lock) => lock,
                    Err(_) => {
                        warn!("Timeout acquiring wallets lock for user {} registration", user_id);
                        return Ok::<_, Rejection>(warp::reply::json(&json!({
                            "status": "registered",
                            "message": "User registered but wallet initialization delayed"
                        })));
                    }
                };
                
                // If user doesn’t exist, initialize wallet and update public data
                if !wallets_lock.contains_key(&user_id) {
                    match DepositWallet::from_config(&config, &user_id, &state_manager, price_feed.clone()).await {
                        Ok((wallet, deposit_info, chain)) => {
                            // Use the public_data from init_wallet (via DepositWallet::from_config)
                            let init_result = init_wallet(&config, &user_id)?;
                            let full_public_data = init_result.public_data;
                            
                            // Save the full public data (includes descriptors)
                            match tokio::time::timeout(Duration::from_secs(5), state_manager.save(&public_path, &full_public_data)).await {
                                Ok(Ok(_)) => info!("Public data with descriptors saved to {}", public_path.display()),
                                Ok(Err(e)) => {
                                    error!("Failed to save user {} public data: {}", user_id, e);
                                    return Ok::<_, Rejection>(warp::reply::json(&json!({
                                        "status": "error",
                                        "message": format!("Failed to save user data: {}", e)
                                    })));
                                },
                                Err(_) => {
                                    error!("Timeout saving user {} public data", user_id);
                                    return Ok::<_, Rejection>(warp::reply::json(&json!({
                                        "status": "error",
                                        "message": "Timeout saving user data"
                                    })));
                                }
                            }
                            
                            // Update user_statuses
                            let mut statuses = match tokio::time::timeout(Duration::from_secs(5), user_statuses.lock()).await {
                                Ok(lock) => lock,
                                Err(_) => {
                                    wallets_lock.insert(user_id.clone(), (wallet, chain));
                                    return Ok::<_, Rejection>(warp::reply::json(&json!({
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
                                current_deposit_address: deposit_info.address,
                                last_deposit_time: None,
                            });
                            wallets_lock.insert(user_id.clone(), (wallet, chain));
                            
                            // Update service status
                            match tokio::time::timeout(Duration::from_secs(5), service_status.lock()).await {
                                Ok(mut status) => status.users_monitored = wallets_lock.len() as u32,
                                Err(_) => warn!("Timeout updating service status for new user {}", user_id)
                            }
                            
                            info!("Registered user {} with descriptors", user_id);
                        }
                        Err(e) => {
                            warn!("Failed to init wallet for {}: {}", user_id, e);
                            return Ok::<_, Rejection>(warp::reply::json(&json!({
                                "status": "error",
                                "message": format!("Failed to initialize wallet: {}", e)
                            })));
                        }
                    }
                } else {
                    debug!("User {} already registered, updating public data", user_id);
                    // If user exists, still ensure public data is updated if needed
                    let init_result = init_wallet(&config, &user_id)?;
                    if !public_path.exists() {
                        match tokio::time::timeout(Duration::from_secs(5), state_manager.save(&public_path, &init_result.public_data)).await {
                            Ok(Ok(_)) => info!("Updated public data for existing user {}", user_id),
                            Ok(Err(e)) => error!("Failed to update public data for {}: {}", user_id, e),
                            Err(_) => error!("Timeout updating public data for {}", user_id),
                        }
                    }
                }
                
                Ok::<_, Rejection>(warp::reply::json(&json!({"status": "registered"})))
            }
        }
    });

// Add helper filter function for config
fn with_config(config: Arc<Config>) -> impl Filter<Extract = (Arc<Config>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || config.clone())
}

let user_address = warp::path!("user" / String / "address")
    .and(warp::get())
    .and(with_statuses(user_statuses.clone()))
    .and_then(move |user_id: String, user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>| async move {
        let statuses_lock = match tokio::time::timeout(Duration::from_secs(3), user_statuses.lock()).await {
            Ok(lock) => lock,
            Err(_) => {
                warn!("Timeout acquiring user_statuses lock for address request");
                return Ok::<_, Rejection>(warp::reply::json(&json!({
                    "status": "error",
                    "message": "Service busy, try again"
                })));
            }
        };
        
        match statuses_lock.get(&user_id) {
            Some(status) => {
                // Return only address-related info
                Ok::<_, Rejection>(warp::reply::json(&json!({
                    "deposit_address": status.current_deposit_address,
                    "ready": status.sync_status != "error" && status.sync_status != "initializing"
                })))
            },
            None => Ok::<_, Rejection>(warp::reply::json(&json!({"error": "User not found"}))),
        }
    });
    
        let user_change_log = warp::path!("user" / String / "changes")
        .and(warp::get())
        .and(with_wallets(wallets.clone()))
        .and_then(move |user_id: String, wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>| async move {
            // Add timeout protection
            let wallets_lock = match tokio::time::timeout(
                Duration::from_secs(5),
                wallets.lock()
            ).await {
                Ok(lock) => lock,
                Err(_) => return Ok::<_, Rejection>(warp::reply::json(&json!({
                    "error": "Timeout acquiring wallets lock"
                })))
            };
            
            match wallets_lock.get(&user_id) {
                Some((_, chain)) => {
                    Ok::<_, Rejection>(warp::reply::json(&chain.change_log))
                },
                None => Ok::<_, Rejection>(warp::reply::json(&json!({
                    "error": "User not found"
                }))),
            }
        });

health.or(user_status).or(user_txs).or(sync_route)
    .or(force_sync).or(register).or(user_address).or(user_change_log)
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
        config: Arc<Config>, // Add this

) -> Result<impl Reply, Rejection> {
    debug!("Received sync request for user {}", user_id);

    // Check if user is already being processed - with timeout
    let is_active = match tokio::time::timeout(
        Duration::from_secs(5),
        active_tasks_manager.is_user_active(&user_id)
    ).await {
        Ok(active) => active,
        Err(_) => {
            warn!("Timeout checking if user {} is active", user_id);
            return Ok(warp::reply::json(&json!({"status": "error", "message": "Timeout checking user status"})));
        }
    };

    if is_active {
        debug!("User {} is already being synced", user_id);
        return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
    }

    // Mark user as active with timeout
    match tokio::time::timeout(
        Duration::from_secs(5),
        active_tasks_manager.mark_user_active(&user_id, "sync")
    ).await {
        Ok(result) => {
            if !result {
                debug!("Failed to mark user {} as active", user_id);
                return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
            }
        },
        Err(_) => {
            warn!("Timeout marking user {} as active", user_id);
            return Ok(warp::reply::json(&json!({"status": "error", "message": "Timeout acquiring lock"})));
        }
    };

    let _guard = ActivityGuard {
        user_id: &user_id,
        manager: &active_tasks_manager,
    };

    // Update service status to show active sync - with timeout
    {
        match tokio::time::timeout(Duration::from_secs(5), service_status.lock()).await {
            Ok(mut status) => status.active_syncs += 1,
            Err(_) => warn!("Timeout updating service status for user {}", user_id),
        }
    }

    // Execute the sync with timeout
    let result = tokio::time::timeout(
        Duration::from_secs(120), // 2 minute overall timeout
        sync_user(
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
                config.clone(), // Add this

        )
    ).await;

    // Update service status after sync - with timeout
    {
        match tokio::time::timeout(Duration::from_secs(5), service_status.lock()).await {
            Ok(mut status) => {
                status.active_syncs = status.active_syncs.saturating_sub(1);
                
                // Update total counts for service status - with timeout
                if let Ok(wallets_lock) = tokio::time::timeout(
                    Duration::from_secs(5), 
                    wallets.lock()
                ).await {
                    status.total_utxos = wallets_lock.values()
                        .map(|(_, chain)| chain.utxos.len() as u32)
                        .sum();
                    status.total_value_btc = wallets_lock.values()
                        .map(|(wallet, _)| wallet.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0)
                        .sum();
                    status.total_value_usd = wallets_lock.values()
                        .map(|(_, chain)| chain.stabilized_usd.0)
                        .sum();
                }
            },
            Err(_) => warn!("Timeout updating service status after sync for user {}", user_id),
        }
    }

    // Return appropriate response based on sync result
    match result {
        Ok(Ok(new_funds)) => {
            let message = if new_funds {
                "Sync completed with new funds".to_string()
            } else {
                "Sync completed".to_string()
            };
            Ok(warp::reply::json(&json!({"status": "ok", "message": message})))
        },
        Ok(Err(e)) => {
            error!("Sync error for user {}: {}", user_id, e);
            Ok(warp::reply::json(&json!({"status": "error", "message": format!("Sync failed: {}", e)})))
        },
        Err(_) => {
            error!("Sync timeout for user {}", user_id);
            Ok(warp::reply::json(&json!({"status": "error", "message": "Sync timed out"})))
        },
    }
}

// And also fix the sync_user method to correctly use config:
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
    config: Arc<Config>, // Add this parameter
) -> Result<bool, PulserError> {
    let start_time = Instant::now();
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    // Update user status to "syncing"
    update_user_status(user_id, "syncing", "Starting sync", &user_statuses).await;

    // Get price info (with timeout)
    let price_info_data = match tokio::time::timeout(Duration::from_secs(5), price_info.lock()).await {
        Ok(guard) => guard.clone(),
        Err(_) => {
            warn!("Timeout accessing price info for user {}", user_id);
            update_user_status(user_id, "error", "Price info timeout", &user_statuses).await;
            return Err(PulserError::InternalError("Price info timeout".to_string()));
        }
    };

    // Check if price data is valid
    if price_info_data.raw_btc_usd <= 0.0 {
        error!("Invalid price data for user {}: ${}", user_id, price_info_data.raw_btc_usd);
        update_user_status(user_id, "error", "Invalid price data", &user_statuses).await;
        return Err(PulserError::PriceFeedError("Invalid price data".to_string()));
    }

    // Execute wallet update with the wallets lock
    let update_result = {
        let mut wallets_guard = match tokio::time::timeout(Duration::from_secs(10), wallets.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                warn!("Timeout acquiring wallet lock for user {}", user_id);
                update_user_status(user_id, "error", "Wallet lock timeout", &user_statuses).await;
                return Err(PulserError::InternalError("Wallet lock timeout".to_string()));
            }
        };

        // Initialize wallet if it doesn't exist
        if !wallets_guard.contains_key(user_id) {
            debug!("Initializing wallet for user {}", user_id);
            match DepositWallet::from_config(&config, user_id, &state_manager, price_feed.clone()).await {
                Ok((wallet, deposit_info, chain)) => {
                    // Update user status with address (with timeout)
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
                },
                Err(e) => {
                    error!("Failed to initialize wallet for user {}: {}", user_id, e);
                    update_user_status(
                        user_id, "error", 
                        &format!("Wallet initialization failed: {}", e), 
                        &user_statuses
                    ).await;
                    return Err(e);
                }
            }
        }

        // Get wallet and chain
        if let Some((wallet, chain)) = wallets_guard.get_mut(user_id) {
            // Perform the wallet update inside the lock
            match wallet.update_stable_chain(&price_info_data).await {
                Ok(new_utxos) => {
                    let new_funds_detected = !new_utxos.is_empty();
                    
                    // For webhook notification, clone what we need
                    let new_utxos_clone = new_utxos.clone();
                    let chain_clone = chain.clone();

                    // Calculate balance information
                    let balance = wallet.wallet.balance();
                    let confirmed_btc = balance.confirmed.to_sat() as f64 / 100_000_000.0;
                    let unconfirmed_btc = balance.untrusted_pending.to_sat() as f64 / 100_000_000.0;
                    let confirmations_pending = balance.untrusted_pending.to_sat() > 0;
                    let utxo_count = chain.utxos.len() as u32;
                    let stabilized_usd = chain.stabilized_usd.0;

                    // Return the cloned data and result info for processing outside the lock
                    Ok((new_funds_detected, new_utxos_clone, chain_clone, confirmed_btc, unconfirmed_btc, confirmations_pending, utxo_count, stabilized_usd))
                },
                Err(e) => Err(e),
            }
        } else {
            Err(PulserError::UserNotFound(user_id.to_string()))
        }
    };

    // Process the update result outside the lock
    match update_result {
        Ok((new_funds_detected, new_utxos, chain, confirmed_btc, unconfirmed_btc, confirmations_pending, utxo_count, stabilized_usd)) => {
            // Notify via webhook if needed
            if new_funds_detected && !webhook_url.is_empty() {
                spawn_webhook_notification(
                    &client, user_id, &new_utxos, &chain, 
                    webhook_url, retry_queue.clone(), webhook_config
                ).await;
            }
            
            // Update user status with success (with timeout)
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
                info!("User {} sync complete: {} UTXOs, {} BTC (${:.2})", 
                     user_id, utxo_count, confirmed_btc, stabilized_usd);
            }
            
            Ok(new_funds_detected)
        },
        Err(e) => {
            error!("Failed to update wallet for user {}: {}", user_id, e);
            
            // Update user status with error (with timeout)
            update_user_status_with_error(
                user_id, "error", &format!("Wallet update failed: {}", e), 
                &user_statuses
            ).await;
            
            Err(e)
        }
    }
}

// Helper function to spawn webhook notification without blocking
async fn spawn_webhook_notification(
    client: &Client,
    user_id: &str,
    new_utxos: &[UtxoInfo],
    chain: &StableChain,  // Chain is already cloneable
    webhook_url: &str,
    retry_queue: Arc<Mutex<VecDeque<WebhookRetry>>>,
    webhook_config: &WebhookConfig,
) {
    let client = client.clone();
    let user_id = user_id.to_string();
    let new_utxos = new_utxos.to_vec();
    let chain = chain.clone();  // This works now because StableChain implements Clone
    let webhook_url = webhook_url.to_string();
    let webhook_config = webhook_config.clone();
    
    tokio::spawn(async move {
        if let Err(e) = notify_new_utxos(
            &client, &user_id, &new_utxos, &chain, 
            &webhook_url, retry_queue, &webhook_config
        ).await {
            warn!("Webhook failed for user {}: {}. Queued for retry.", user_id, e);
        } else {
            debug!("Webhook sent for user {}: {} new UTXOs", user_id, new_utxos.len());
        }
    });
}

// Helper function to update user status with timeout
async fn update_user_status(
    user_id: &str,
    status: &str,
    message: &str,
    user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>,
) {
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

// Helper function to update user status with address
async fn update_user_status_with_address(
    user_id: &str,
    status: &str,
    address: &str,
    user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>,
) {
    match tokio::time::timeout(Duration::from_secs(5), user_statuses.lock()).await {
        Ok(mut statuses) => {
            let status_obj = statuses.entry(user_id.to_string()).or_insert_with(|| UserStatus::new(user_id));
            status_obj.sync_status = status.to_string();
            status_obj.current_deposit_address = address.to_string();
        },
        Err(_) => warn!("Timeout updating status with address for user {}", user_id),
    }
}

// Helper function to update user status with error
async fn update_user_status_with_error(
    user_id: &str,
    status: &str,
    error: &str,
    user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>,
) {
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

// Helper function for complete status update
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
                
                // Keep existing values for these fields
                // status.current_deposit_address - unchanged
                // status.last_deposit_time - unchanged
            }
        },
        Err(_) => warn!("Timeout updating full status for user {}", user_id),
    }
}

// Additional API routes improvements

fn improved_health_route(
    service_status: Arc<Mutex<ServiceStatus>>,
    esplora_urls: Arc<Mutex<Vec<(String, u32)>>>,
    price_info: Arc<Mutex<PriceInfo>>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path("health")
        .and(warp::get())
        .and_then(move || {
            let service_status = service_status.clone();
            let esplora_urls = esplora_urls.clone();
            let price_info = price_info.clone();
            
            async move {
                // Use a timeout for each lock to prevent cascading deadlocks
                let status = match tokio::time::timeout(Duration::from_secs(3), service_status.lock()).await {
                    Ok(status) => status.clone(),
                    Err(_) => {
                        warn!("Timeout acquiring service status lock for health check");
                        return Ok::<_, Rejection>(warp::reply::json(&json!({
                            "status": "ERROR",
                            "message": "Timeout acquiring service status"
                        })));
                    }
                };
                
                let esplora_status = match tokio::time::timeout(Duration::from_secs(3), esplora_urls.lock()).await {
                    Ok(urls) => {
                        urls.iter()
                           .map(|(url, errors)| (url.clone(), *errors < 10))
                           .collect::<HashMap<String, bool>>()
                    },
                    Err(_) => {
                        warn!("Timeout acquiring esplora urls lock for health check");
                        HashMap::new()
                    }
                };
                
                let price_data = match tokio::time::timeout(Duration::from_secs(3), price_info.lock()).await {
                    Ok(price) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let staleness_secs = now as i64 - price.timestamp;
                        (price.raw_btc_usd, staleness_secs as u64)
                    },
                    Err(_) => {
                        warn!("Timeout acquiring price info lock for health check");
                        (0.0, 999999)
                    }
                };
                
                let (price, staleness_secs) = price_data;
                let price_action = if staleness_secs < 60 {
                    "Price feed current" 
                } else { 
                    "Action: Check Deribit connectivity"
                };
                
                let esplora_action = if esplora_status.values().all(|&healthy| healthy) {
                    "Esplora endpoints operational"
                } else {
                    "Action: Check Esplora URLs"
                };
                
                let websocket_action = if status.websocket_active {
                    "WebSocket operational"
                } else {
                    "Action: Check WebSocket connection"
                };
                
                Ok::<_, Rejection>(warp::reply::json(&json!({
                    "status": "OK",
                    "health": status.health,
                    "active_syncs": status.active_syncs,
                    "websocket": {
                        "active": status.websocket_active,
                        "action": websocket_action
                    },
                    "price": {
                        "staleness_secs": staleness_secs,
                        "last_price": price,
                        "action": price_action
                    },
                    "esplora": {
                        "endpoints": esplora_status,
                        "action": esplora_action
                    }
                })))
            }
        })
}

fn improved_user_status_route(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path("user")
        .and(warp::path::param::<String>())
        .and(warp::get())
        .and_then(move |user_id: String| {
            let wallets = wallets.clone();
            let statuses = user_statuses.clone();
            
            async move {
                // First lock both mutexes with timeouts
                let wallets_lock = match tokio::time::timeout(
                    Duration::from_secs(5),
                    wallets.lock()
                ).await {
                    Ok(lock) => lock,
                    Err(_) => return Ok::<_, Rejection>(warp::reply::json(&json!({
                        "error": "Timeout acquiring wallets lock"
                    })))
                };
                
                let statuses_lock = match tokio::time::timeout(
                    Duration::from_secs(5),
                    statuses.lock()
                ).await {
                    Ok(lock) => lock,
                    Err(_) => return Ok::<_, Rejection>(warp::reply::json(&json!({
                        "error": "Timeout acquiring statuses lock"
                    })))
                };
                
                // Now use the locks to access the data
                match (statuses_lock.get(&user_id), wallets_lock.get(&user_id)) {
                    (Some(status), Some((wallet, chain))) => {
                        let balance = wallet.wallet.balance();
                        Ok::<_, Rejection>(warp::reply::json(&json!({
                            "user_id": user_id,
                            "status": {
                                "sync_status": status.sync_status,
                                "last_sync": status.last_sync,
                                "utxo_count": status.utxo_count,
                                "total_value_btc": status.total_value_btc,
                                "total_value_usd": status.total_value_usd,
                                "confirmations_pending": status.confirmations_pending,
                                "last_update_message": status.last_update_message,
                                "current_deposit_address": status.current_deposit_address,
                                "last_deposit_time": status.last_deposit_time,
                                "sync_duration_ms": status.sync_duration_ms,
                                "last_error": status.last_error,
                                "last_success": status.last_success
                            },
                            "balance": {
                                "confirmed_btc": balance.confirmed.to_sat() as f64 / 100_000_000.0,
                                "unconfirmed_btc": balance.untrusted_pending.to_sat() as f64 / 100_000_000.0,
                                "immature_btc": balance.immature.to_sat() as f64 / 100_000_000.0,
                                "stabilized_usd": chain.stabilized_usd.0
                            },
                            "chain": {
                                "multisig_addr": chain.multisig_addr,
                                "utxo_count": chain.utxos.len(),
                                "raw_btc_usd": chain.raw_btc_usd,
                                "hedge_ready": chain.hedge_ready,
                                "hedge_position_id": chain.hedge_position_id
                            }
                        })))
                    }
                    _ => Ok::<_, Rejection>(warp::reply::json(&json!({"error": "User not found"}))),
                }
            }
        })
}

// Improved route for force sync with better concurrency control
fn improved_force_sync_route(
    sync_tx: mpsc::Sender<String>,
    active_tasks_manager: Arc<UserTaskLock>,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path!("force_sync" / String)
        .and(warp::post())
        .and_then(move |user_id: String| {
            let tx = sync_tx.clone();
            let active_tasks_manager = active_tasks_manager.clone();
            let wallets = wallets.clone();
            async move {
                let user_exists = match tokio::time::timeout(Duration::from_secs(3), wallets.lock()).await {
                    Ok(wallets_lock) => wallets_lock.contains_key(&user_id),
                    Err(_) => {
                        warn!("Timeout checking if user {} exists", user_id);
                        return Ok::<_, Rejection>(warp::reply::json(&json!({"status": "error", "message": "Timeout checking if user exists"})));
                    }
                };
                if !user_exists {
                    return Ok::<_, Rejection>(warp::reply::json(&json!({"status": "error", "message": "User not found"})));
                }
                let is_active = match tokio::time::timeout(Duration::from_secs(3), active_tasks_manager.is_user_active(&user_id)).await {
                    Ok(active) => active,
                    Err(_) => {
                        warn!("Timeout checking if user {} is active", user_id);
                        return Ok::<_, Rejection>(warp::reply::json(&json!({"status": "error", "message": "Timeout checking if user is active"})));
                    }
                };
                if is_active {
                    return Ok::<_, Rejection>(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
                }
                match tx.try_send(user_id.clone()) {
                    Ok(_) => {
                        info!("Force sync triggered for user {}", user_id);
                        Ok(warp::reply::json(&json!({"status": "ok", "message": "Sync triggered"})))
                    },
                    Err(error) => {
                        error!("Failed to trigger sync: {:?}", error);
                        match error {
                            tokio::sync::mpsc::error::TrySendError::Full(_) => {
                                Ok(warp::reply::json(&json!({"status": "error", "message": "Sync queue is full, try again later"})))
                            },
                            _ => {
                                Ok(warp::reply::json(&json!({"status": "error", "message": "Failed to trigger sync"})))
                            }
                        }
                    }
                }
            } // Added closing brace here
        })
}

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
