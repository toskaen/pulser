// deposit-service/src/api.rs
use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use serde_json::json;
use log::{info, warn, debug};
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
use common::wallet_utils;
use serde::Serialize;

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
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let client_for_sync = client.clone();
    let price_feed_for_sync = price_feed.clone();

let health = warp::path("health")
    .and(warp::get())
    .and(with_service_status(service_status.clone()))
    .and(with_esplora_urls(esplora_urls.clone()))
    .and(with_price(price_info.clone()))
    .and_then({
        move |service_status: Arc<Mutex<ServiceStatus>>, 
              esplora_urls: Arc<Mutex<Vec<(String, u32)>>>, 
              price_info: Arc<Mutex<PriceInfo>>| async move {
            let status = service_status.lock().await;
            let esplora_urls = esplora_urls.lock().await;
            let price_info = price_info.lock().await;
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0)).as_secs();
            let websocket_active = status.websocket_active; // Use existing field
            let websocket_action = if websocket_active { "WebSocket operational" } else { "Action: Check WebSocket connection" };
            let price_staleness_secs = now - price_info.timestamp as u64;
            let price_action = if price_staleness_secs < 60 { "Price feed current" } else { "Action: Check Deribit connectivity" };
            let esplora_status: HashMap<String, bool> = esplora_urls.iter().map(|(url, errors)| (url.clone(), *errors < 10)).collect();
            let esplora_action = if esplora_status.values().all(|&healthy| healthy) { "Esplora endpoints operational" } else { "Action: Check Esplora URLs" };
            Ok::<_, Rejection>(warp::reply::json(&json!({
                "status": "OK",
                "health": status.health.clone(),
                "active_syncs": status.active_syncs,
                "websocket": { "active": websocket_active, "action": websocket_action },
                "price": { "staleness_secs": price_staleness_secs, "last_price": price_info.raw_btc_usd, "action": price_action },
                "esplora": { "endpoints": esplora_status, "action": esplora_action }
            })))
        }
    });

    let status = warp::path("status").and(warp::get()).and_then({
        let status_data = service_status.clone();
        move || {
            let status_clone = status_data.clone();
            async move { Ok::<_, Rejection>(warp::reply::json(&status_clone.lock().await.clone())) }
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
              statuses: Arc<Mutex<HashMap<String, UserStatus>>>| async move {
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
                (Some(status), Some((wallet, _))) => {
                    let balance = wallet.wallet.balance();
                    Ok::<_, Rejection>(warp::reply::json(&json!({
                        "user_id": user_id,
                        "status": status,
                        "balance": {
                            "confirmed_btc": balance.confirmed.to_sat() as f64 / 100_000_000.0,
                            "unconfirmed_btc": balance.untrusted_pending.to_sat() as f64 / 100_000_000.0,
                            "immature_btc": balance.immature.to_sat() as f64 / 100_000_000.0
                        }
                    })))
                }
                _ => Ok::<_, Rejection>(warp::reply::json(&json!({"error": "User not found"}))),
            }
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
            let wallets_lock = wallets.lock().await;
            match wallets_lock.get(&user_id) {
                Some((wallet, _)) => {
                    let txs: Vec<TxInfo> = wallet.wallet.transactions()
                        .map(|tx| {
                            let tx_node = &*tx.tx_node; // Dereference Arc
                            let amount = tx_node.output.iter().map(|o| o.value.to_sat()).sum();
                            let is_spent = wallet.wallet.list_unspent() // No .iter()
                                .filter(|u| u.outpoint.txid == tx_node.compute_txid())
                                .all(|u| u.is_spent);
                            let (height, timestamp) = match tx.chain_position {
                                bdk_chain::ChainPosition::Confirmed { anchor, .. } => {
                                    (Some(anchor.block_id.height), Some(anchor.confirmation_time))
                                }
                                bdk_chain::ChainPosition::Unconfirmed { .. } => (None, None), // Fixed syntax
                            };
                            TxInfo {
                                txid: tx_node.compute_txid().to_string(), // Updated
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
        .and(warp::any().map(move || Arc::clone(&esplora)))
        .and(with_state_manager(state_manager.clone()))
        .and(with_retry_queue(retry_queue.clone()))
        .and(warp::any().map(move || webhook_url.clone()))
        .and(warp::any().map(move || webhook_config.clone()))
        .and(warp::any().map(move || client_for_sync.clone()))
        .and(with_active_tasks_manager(active_tasks_manager.clone()))
        .and(warp::any().map(move || price_feed_for_sync.clone()))
        .and_then(sync_user_handler);

let force_sync = warp::path!("force_sync" / String)
    .and(warp::post())
    .and_then({
        let tx = sync_tx.clone();
        let active_tasks_manager = active_tasks_manager.clone();
        move |user_id: String| {
            let tx = tx.clone();
            let active_tasks_manager = active_tasks_manager.clone();
            async move {
                if active_tasks_manager.is_user_active(&user_id).await {
                    return Ok::<_, Rejection>(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
                }
                tx.send(user_id.clone()).await
                    .map_err(|_| warp::reject::custom(CustomError::Pulser(PulserError::InternalError("Failed to trigger sync".to_string()))))?;
                Ok(warp::reply::json(&json!({"status": "ok", "message": "Sync triggered"})))
            }
        }
    });

let register = warp::path("register")
    .and(warp::post())
    .and(warp::body::json())
    .and_then({
        let state_manager = state_manager.clone();
        let wallets = wallets.clone();
        let service_status = service_status.clone();
        let user_statuses = user_statuses.clone();
        let price_feed = price_feed.clone();
        move |public_data: serde_json::Value| {
            let state_manager = state_manager.clone();
            let wallets = wallets.clone();
            let service_status = service_status.clone();
            let user_statuses = user_statuses.clone();
            let price_feed = price_feed.clone();
            async move {
                let user_id = public_data["user_id"].as_str().unwrap_or("unknown").to_string();
                let user_dir = PathBuf::from(format!("user_{}", user_id));
                let public_path = user_dir.join(format!("user_{}_public.json", user_id));
                state_manager.save(&public_path, &public_data).await
                    .map_err(|e| warp::reject::custom(CustomError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))))?;
                info!("Registered user: {}", user_id);

                let mut wallets_lock = wallets.lock().await;
                if !wallets_lock.contains_key(&user_id) {
                    match DepositWallet::from_config("config/service_config.toml", &user_id, &state_manager, price_feed.clone()).await {
                        Ok((wallet, deposit_info, chain)) => {
                            let mut statuses = user_statuses.lock().await;
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
                            let mut status = service_status.lock().await;
                            status.users_monitored = wallets_lock.len() as u32;
                        }
                        Err(e) => warn!("Failed to init wallet for {}: {}", user_id, e),
                    }
                }
                Ok::<_, Rejection>(warp::reply::json(&json!({"status": "registered"})))
            }
        }
    });

    health.or(status).or(user_status).or(user_txs).or(sync_route).or(force_sync).or(register)
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
    active_tasks_manager: Arc<UserTaskLock>, // Updated to UserTaskLock
    price_feed: Arc<PriceFeed>,
) -> Result<impl Reply, Rejection> {
    if active_tasks_manager.is_user_active(&user_id).await {
        return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
    }
    active_tasks_manager.mark_user_active(&user_id).await;

    let result = sync_user(
        &user_id,
        wallets,
        user_statuses,
        price,
        price_feed,
        esplora_urls,
        &esplora,
        state_manager,
        retry_queue,
        &webhook_url,
        &webhook_config,
        client,
    ).await;

    active_tasks_manager.mark_user_inactive(&user_id).await;
    result.map(|new_funds| {
        let message = if new_funds { "Sync completed with new funds".to_string() } else { "Sync completed".to_string() };
        warp::reply::json(&json!({"status": "ok", "message": message}))
    }).map_err(warp::reject::custom)
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
) -> Result<bool, PulserError> {
    let start_time = Instant::now();
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    let mut statuses = user_statuses.lock().await;
    let status = statuses.entry(user_id.to_string()).or_insert_with(|| UserStatus::new(user_id));

    let last_sync = status.last_sync;
    let sync_interval = 24 * 3600;
    let should_full_sync = last_sync == 0 || now - last_sync > sync_interval;
    debug!("Syncing user {}: last_sync={}, should_full_sync={}", user_id, last_sync, should_full_sync);

    let mut wallets_lock = wallets.lock().await;
    if !wallets_lock.contains_key(user_id) {
        let (wallet, deposit_info, chain) = DepositWallet::from_config("config/service_config.toml", user_id, &state_manager, price_feed.clone()).await?;
        status.current_deposit_address = deposit_info.address;
        wallets_lock.insert(user_id.to_string(), (wallet, chain));
        info!("Initialized wallet for user {} during sync", user_id);
    }

    let (wallet, chain) = wallets_lock.get_mut(user_id)
        .ok_or_else(|| PulserError::UserNotFound(user_id.to_string()))?;
    let price_info_data = price_info.lock().await.clone();
    let new_utxos = wallet.update_stable_chain(&price_info_data).await?;

    let new_funds_detected = !new_utxos.is_empty();
    if new_funds_detected && !webhook_url.is_empty() {
        if let Err(e) = notify_new_utxos(&client, user_id, &new_utxos, chain, webhook_url, retry_queue.clone(), webhook_config).await {
            warn!("Webhook failed for user {}: {}. Queued for retry.", user_id, e);
        } else {
            debug!("Webhook sent for user {}: {} new UTXOs", user_id, new_utxos.len());
        }
    }

    let balance = wallet.wallet.balance();
    status.last_sync = now;
    status.sync_status = "completed".to_string();
    status.utxo_count = chain.utxos.len() as u32;
    status.total_value_btc = balance.confirmed.to_sat() as f64 / 100_000_000.0;
    status.total_value_usd = chain.stabilized_usd.0; // Extract f64 from USD

    status.last_success = now;
    status.sync_duration_ms = start_time.elapsed().as_millis() as u64;
    status.last_update_message = if new_funds_detected { "Sync completed with new funds".to_string() } else { "Sync completed".to_string() };
    status.confirmations_pending = balance.untrusted_pending.to_sat() > 0;

    if !chain.utxos.is_empty() {
        info!("User {} sync complete: {} UTXOs, {} BTC (${:.2})", user_id, chain.utxos.len(), balance.confirmed.to_sat() as f64 / 100_000_000.0, chain.stabilized_usd);
    } else if should_full_sync {
        info!("No deposits yet for user {} (balance: 0 BTC)", user_id);
    }

    Ok(new_funds_detected)
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
