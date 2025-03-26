// deposit-service/src/api.rs
use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use serde_json::json;
use log::{info, warn, debug};
use bdk_wallet::{KeychainKind, bitcoin::{Address, Network, ScriptBuf}};
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::PathBuf;
use reqwest::Client;
use common::error::PulserError;
use common::price_feed::PriceInfo;
use common::types::USD;
use deposit_service::wallet::DepositWallet;
use deposit_service::types::{ServiceStatus, UserStatus, StableChain, WebhookRetry, Bitcoin};
use deposit_service::storage::{StateManager, UtxoInfo};
use deposit_service::webhook::{WebhookConfig, notify_new_utxos};
use tokio::time::sleep;
use crate::wallet_init::{self, WalletInitResult}; // Add to imports
use crate::storage::StateManager; // Add this



#[derive(Debug)]
enum CustomError {
    Serde(serde_json::Error),
    Io(std::io::Error),
}
impl warp::reject::Reject for CustomError {}

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
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let health = warp::path("health").map({
        let service_status = service_status.clone();
        move || {
            let status = service_status.blocking_lock().unwrap();
            warp::reply::json(&json!({
                "status": "OK",
                "health": status.health.clone(),
                "active_syncs": status.active_syncs,
                "api_status": status.api_status.clone(),
                "error_rate": status.error_rate
            }))
        }
    });

    let status = warp::path("status").and(warp::get()).and_then({
        let status_data = service_status.clone();
        move || {
            let status_clone = status_data.clone();
            async move { Ok::<_, Rejection>(warp::reply::json(&status_clone.lock().await.unwrap().clone())) }
        }
    });

    let user_status = warp::path("user").and(warp::path::param::<String>()).and(warp::get()).and_then({
        let user_data = user_statuses.clone();
        move |user_id: String| {
            let user_data_clone = user_data.clone();
            async move {
                let statuses = user_data_clone.lock().await.unwrap();
                match statuses.get(&user_id) {
                    Some(status) => Ok::<_, Rejection>(warp::reply::json(status)),
                    None => Ok::<_, Rejection>(warp::reply::json(&json!({"error": "User not found"}))),
                }
            }
        }
    });

    let activity = warp::path!("activity" / String).and(warp::get()).and_then({
        let state_manager = state_manager.clone();
        move |user_id: String| {
            let state_manager = state_manager.clone();
            async move {
                let activity_path = PathBuf::from(format!("user_{}/activity_{}.json", user_id, user_id));
                match state_manager.load::<Vec<UtxoInfo>>(&activity_path).await {
                    Ok(utxos) => Ok::<_, Rejection>(warp::reply::json(&json!({"utxos": utxos}))),
                    Err(_) => Ok::<_, Rejection>(warp::reply::json(&json!({"utxos": []}))),
                }
            }
        }
    });

    let sync_route = warp::post()
        .and(warp::path!("sync" / String))
        .and(with_wallets(wallets.clone()))
        .and(with_statuses(user_statuses.clone()))
        .and(with_price(price_info.clone()))
        .and(with_esplora_urls(esplora_urls.clone()))
        .and(with_state_manager(state_manager.clone()))
        .and(with_retry_queue(retry_queue.clone()))
        .and(warp::any().map(move || webhook_url.clone()))
        .and(warp::any().map(move || webhook_config.clone()))
        .and(warp::any().map(move || client.clone()))
        .and_then(sync_user_handler);

    let force_sync = warp::path("force_sync")
        .and(warp::path::param::<String>())
        .and(warp::post())
        .and_then({
            let sync_sender = sync_tx.clone();
            move |user_id: String| {
                let tx = sync_sender.clone();
                async move {
                    if is_user_active(&user_id) {
                        return Ok::<_, Rejection>(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
                    }
                    tx.send(user_id.clone()).await
                        .map_err(|_| warp::reject::custom(PulserError::InternalError("Failed to trigger sync".to_string())))?;
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
            move |public_data: serde_json::Value| {
                let state_manager = state_manager.clone();
                let wallets = wallets.clone();
                let service_status = service_status.clone();
                let user_statuses = user_statuses.clone();
                async move {
                    let user_id = public_data["user_id"].as_str().unwrap_or("unknown").to_string();
                    let user_dir = PathBuf::from(format!("user_{}", user_id));
                    let public_path = user_dir.join(format!("user_{}_public.json", user_id));
                    state_manager.save(&public_path, &public_data).await
                        .map_err(|e| warp::reject::custom(CustomError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))))?;
                    info!("Registered user: {}", user_id);

                    let mut wallets_lock = wallets.lock().await.unwrap();
                    if !wallets_lock.contains_key(&user_id) {
                        match DepositWallet::from_config("config/service_config.toml", &user_id).await {
                            Ok((wallet, deposit_info, chain)) => {
                                let mut statuses = user_statuses.lock().await.unwrap();
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
                                let mut status = service_status.lock().await.unwrap();
                                status.users_monitored = wallets_lock.len() as u32;
                            }
                            Err(e) => warn!("Failed to init wallet for {}: {}", user_id, e),
                        }
                    }
                    Ok::<_, Rejection>(warp::reply::json(&json!({"status": "registered"})))
                }
            }
        });

    let sync_utxos = warp::path!("sync_utxos" / String).and_then({
        let wallets = wallets.clone();
        let price_info = price_info.clone();
        let user_statuses = user_statuses.clone();
        move |user_id: String| {
            let wallets = wallets.clone();
            let price_info = price_info.clone();
            let user_statuses = user_statuses.clone();
            async move {
                let mut status_guard = user_statuses.lock().await.unwrap();
                let status = status_guard.entry(user_id.clone()).or_insert_with(|| UserStatus::new(&user_id));
                let addr = Address::from_str(&status.current_deposit_address)
                    .map_err(|e| warp::reject::custom(PulserError::InvalidInput(e.to_string())))?
                    .require_network(Network::Testnet)
                    .map_err(|e| warp::reject::custom(PulserError::InvalidInput(e.to_string())))?;
                drop(status_guard);

                let wallets_guard = wallets.lock().await.unwrap();
                let wallet_opt = wallets_guard.get(&user_id).map(|(w, c)| (w, c.clone()));
                drop(wallets_guard);

                match wallet_opt {
                    Some((wallet, _)) => {
                        let price_info_data = price_info.lock().await.unwrap().clone();
                        match wallet.check_address(&addr, &price_info_data).await {
                            Ok(utxos) => Ok(warp::reply::json(&utxos)),
                            Err(e) => Err(warp::reject::custom(e)),
                        }
                    }
                    None => Err(warp::reject::custom(PulserError::UserNotFound(user_id))),
                }
            }
        }
    });
    
    let init_wallet = warp::path("init_wallet")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_wallets(wallets.clone()))
        .and(with_statuses(user_statuses.clone()))
        .and(with_state_manager(state_manager.clone()))
        .and(with_client(client.clone()))
        .and(with_config_path("config/service_config.toml".to_string()))
        .and_then({
            move |req: serde_json::Value,
                  wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
                  user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
                  state_manager: Arc<StateManager>,
                  client: Client,
                  config_path: String| {
                async move {
                    let user_id = req["user_id"].as_str().ok_or_else(|| warp::reject::custom(PulserError::InvalidInput("Missing user_id".into())))?.to_string();
                    let config_str = fs::read_to_string(&config_path)?;
                    let config: wallet_init::Config = toml::from_str(&config_str)?;
                    let init_result = wallet_init::init_wallet(&config, &user_id)?;

                    let wallet = DepositWallet::from_descriptors(
                        init_result.external_descriptor.clone(),
                        init_result.internal_descriptor.clone(),
                        Network::from_str(&config.network)?,
                        &config.esplora_url,
                        &format!("{}/user_{}/multisig", config.data_dir, user_id),
                        &user_id,
                        Address::from_str(&init_result.deposit_info.address)?,
                        &state_manager,
                    ).await?;

                    client.post("http://localhost:8081/register")
                        .json(&init_result.public_data)
                        .send()
                        .await?;

                    let mut wallets_lock = wallets.lock().await.unwrap();
                    wallets_lock.insert(user_id.clone(), (wallet.clone(), wallet.stable_chain.clone()));
                    let mut statuses = user_statuses.lock().await.unwrap();
                    let status = statuses.entry(user_id.clone()).or_insert_with(|| UserStatus::new(&user_id));
                    status.current_deposit_address = init_result.deposit_info.address.clone();

                    let response = json!({
                        "user_id": user_id,
                        "recovery_doc": init_result.recovery_doc,
                        "address": init_result.deposit_info.address,
                        "descriptor": init_result.deposit_info.descriptor
                    });
                    info!("Initialized wallet for user {}", user_id);
                    Ok::<_, Rejection>(warp::reply::json(&response))
                }
            }
        });
        
    health
        .or(status)
        .or(user_status)
        .or(activity)
        .or(sync_route)
        .or(force_sync)
        .or(register)
        .or(sync_utxos)
        .or(init_wallet)
}



async fn sync_user_handler(
    user_id: String,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price: Arc<Mutex<PriceInfo>>,
    esplora_urls: Arc<Mutex<Vec<(String, u32)>>>,
    state_manager: Arc<StateManager>,
    retry_queue: Arc<Mutex<VecDeque<WebhookRetry>>>,
    webhook_url: String,
    webhook_config: WebhookConfig,
    client: Client,
) -> Result<impl Reply, Rejection> {
    if is_user_active(&user_id) {
        return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
    }
    mark_user_active(&user_id);
    let price_info = price.lock().await.unwrap().clone();
    let result = sync_user(
        &user_id,
        wallets,
        user_statuses,
        price_info,
        esplora_urls,
        state_manager,
        retry_queue,
        &webhook_url,
        &webhook_config,
        client,
    ).await;
    mark_user_inactive(&user_id);
    result.map(|_| warp::reply::json(&json!({"status": "ok", "message": "Sync completed"})))
        .map_err(warp::reject::custom)
}

pub async fn sync_user(
    user_id: &str,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_info: PriceInfo,
    esplora_urls: Arc<Mutex<Vec<(String, u32)>>>,
    state_manager: Arc<StateManager>,
    retry_queue: Arc<Mutex<VecDeque<WebhookRetry>>>,
    webhook_url: &str,
    webhook_config: &WebhookConfig,
    client: Client,
) -> Result<bool, PulserError> {
    let start_time = Instant::now();
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let user_dir = PathBuf::from(format!("user_{}", user_id));
    let status_path = user_dir.join(format!("status_{}.json", user_id));
    let activity_path = user_dir.join(format!("activity_{}.json", user_id));

    let mut statuses = user_statuses.lock().await.unwrap();
    let status = statuses.entry(user_id.to_string()).or_insert_with(|| {
        info!("Initializing status for user {}", user_id);
        UserStatus::new(user_id)
    });

    let mut activity = state_manager.load::<Vec<UtxoInfo>>(&activity_path).await.unwrap_or_else(|_| Vec::new());
    let last_sync = status.last_sync;
    let sync_interval = 24 * 3600;
    let should_full_sync = last_sync == 0 || now - last_sync > sync_interval;

    let mut wallets_lock = wallets.lock().await.unwrap();
    if !wallets_lock.contains_key(user_id) {
        let (wallet, deposit_info, chain) = DepositWallet::from_config("config/service_config.toml", user_id).await?;
        status.current_deposit_address = deposit_info.address;
        wallets_lock.insert(user_id.to_string(), (wallet, chain));
    }
    let (wallet, chain) = wallets_lock.get_mut(user_id).unwrap();
    let previous_utxos = chain.utxos.clone();

    let mut new_funds_detected = false;
    let deposit_addr = Address::from_str(&status.current_deposit_address)?.require_network(Network::Testnet)?;
    let change_addr = wallet.wallet.reveal_next_address(KeychainKind::Internal).address;
    let mut attempts = 0;
    let mut utxos = Vec::new();
    let mut esplora_failed = false;
    const DEFAULT_RETRY_MAX: u32 = 3;

    while attempts < DEFAULT_RETRY_MAX {
        let best_url = {
            let mut urls = esplora_urls.lock().await.unwrap();
            urls.iter_mut().min_by_key(|(_, f)| *f).unwrap().clone()
        };

        match wallet.check_address(&deposit_addr, &price_info).await {
            Ok(deposit_utxos) => {
                utxos.extend(deposit_utxos);
                match wallet.check_address(&change_addr, &price_info).await {
                    Ok(change_utxos) => {
                        utxos.extend(change_utxos);
                        let mut urls = esplora_urls.lock().await.unwrap();
                        urls.iter_mut().find(|(u, _)| u == &best_url.0).unwrap().1 = 0;
                        break;
                    }
                    Err(e) => {
                        let mut urls = esplora_urls.lock().await.unwrap();
                        urls.iter_mut().find(|(u, _)| u == &best_url.0).unwrap().1 += 1;
                        warn!("Change address sync failed with {}: {}. Attempt {}/{}", best_url.0, e, attempts, DEFAULT_RETRY_MAX);
                    }
                }
            }
            Err(e) => {
                let mut urls = esplora_urls.lock().await.unwrap();
                urls.iter_mut().find(|(u, _)| u == &best_url.0).unwrap().1 += 1;
                warn!("Deposit address sync failed with {}: {}. Attempt {}/{}", best_url.0, e, attempts, DEFAULT_RETRY_MAX);
            }
        }

        attempts += 1;
        if attempts == DEFAULT_RETRY_MAX {
            warn!("All Esplora attempts failed for user {}. Using cached UTXOs.", user_id);
            utxos = wallet.get_cached_utxos();
            esplora_failed = true;
            status.last_error = Some("Esplora unavailable, using cached data".to_string());
            break;
        }
        sleep(Duration::from_secs(2)).await;
    }

    chain.utxos = utxos;
    chain.accumulated_btc = Bitcoin::from_sats(chain.utxos.iter().map(|u| u.amount).sum());

    for utxo in &previous_utxos {
        if !chain.utxos.iter().any(|u| u.txid == utxo.txid && u.vout == utxo.vout) {
            if let Some(activity_utxo) = activity.iter_mut().find(|a| a.txid == utxo.txid && a.vout == utxo.vout) {
                activity_utxo.spent = true;
            }
        }
    }

    let mut new_utxos = Vec::new();
    for utxo in &chain.utxos {
        if !previous_utxos.iter().any(|u| u.txid == utxo.txid && u.vout == utxo.vout) {
            let utxo_addr = Address::from_script(&ScriptBuf::from_hex(&utxo.script_pubkey)?, Network::Testnet)?;
            let is_external = utxo_addr.to_string() == deposit_addr.to_string();

            let utxo_info = UtxoInfo {
                txid: utxo.txid.clone(),
                amount_sat: utxo.amount,
                address: utxo_addr.to_string(),
                keychain: if is_external { "External".to_string() } else { "Internal".to_string() },
                timestamp: now,
                confirmations: utxo.confirmations,
                participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
                stable_value_usd: utxo.usd_value.as_ref().unwrap_or(&USD(0.0)).0,
                spendable: utxo.confirmations >= 1,
                derivation_path: if is_external { "m/84'/1'/0'/0/0".to_string() } else { "m/84'/1'/0'/1/0".to_string() },
                vout: utxo.vout,
                spent: false,
            };
            new_utxos.push(utxo_info);
            new_funds_detected = true;
        }
    }

if new_funds_detected {
    debug!("New funds detected for user {}", user_id);
    let total_btc = new_utxos.iter().map(|u| u.amount_sat as f64 / 100_000_000.0).sum::<f64>();
    let total_usd = new_utxos.iter().map(|u| u.stable_value_usd).sum::<f64>();
    info!("Detected {} new UTXOs for user {}: {} BTC (${:.2})", new_utxos.len(), user_id, total_btc, total_usd);
    let new_addr = wallet.reveal_new_address().await?;
    status.current_deposit_address = new_addr.to_string();
    status.last_deposit_time = Some(now);
}
    activity.extend(new_utxos.clone());
    activity.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    state_manager.prune_activity_log(&activity_path, 100).await?;
    state_manager.save(&activity_path, &activity).await?;

    if !new_utxos.is_empty() && !webhook_url.is_empty() {
        if let Err(e) = notify_new_utxos(&client, user_id, &new_utxos, chain, webhook_url, retry_queue.clone(), webhook_config).await {
            warn!("Webhook failed for user {}: {}. Queued for retry.", user_id, e);
        }
    }

    let total_stable_usd: f64 = chain.utxos.iter().map(|u| u.usd_value.as_ref().unwrap_or(&USD(0.0)).0).sum();
    status.last_sync = now;
    status.sync_status = if esplora_failed { "completed_with_cache".to_string() } else { "completed".to_string() };
    status.utxo_count = chain.utxos.len() as u32;
    status.total_value_btc = chain.accumulated_btc.to_btc();
    status.total_value_usd = total_stable_usd;
    status.last_success = now;
    status.sync_duration_ms = start_time.elapsed().as_millis() as u64;
    status.last_update_message = if esplora_failed { "Sync completed with cached data".to_string() } else { "Sync completed".to_string() };
    state_manager.save(&status_path, status).await?;

    if !chain.utxos.is_empty() {
        info!("User {} sync complete: {} UTXOs, {} BTC (${:.2})", user_id, chain.utxos.len(), chain.accumulated_btc.to_btc(), total_stable_usd);
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

fn with_price(price: Arc<Mutex<PriceInfo>>) -> impl Filter<Extract = (Arc<Mutex<PriceInfo>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || price.clone())
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

fn with_config_path(config_path: String) -> impl Filter<Extract = (String,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || config_path.clone())
}

pub fn is_user_active(user_id: &str) -> bool {
    crate::is_user_active(user_id)
}

pub fn mark_user_active(user_id: &str) {
    crate::mark_user_active(user_id)
}

pub fn mark_user_inactive(user_id: &str) {
    crate::mark_user_inactive(user_id)
}

impl UserStatus {
    pub fn new(user_id: &str) -> Self {
        Self {
            user_id: user_id.to_string(),
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
            current_deposit_address: "Not yet determined".to_string(),
            last_deposit_time: None,
        }
    }
}
