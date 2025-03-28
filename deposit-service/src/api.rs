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
use std::str::FromStr;
use reqwest::Client;
use common::error::PulserError;
use common::types::PriceInfo;
use common::types::USD;
use crate::wallet::DepositWallet;
use common::{ServiceStatus, UserStatus, StableChain, WebhookRetry, Bitcoin};
use common::types::{UtxoInfo as TypesUtxoInfo};
use common::storage::{StateManager, UtxoInfo as StorageUtxoInfo};
use crate::webhook::{WebhookConfig, notify_new_utxos};
use tokio::time::sleep;
use crate::wallet_init::{self}; // Add to imports
use crate::task_manager::ActiveTasksManager;
use common::price_feed::PriceFeed;

#[derive(Debug)]
enum CustomError {
    Io(std::io::Error),
    Config(String),
    Pulser(PulserError),
    HttpError(reqwest::Error),          // Make sure this is the correct name
    AddressError(bitcoin::address::ParseError),  
    NetworkError(bitcoin::network::ParseNetworkError),
}


// Similarly for other implementations

// Make it a valid rejection type
impl warp::reject::Reject for CustomError {}

// Then implement utility functions to convert from various errors to warp::Rejection

// For use in ? operator inside route handlers
fn to_rejection<E>(err: E) -> warp::Rejection 
where 
    E: Into<CustomError>,
{
    warp::reject::custom(err.into())
}

// Implementations to convert error types to CustomError
impl From<std::io::Error> for CustomError { 
    fn from(err: std::io::Error) -> Self { 
        CustomError::Io(err) 
    }
}

impl From<toml::de::Error> for CustomError {
    fn from(err: toml::de::Error) -> Self {
        CustomError::Config(err.to_string())
    }
}

// Then implement From traits with matching variant names:
impl From<reqwest::Error> for CustomError {
    fn from(err: reqwest::Error) -> Self { 
        CustomError::HttpError(err)     // Make sure this matches the enum variant
    }
}


impl From<bitcoin::address::ParseError> for CustomError {
    fn from(err: bitcoin::address::ParseError) -> Self {
        CustomError::AddressError(err)  // Correct variant name
    }
}

impl From<bitcoin::network::ParseNetworkError> for CustomError {
    fn from(err: bitcoin::network::ParseNetworkError) -> Self {
        CustomError::NetworkError(err)  // Correct variant name
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
    active_tasks_manager: Arc<ActiveTasksManager>,
    price_feed: Arc<PriceFeed>, 
    
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
let client_for_sync = client.clone();
let client_for_init = client.clone();
let price_feed_for_sync = price_feed.clone();
let price_feed_for_register = price_feed.clone();
let price_feed_for_sync_utxos = price_feed.clone();
let price_feed_for_init_wallet = price_feed.clone();
let health = warp::path("health")
    .and(warp::get())
    .and_then({
        let service_status = service_status.clone();
        move || {
            let service_status = service_status.clone();
            async move {
                let status = service_status.lock().await;
                Ok::<_, Rejection>(warp::reply::json(&json!({
                    "status": "OK",
                    "health": status.health.clone(),
                    "active_syncs": status.active_syncs,
                    "api_status": status.api_status.clone(),
                    "error_rate": status.error_rate
                })))
            }
        }
    });

    let status = warp::path("status").and(warp::get()).and_then({
        let status_data = service_status.clone();
        move || {
            let status_clone = status_data.clone();
            async move { Ok::<_, Rejection>(warp::reply::json(&status_clone.lock().await.clone())) }
        }
    });

    let user_status = warp::path("user").and(warp::path::param::<String>()).and(warp::get()).and_then({
        let user_data = user_statuses.clone();
        move |user_id: String| {
            let user_data_clone = user_data.clone();
            async move {
                let statuses = user_data_clone.lock().await;
                match statuses.get(&user_id) {
                    Some(status) => Ok::<_, Rejection>(warp::reply::json(status)),
                    None => Ok::<_, Rejection>(warp::reply::json(&json!({"error": "User not found"}))),
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
        .and(warp::any().map(move || client_for_sync.clone()))
        .and(with_active_tasks_manager(active_tasks_manager.clone()))
        .and(warp::any().map(move || price_feed_for_sync.clone())) // Use the clone for this route
        .and_then(sync_user_handler);

let active_tasks_manager_for_force = active_tasks_manager.clone();
let force_sync = warp::path!("force_sync" / String)  // Add String parameter in path
    .and(warp::post())
    .and_then({
        let tx = sync_tx.clone();  // Use sync_tx instead of sync_sender
        let active_tasks_manager = active_tasks_manager.clone();
        move |user_id: String| {
            let tx = tx.clone();
            let active_tasks_manager = active_tasks_manager.clone();
            async move {
                if active_tasks_manager.is_user_active(&user_id).await {
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
            let price_feed = price_feed_for_register.clone(); // Use the clone for this route
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
                    // Don't clone the wallet - just insert it directly
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
                            // Insert the wallet directly - no clone needed
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

// Fix for sync_utxos route - avoid cloning wallet
let sync_utxos = warp::path!("sync_utxos" / String)
    .and(with_active_tasks_manager(active_tasks_manager.clone()))
    .and_then({
        let wallets = wallets.clone();
        let price_info = price_info.clone();
        let user_statuses = user_statuses.clone();
            let price_feed = price_feed_for_sync_utxos.clone(); // Use the clone for this route
        move |user_id: String, active_tasks_manager: Arc<ActiveTasksManager>| {
            let wallets = wallets.clone();
            let price_info = price_info.clone();
            let price_feed = price_feed.clone();
            let user_statuses = user_statuses.clone();
            async move {
                if active_tasks_manager.is_user_active(&user_id).await {
                    return Err(warp::reject::custom(PulserError::InternalError("User already syncing".to_string())));
                }
                active_tasks_manager.mark_user_active(&user_id).await;

                let mut status_guard = user_statuses.lock().await;
                let status = status_guard.entry(user_id.clone()).or_insert_with(|| UserStatus::new(&user_id));
                let addr = Address::from_str(&status.current_deposit_address)
                    .map_err(|e| warp::reject::custom(PulserError::InvalidInput(e.to_string())))?
                    .require_network(Network::Testnet)
                    .map_err(|e| warp::reject::custom(PulserError::InvalidInput(e.to_string())))?;
                drop(status_guard);

                // Use the wallet reference inside the lock scope - don't clone it
                let result = {
                    let mut wallets_guard = wallets.lock().await;
                    match wallets_guard.get_mut(&user_id) {
                        Some((wallet, _)) => {
                            let price_info_data = price_info.lock().await.clone();
                            wallet.check_address(&addr, &price_info_data, &price_feed).await
                                .map(|utxos| warp::reply::json(&utxos))
                        }
                        None => Err(PulserError::UserNotFound(user_id.clone())),
                    }
                };

                active_tasks_manager.mark_user_inactive(&user_id).await;
                result.map_err(warp::reject::custom)
            }
        }
    });
    
    let init_wallet = warp::path("init_wallet")
    .and(warp::post())
    .and(warp::body::json())
    .and(with_wallets(wallets.clone()))
    .and(with_statuses(user_statuses.clone()))
    .and(with_state_manager(state_manager.clone()))
    .and(with_client(client_for_init))
    .and(with_config_path("config/service_config.toml".to_string()))
    .and_then({
            let price_feed = price_feed_for_init_wallet.clone(); // Use the clone for this route
        move |req: serde_json::Value,
              wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
              user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
              state_manager: Arc<StateManager>,
              client: Client,
              config_path: String| {
            let price_feed = price_feed.clone(); // Clone for async move
            async move {
                let user_id = req["user_id"].as_str().ok_or_else(|| warp::reject::custom(PulserError::InvalidInput("Missing user_id".into())))?.to_string();
let config_str = fs::read_to_string(&config_path).map_err(|e| warp::reject::custom(CustomError::from(e)))?;
                    let config: wallet_init::Config = toml::from_str(&config_str).map_err(to_rejection)?;
                    let init_result = wallet_init::init_wallet(&config, &user_id)?;

let wallet = DepositWallet::from_descriptors(
    init_result.external_descriptor.clone(),
    init_result.internal_descriptor.clone(),
    Network::from_str(&config.network).map_err(to_rejection)?,
    &config.esplora_url,
    &format!("{}/user_{}/multisig", config.data_dir, user_id),
    &user_id,
Address::from_str(&init_result.deposit_info.address)
    .map_err(to_rejection)?
    .assume_checked(),
    &state_manager,
    price_feed,
    
).await?;
client.post("http://localhost:8081/register")
    .json(&init_result.public_data)
    .send()
    .await
    .map_err(to_rejection)?;

                    let mut wallets_lock = wallets.lock().await;
let stable_chain = wallet.stable_chain.clone();
wallets_lock.insert(user_id.clone(), (wallet, stable_chain));
                    let mut statuses = user_statuses.lock().await;
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
    active_tasks_manager: Arc<ActiveTasksManager>,
    price_feed: Arc<PriceFeed>, // Fix: Add type
) -> Result<impl Reply, Rejection> {
    if active_tasks_manager.is_user_active(&user_id).await {
        return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
    }
    active_tasks_manager.mark_user_active(&user_id);
    
    // Note the _ prefix to silence the warning about unused variable
    let _price_info = price.lock().await.clone();
    
    let result = sync_user(
        &user_id,
        wallets.clone(),
        user_statuses.clone(),
        price.clone(),
        price_feed.clone(), 
        esplora_urls.clone(),
        state_manager.clone(),
        retry_queue.clone(),
        &webhook_url,
        &webhook_config,
        client.clone(),
    ).await;
    
    active_tasks_manager.mark_user_inactive(&user_id);
    result.map(|_| warp::reply::json(&json!({"status": "ok", "message": "Sync completed"})))
        .map_err(warp::reject::custom)
}

pub async fn sync_user(
    user_id: &str,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_info: Arc<Mutex<PriceInfo>>, 
    price_feed: Arc<PriceFeed>,      
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

    let mut statuses = user_statuses.lock().await;
    let status = statuses.entry(user_id.to_string()).or_insert_with(|| UserStatus::new(user_id));

    let last_sync = status.last_sync;
    let sync_interval = 24 * 3600;
    let should_full_sync = last_sync == 0 || now - last_sync > sync_interval;

    // Only initialize the wallet if needed - avoid unnecessary cloning
    {
        let mut wallets_lock = wallets.lock().await;
        if !wallets_lock.contains_key(user_id) {
            let (wallet, deposit_info, chain) = DepositWallet::from_config("config/service_config.toml", user_id, &state_manager, price_feed.clone()).await?;
            status.current_deposit_address = deposit_info.address;
            wallets_lock.insert(user_id.to_string(), (wallet, chain));
        }
    }

    // Get previous UTXOs without cloning the whole wallet
    let previous_utxos = {
        let wallets_lock = wallets.lock().await;
        if let Some((_, chain)) = wallets_lock.get(user_id) {
            chain.utxos.clone()
        } else {
            return Err(PulserError::UserNotFound(user_id.to_string()));
        }
    };

    let mut new_funds_detected = false;
    let deposit_addr = Address::from_str(&status.current_deposit_address)?.assume_checked();
    
    // Get change address without cloning the wallet
    let change_addr = {
        let mut wallets_lock = wallets.lock().await;
        if let Some((wallet, _)) = wallets_lock.get_mut(user_id) {
            wallet.wallet.reveal_next_address(KeychainKind::Internal).address
        } else {
            return Err(PulserError::UserNotFound(user_id.to_string()));
        }
    };
    
    let mut attempts = 0;
    let mut utxos = Vec::new();
    let mut esplora_failed = false;
    const DEFAULT_RETRY_MAX: u32 = 3;

    while attempts < DEFAULT_RETRY_MAX {
        let best_url = {
            let urls = esplora_urls.lock().await;
            urls.iter().min_by_key(|(_, f)| *f).cloned().unwrap_or_else(|| (String::new(), 0))
        };
        let price_info_data = price_info.lock().await.clone();

        // Check deposit address without cloning the wallet
   let deposit_utxos = {
            let mut wallets_lock = wallets.lock().await;
            if let Some((wallet, _)) = wallets_lock.get_mut(user_id) {
                wallet.update_stable_chain(&price_info_data, &price_feed).await
            } else {
                Err(PulserError::UserNotFound(user_id.to_string()))
            }
        };

        match deposit_utxos {
            Ok(deposit_utxos) => {
                utxos.extend(deposit_utxos);
                
                // Check change address without cloning the wallet
                let change_utxos = {
                    let mut wallets_lock = wallets.lock().await;
                    if let Some((wallet, _)) = wallets_lock.get_mut(user_id) {
                        wallet.check_address(&change_addr, &price_info_data, &price_feed).await
                    } else {
                        Err(PulserError::UserNotFound(user_id.to_string()))
                    }
                };
                
                match change_utxos {
                    Ok(change_utxos) => {
                        utxos.extend(change_utxos);
                        let mut urls = esplora_urls.lock().await;
                        if let Some(entry) = urls.iter_mut().find(|(u, _)| u == &best_url.0) {
                            entry.1 += 1;
                        }
                        break;
                    }
                    Err(e) => {
                        let mut urls = esplora_urls.lock().await;
                        if let Some(entry) = urls.iter_mut().find(|(u, _)| u == &best_url.0) {
                            entry.1 += 1;
                        }
                        warn!("Change address sync failed with {}: {}. Attempt {}/{}", best_url.0, e, attempts, DEFAULT_RETRY_MAX);
                    }
                }
            }
            Err(e) => {
                let mut urls = esplora_urls.lock().await;
                if let Some(entry) = urls.iter_mut().find(|(u, _)| u == &best_url.0) {
                    entry.1 += 1;
                }
                warn!("Deposit address sync failed with {}: {}. Attempt {}/{}", best_url.0, e, attempts, DEFAULT_RETRY_MAX);
            }
        }

        attempts += 1;
        if attempts == DEFAULT_RETRY_MAX {
            warn!("All Esplora attempts failed for user {}. Using cached UTXOs.", user_id);
            
            // Get cached UTXOs without cloning the wallet
            utxos = {
                let wallets_lock = wallets.lock().await;
                if let Some((wallet, _)) = wallets_lock.get(user_id) {
                    wallet.get_cached_utxos()
                } else {
                    Vec::new()
                }
            };
            
            esplora_failed = true;
            status.last_error = Some("Esplora unavailable, using cached data".to_string());
            break;
        }
        sleep(Duration::from_secs(2)).await;
    }

// Update the chain with the new UTXOs
{
    let mut wallets_lock = wallets.lock().await;
    if let Some((_, chain)) = wallets_lock.get_mut(user_id) {
        debug!("Updated chain for user {}: {} UTXOs, {} BTC (${:.2}), {} history entries", 
            user_id, chain.utxos.len(), chain.accumulated_btc.to_btc(), chain.stabilized_usd.0, chain.history.len());
    }
}

// Ensure historical data is preserved for existing UTXOs
{
    let mut wallets_lock = wallets.lock().await;
    if let Some((_, chain)) = wallets_lock.get_mut(user_id) {
        // For each UTXO, make sure it's in the history if not already present
        for utxo in &utxos {
            if !chain.history.iter().any(|h| h.txid == utxo.txid && h.vout == utxo.vout) {
                let utxo_addr = Address::from_script(&ScriptBuf::from_hex(&utxo.script_pubkey)?, Network::Testnet)?;
                let is_external = utxo_addr.to_string() == deposit_addr.to_string();
                
                let utxo_info = TypesUtxoInfo {
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
                
                chain.history.push(utxo_info);
                debug!("Added UTXO to history: {}:{} (${:.2})", utxo.txid, utxo.vout, 
                    utxo.usd_value.as_ref().unwrap_or(&USD(0.0)).0);
            }
        }
    }
}

// Find new UTXOs (compared to previous state)
let mut new_utxos = Vec::new();
for utxo in &utxos {
        if !previous_utxos.iter().any(|u| u.txid == utxo.txid && u.vout == utxo.vout) {
            let utxo_addr = Address::from_script(&ScriptBuf::from_hex(&utxo.script_pubkey)?, Network::Testnet)?;
            let is_external = utxo_addr.to_string() == deposit_addr.to_string();

            let utxo_info = StorageUtxoInfo {
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
    
    let new_addr = {
        let mut wallets_lock = wallets.lock().await;
        if let Some((wallet, _)) = wallets_lock.get_mut(user_id) {
            wallet.reveal_new_address().await?
        } else {
            return Err(PulserError::UserNotFound(user_id.to_string()));
        }
    };
    
    status.current_deposit_address = new_addr.to_string();
    status.last_deposit_time = Some(now);
}

    // Add stabilization and hedging here
// Add stabilization and hedging here
if new_funds_detected {
    let btc_amount = {  // Corrected syntax
        let wallets_lock = wallets.lock().await;
        if let Some((_, chain)) = wallets_lock.get(user_id) {
            chain.accumulated_btc.to_btc()
        } else {
            0.0
        }
    };
    
    let median_price = price_info.lock().await.raw_btc_usd;
    let deribit_price = price_feed.get_deribit_price().await.unwrap_or(median_price);
    let diff_percent = if median_price > 0.0 && deribit_price > 0.0 {
        ((deribit_price - median_price) / median_price * 100.0).abs()
    } else { 0.0 };
    
    if diff_percent > 5.0 {
        warn!("Price divergence for user {}: Median ${:.2} vs Deribit ${:.2} ({}%)", user_id, median_price, deribit_price, diff_percent);
    }
}


    // Get the chain for webhook notification
    let chain_for_webhook = {
        let wallets_lock = wallets.lock().await;
        if let Some((_, chain)) = wallets_lock.get(user_id) {
            chain.clone()
        } else {
            return Err(PulserError::UserNotFound(user_id.to_string()));
        }
    };

    // Send webhook notification if needed
    if !new_utxos.is_empty() && !webhook_url.is_empty() {
        if let Err(e) = notify_new_utxos(&client, user_id, &new_utxos, &chain_for_webhook, webhook_url, retry_queue.clone(), webhook_config).await {
            warn!("Webhook failed for user {}: {}. Queued for retry.", user_id, e);
        }
    }

    // Get totals for status update
    let total_stable_usd = {
        let wallets_lock = wallets.lock().await;
        if let Some((_, chain)) = wallets_lock.get(user_id) {
            chain.utxos.iter().map(|u| u.usd_value.as_ref().unwrap_or(&USD(0.0)).0).sum()
        } else {
            0.0
        }
    };

    let accumulated_btc = {
        let wallets_lock = wallets.lock().await;
        if let Some((_, chain)) = wallets_lock.get(user_id) {
            chain.accumulated_btc.to_btc()
        } else {
            0.0
        }
    };

    // Update status
    status.last_sync = now;
    status.sync_status = if esplora_failed { "completed_with_cache".to_string() } else { "completed".to_string() };
    status.utxo_count = utxos.len() as u32;
    status.total_value_btc = accumulated_btc;
    status.total_value_usd = total_stable_usd;
    status.last_success = now;
    status.sync_duration_ms = start_time.elapsed().as_millis() as u64;
    status.last_update_message = if esplora_failed { "Sync completed with cached data".to_string() } else { "Sync completed".to_string() };
    state_manager.save(&status_path, status).await?;

    // Log the result
    if !utxos.is_empty() {
        info!("User {} sync complete: {} UTXOs, {} BTC (${:.2})", user_id, utxos.len(), accumulated_btc, total_stable_usd);
    } else if should_full_sync {
        info!("No deposits yet for user {} (balance: 0 BTC)", user_id);
    }
    
    // Ensure StableChain is explicitly saved again here
let chain_to_save = {
    let mut wallets_lock = wallets.lock().await;
    if let Some((_, chain)) = wallets_lock.get_mut(user_id) {
        // Ensure accumulated_btc matches utxos
        let total_sats: u64 = chain.utxos.iter().map(|u| u.amount).sum();
        chain.accumulated_btc = Bitcoin::from_sats(total_sats);
        
        // Ensure stabilized_usd reflects history
        chain.stabilized_usd = USD(
            chain.history.iter()
                .filter(|h| !h.spent)
                .map(|h| h.stable_value_usd)
                .sum()
        );
        
        // Update timestamp
        chain.timestamp = now as i64;
        
        chain.clone()
    } else {
        return Err(PulserError::UserNotFound(user_id.to_string()));
    }
  
};


// Log detailed information for debugging
info!("Saving StableChain for user {}: {} UTXOs, {} BTC (${:.2}), {} history entries", 
    user_id, 
    chain_to_save.utxos.len(),
    chain_to_save.accumulated_btc.to_btc(), 
    chain_to_save.stabilized_usd.0,
    chain_to_save.history.len());

if let Err(e) = state_manager.save_stable_chain(user_id, &chain_to_save).await {
    warn!("Failed to save StableChain: {}", e);
} else {
    debug!("Successfully saved StableChain to {}/user_{}/stable_chain_{}.json", 
        state_manager.data_dir.display(), user_id, user_id);
}
    
return Ok(new_funds_detected);
}

fn with_wallets(wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>) -> impl Filter<Extract = (Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || wallets.clone())
}

fn with_statuses(statuses: Arc<Mutex<HashMap<String, UserStatus>>>) -> impl Filter<Extract = (Arc<Mutex<HashMap<String, UserStatus>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || statuses.clone())
}

fn with_price(price: Arc<Mutex<PriceInfo>>) -> impl Filter<Extract = (Arc<Mutex<PriceInfo>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || price.clone())
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

fn with_config_path(config_path: String) -> impl Filter<Extract = (String,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || config_path.clone())
}

fn with_active_tasks_manager(manager: Arc<ActiveTasksManager>) -> impl Filter<Extract = (Arc<ActiveTasksManager>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || manager.clone())
}
