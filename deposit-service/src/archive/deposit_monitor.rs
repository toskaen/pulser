use bdk_wallet::{Wallet, KeychainKind};
use bdk_wallet::bitcoin::{Address, Network, ScriptBuf};
use common::error::PulserError;
use common::price_feed::{fetch_btc_usd_price, PriceInfo};
use common::types::{USD, Utxo};
use common::utils::now_timestamp;
use lazy_static::lazy_static;
use log::{info, warn, error, debug};
use reqwest::Client;
use serde::{Serialize, Deserialize};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use tokio::sync::{Mutex, mpsc, broadcast};
use tokio::time::{sleep, timeout};
use warp::{Filter, Rejection, Reply};
use deposit_service::wallet::DepositWallet;
use deposit_service::types::{StableChain, Bitcoin, UserStatus, ServiceStatus, WebhookRetry};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

// File lock singleton for concurrency control
lazy_static! {
    static ref FILE_LOCKS: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>> = 
        Arc::new(Mutex::new(HashMap::new()));
    static ref ACTIVE_TASKS: Arc<Mutex<HashMap<String, bool>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

// Constants
const MAX_UTXO_ACTIVITY_ENTRIES: usize = 100;
const DEFAULT_CACHE_DURATION_SECS: u64 = 120;
const MAX_WEBHOOK_RETRIES: u32 = 3;
const MAX_CONCURRENT_USERS: usize = 10;
const WEBHOOK_TIMEOUT_SECS: u64 = 5;
const DEFAULT_RETRY_MAX: u32 = 3;
const DEFAULT_MAX_RETRY_TIME_SECS: u64 = 120;
const DEFAULT_PRICE_FALLBACK: f64 = 0.0;

#[derive(Debug)]
enum CustomError {
    Serde(serde_json::Error),
    Io(std::io::Error),
}
impl warp::reject::Reject for CustomError {}

#[derive(Serialize, Deserialize, Clone)]
struct UtxoInfo {
    txid: String,
    amount_sat: u64,
    address: String,
    keychain: String,
    timestamp: u64,
    confirmations: u32,
    participants: Vec<String>,
    stable_value_usd: f64,
    spendable: bool,
    derivation_path: String,
    vout: u32,
    spent: bool,
}

fn get_file_lock(path: &str) -> Arc<Mutex<()>> {
    let mut locks = FILE_LOCKS.lock().unwrap();
    locks.entry(path.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn save_to_file<T: Serialize>(data: &T, file_path: &Path) -> Result<(), PulserError> {
    let temp_path = file_path.with_extension("temp");
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let lock = get_file_lock(file_path.to_str().unwrap_or("unknown"));
    let _guard = lock.lock().unwrap();
    let json = serde_json::to_string_pretty(data)?;
    fs::write(&temp_path, json)?;
    fs::rename(&temp_path, file_path)?;
    #[cfg(unix)]
    fs::set_permissions(file_path, fs::Permissions::from_mode(0o600))?;
    #[cfg(not(unix))]
    fs::set_permissions(file_path, fs::Permissions::from_mode(0o644))?;
    Ok(())
}

fn prune_activity_log(activity_path: &Path, max_entries: usize) -> Result<(), PulserError> {
    if !activity_path.exists() { return Ok(()); }
    let lock = get_file_lock(activity_path.to_str().unwrap_or("unknown"));
    let _guard = lock.lock().unwrap();
    let contents = fs::read_to_string(activity_path)?;
    let mut utxos: Vec<UtxoInfo> = serde_json::from_str(&contents)?;
    utxos.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    if utxos.len() > max_entries {
        utxos.truncate(max_entries);
        save_to_file(&utxos, activity_path)?;
        debug!("Pruned activity log to {} entries", max_entries);
    }
    Ok(())
}

fn is_user_active(user_id: &str) -> bool {
    ACTIVE_TASKS.lock().unwrap().contains_key(user_id)
}

fn mark_user_active(user_id: &str) {
    ACTIVE_TASKS.lock().unwrap().insert(user_id.to_string(), true);
}

fn mark_user_inactive(user_id: &str) {
    ACTIVE_TASKS.lock().unwrap().remove(user_id);
}

async fn notify_new_utxos(
    client: &Client,
    user_id: &str,
    new_utxos: &[UtxoInfo],
    webhook_url: &str,
    retry_queue: Arc<Mutex<VecDeque<WebhookRetry>>>,
) -> Result<(), PulserError> {
    if new_utxos.is_empty() || webhook_url.is_empty() { return Ok(()); }
    let payload = json!({
        "event": "new_utxos",
        "user_id": user_id,
        "utxos": new_utxos,
        "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    });

    let start_time = Instant::now();
    let max_duration = Duration::from_secs(DEFAULT_MAX_RETRY_TIME_SECS);

    for retry in 0..MAX_WEBHOOK_RETRIES {
        if start_time.elapsed() >= max_duration { break; }
        if retry > 0 {
            let backoff = Duration::from_millis(500 * 2u64.pow(retry));
            sleep(backoff).await;
            if start_time.elapsed() >= max_duration { break; }
        }
        match timeout(Duration::from_secs(WEBHOOK_TIMEOUT_SECS), client.post(webhook_url).json(&payload).send()).await {
            Ok(Ok(response)) if response.status().is_success() => {
                info!("Webhook sent for user {}: {} UTXOs", user_id, new_utxos.len());
                return Ok(());
            }
            Ok(Ok(response)) => warn!("Webhook failed with status: {}", response.status()),
            Ok(Err(e)) => warn!("Webhook error: {}", e),
            Err(_) => warn!("Webhook timed out"),
        }
    }

    let mut queue = retry_queue.lock().await.unwrap();
    queue.push_back(WebhookRetry {
        user_id: user_id.to_string(),
        utxos: new_utxos.to_vec(),
        attempts: 0,
        next_attempt: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 60,
    });
    warn!("Queued webhook retry for user {}: {} UTXOs", user_id, new_utxos.len());
    Err(PulserError::NetworkError("Webhook attempts failed, queued for retry".to_string()))
}

async fn sync_user(
    user_id: &str,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_info: PriceInfo,
    esplora_urls: Arc<Mutex<Vec<(String, u32)>>>,
    data_dir: String,
    webhook_url: String,
    client: Client,
    retry_queue: Arc<Mutex<VecDeque<WebhookRetry>>>,
) -> Result<bool, PulserError> {
    let start_time = Instant::now();
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let user_dir = format!("{}/user_{}", data_dir, user_id);
    fs::create_dir_all(&user_dir)?;
    let status_path = format!("{}/status_{}.json", user_dir, user_id);
    let activity_path = format!("{}/activity_{}.json", user_dir, user_id);

    let mut statuses = user_statuses.lock().await.unwrap();
    let status = statuses.entry(user_id.to_string()).or_insert_with(|| {
        info!("Initializing status for user {}", user_id);
        UserStatus {
            user_id: user_id.to_string(),
            last_sync: 0,
            sync_status: "syncing".to_string(),
            utxo_count: 0,
            total_value_btc: 0.0,
            total_value_usd: 0.0,
            confirmations_pending: false,
            last_update_message: format!("Starting sync at {}", now),
            sync_duration_ms: 0,
            last_error: None,
            last_success: 0,
            pruned_utxo_count: 0,
            current_deposit_address: "Not yet determined".to_string(),
            last_deposit_time: None,
        }
    });

    let mut activity: Vec<UtxoInfo> = if Path::new(&activity_path).exists() {
        fs::read_to_string(&activity_path)
            .and_then(|content| serde_json::from_str(&content).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)))
            .unwrap_or_else(|_| Vec::new())
    } else {
        Vec::new()
    };

    let last_sync = status.last_sync;
    let sync_interval = 24 * 3600;
    let should_full_sync = last_sync == 0 || now - last_sync > sync_interval;

    let mut wallets_lock = wallets.lock().await.unwrap();
    if !wallets_lock.contains_key(user_id) {
        let config_path = "config/service_config.toml";
        let (wallet, deposit_info, chain) = DepositWallet::from_config(config_path, user_id).await?;
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

    while attempts < DEFAULT_RETRY_MAX {
        let best_url = {
            let mut urls = esplora_urls.lock().await.unwrap();
            let (best_url, failures) = urls.iter_mut().min_by_key(|(_, f)| *f).unwrap();
            (best_url.clone(), *failures)
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
    chain.accumulated_btc = Bitcoin { sats: chain.utxos.iter().map(|u| u.amount).sum() };

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
        let new_addr = wallet.wallet.reveal_next_address(KeychainKind::External).address;
        info!("New deposit address for user {}: {}", user_id, new_addr);
        let mut logged = deposit_service::wallet::LOGGED_ADDRESSES.lock().await;
        logged.insert(user_id.to_string(), new_addr.clone());
        status.current_deposit_address = new_addr.to_string();
        status.last_deposit_time = Some(now);
    }

    activity.extend(new_utxos.clone());
    activity.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    if activity.len() > MAX_UTXO_ACTIVITY_ENTRIES {
        activity.truncate(MAX_UTXO_ACTIVITY_ENTRIES);
    }
    save_to_file(&activity, Path::new(&activity_path))?;

    if !new_utxos.is_empty() && !webhook_url.is_empty() {
        if let Err(e) = notify_new_utxos(&client, user_id, &new_utxos, &webhook_url, retry_queue.clone()).await {
            warn!("Webhook failed for user {}: {}. Queued for retry.", user_id, e);
        }
    }

    let total_stable_usd: f64 = chain.utxos.iter().map(|u| u.usd_value.as_ref().unwrap_or(&USD(0.0)).0).sum();
    status.last_sync = now;
    status.sync_status = if esplora_failed { "completed_with_cache".to_string() } else { "completed".to_string() };
    status.utxo_count = chain.utxos.len() as u32;
    status.total_value_btc = chain.accumulated_btc.sats as f64 / 100_000_000.0;
    status.total_value_usd = total_stable_usd;
    status.last_success = now;
    status.sync_duration_ms = start_time.elapsed().as_millis() as u64;
    status.last_update_message = if esplora_failed { "Sync completed with cached data".to_string() } else { "Sync completed".to_string() };
    save_to_file(&status, Path::new(&status_path))?;

    if !chain.utxos.is_empty() {
        info!("User {} sync complete: {} UTXOs, {} BTC (${:.2})", user_id, chain.utxos.len(), chain.accumulated_btc.sats as f64 / 100_000_000.0, total_stable_usd);
    } else if should_full_sync {
        info!("No deposits yet for user {} (balance: 0 BTC)", user_id);
    }

    Ok(new_funds_detected)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10))
        .pool_idle_timeout(Some(Duration::from_secs(30)))
        .build()?;

    let start_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let config_str = fs::read_to_string("config/service_config.toml")?;
    let config: toml::Value = toml::from_str(&config_str)?;

    let client_for_warp = client.clone();
    let client_for_retry = client.clone();
    let client_for_periodic = client.clone();

    let webhook_url = config.get("webhook_url").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let webhook_url_for_warp = webhook_url.clone();
    let webhook_url_for_retry = webhook_url.clone();
    let webhook_url_for_periodic = webhook_url.clone();

    let esplora_url = config.get("esplora_url").and_then(|v| v.as_str()).unwrap_or("https://blockstream.info/testnet/api").to_string();
    let fallback_url = config.get("fallback_esplora_url").and_then(|v| v.as_str()).unwrap_or("https://mempool.space/testnet/api").to_string();
    let http_port = config.get("listening_port").and_then(|v| v.as_integer()).unwrap_or(8081) as u16;
    let data_dir = config.get("data_dir").and_then(|v| v.as_str()).unwrap_or("data_lsp").to_string();
    let sync_interval_secs = config.get("sync_interval_secs").and_then(|v| v.as_integer()).unwrap_or(3600) as u64;
    let user_scan_interval_secs = config.get("user_scan_interval_secs").and_then(|v| v.as_integer()).unwrap_or(300) as u64;
    let batch_users = config.get("batch_users").and_then(|v| v.as_bool()).unwrap_or(false);
    let max_concurrent_users = config.get("max_concurrent_users").and_then(|v| v.as_integer()).unwrap_or(MAX_CONCURRENT_USERS as i64) as usize;

    let esplora_urls = Arc::new(Mutex::new(vec![
        (esplora_url.clone(), 0),
        (fallback_url.clone(), 0),
    ]));

    let service_status = Arc::new(Mutex::new(ServiceStatus {
        up_since: start_time,
        last_update: 0,
        version: env!("CARGO_PKG_VERSION").to_string(),
        users_monitored: 0,
        total_utxos: 0,
        total_value_btc: 0.0,
        total_value_usd: 0.0,
        health: "starting".to_string(),
        api_status: HashMap::new(),
        last_price: DEFAULT_PRICE_FALLBACK,
        price_update_count: 0,
        active_syncs: 0,
        price_cache_staleness_secs: 0,
        silent_failures: 0,
        api_calls: 0,
        error_rate: 0.0,
    }));

    info!("Starting deposit monitor service");
    info!("Primary API: {}", esplora_url);
    info!("Fallback API: {}", fallback_url);
    fs::create_dir_all(&data_dir)?;

    let wallets = Arc::new(Mutex::new(HashMap::<String, (DepositWallet, StableChain)>::new()));
    let user_statuses = Arc::new(Mutex::new(HashMap::<String, UserStatus>::new()));
    let (sync_tx, mut sync_rx) = mpsc::channel::<String>(100);
    let retry_queue = Arc::new(Mutex::new(VecDeque::<WebhookRetry>::new()));
    let price_info = Arc::new(Mutex::new(PriceInfo { raw_btc_usd: 0.0, timestamp: 0, price_feeds: HashMap::new() }));
    let last_activity_check = Arc::new(Mutex::new(HashMap::<String, u64>::new()));

    info!("Preloading existing users from {}", data_dir);
    for entry in fs::read_dir(&data_dir)? {
        let path = entry?.path();
        if path.is_dir() {
            let dir_name = path.file_name().unwrap().to_str().unwrap();
            if let Some(user_id) = dir_name.strip_prefix("user_") {
                let public_path = path.join(format!("user_{}_public.json", user_id));
                if public_path.exists() {
                    info!("Preloading user: {}", user_id);
                    match DepositWallet::from_config("config/service_config.toml", user_id).await {
                        Ok((wallet, deposit_info, chain)) => {
                            let mut statuses = user_statuses.lock().await.unwrap();
                            statuses.insert(user_id.to_string(), UserStatus {
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
                                current_deposit_address: deposit_info.address,
                                last_deposit_time: None,
                            });
                            wallets.lock().await.unwrap().insert(user_id.to_string(), (wallet, chain));
                            info!("Successfully preloaded wallet for user {}", user_id);
                        }
                        Err(e) => warn!("Failed to preload wallet for user {}: {}", user_id, e),
                    }
                }
            }
        }
    }
    info!("Preloaded {} users", wallets.lock().await.unwrap().len());

    let service_status_clone = service_status.clone();
    let wallets_clone = wallets.clone();
    let user_statuses_clone = user_statuses.clone();
    let data_dir_clone = data_dir.clone();
    let sync_tx_clone = sync_tx.clone();
    let retry_queue_clone = retry_queue.clone();
    let esplora_urls_clone = esplora_urls.clone();
    let price_info_clone = price_info.clone();

    tokio::spawn(async move {
        let health = warp::path("health").map({
            let service_status = service_status_clone.clone();
            move || {
                let status = service_status.lock().await.unwrap();
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
            let status_data = service_status_clone.clone();
            move || {
                let status_clone = status_data.clone();
                async move { Ok::<_, Rejection>(warp::reply::json(&status_clone.lock().await.unwrap().clone())) }
            }
        });

        let user_status = warp::path("user").and(warp::path::param::<String>()).and(warp::get()).and_then({
            let user_data = user_statuses_clone.clone();
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

        let activity = warp::path!("activity" / String).and(warp::get()).map({
            let data_dir = data_dir_clone.clone();
            move |user_id: String| {
                let activity_path = format!("{}/user_{}/activity_{}.json", data_dir, user_id, user_id);
                let activity = match fs::read_to_string(&activity_path) {
                    Ok(data) => serde_json::from_str::<Vec<UtxoInfo>>(&data)
                        .map(|utxos| json!({"utxos": utxos}))
                        .unwrap_or(json!({"utxos": []})),
                    Err(_) => json!({"utxos": []}),
                };
                warp::reply::json(&activity)
            }
        });

        let data_dir_for_sync = data_dir_clone.clone();
        let webhook_url_for_sync = webhook_url_for_warp.clone();
        let client_for_sync = client_for_warp.clone();

        let sync_route = warp::post()
            .and(warp::path!("sync" / String))
            .and(with_wallets(wallets_clone.clone()))
            .and(with_statuses(user_statuses_clone.clone()))
            .and(with_price(price_info_clone.clone()))
            .and(with_esplora_urls(esplora_urls_clone.clone()))
            .and(with_retry_queue(retry_queue_clone.clone()))
            .and(warp::any().map(move || data_dir_for_sync.clone()))
            .and(warp::any().map(move || webhook_url_for_sync.clone()))
            .and(warp::any().map(move || client_for_sync.clone()))
            .and_then(|user_id: String, wallets, statuses, price, esplora_urls, retry_queue, data_dir, webhook_url, client| {
                async move {
                    if is_user_active(&user_id) {
                        return Ok::<_, Rejection>(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
                    }
                    mark_user_active(&user_id);
                    let price_info = price.lock().await.unwrap().clone();
                    let result = sync_user(&user_id, wallets, statuses, price_info, esplora_urls, data_dir, webhook_url, client, retry_queue).await;
                    mark_user_inactive(&user_id);
                    result.map(|_| warp::reply::json(&json!({"status": "ok", "message": "Sync completed"})))
                        .map_err(warp::reject::custom)
                }
            });

        let force_sync = warp::path("force_sync")
            .and(warp::path::param::<String>())
            .and(warp::post())
            .and_then({
                let sync_sender = sync_tx_clone.clone();
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
                let data_dir = data_dir_clone.clone();
                let wallets = wallets_clone.clone();
                let service_status = service_status_clone.clone();
                let user_statuses = user_statuses_clone.clone();
                move |public_data: serde_json::Value| {
                    let data_dir = data_dir.clone();
                    let wallets = wallets.clone();
                    let service_status = service_status.clone();
                    let user_statuses = user_statuses.clone();
                    async move {
                        let user_id = public_data["user_id"].as_str().unwrap_or("unknown").to_string();
                        let user_dir = format!("{}/user_{}", data_dir, user_id);
                        fs::create_dir_all(&user_dir).map_err(|e| warp::reject::custom(CustomError::Io(e)))?;
                        let public_path = format!("{}/user_{}_public.json", user_dir, user_id);
                        let json_str = serde_json::to_string_pretty(&public_data)
                            .map_err(|e| warp::reject::custom(CustomError::Serde(e)))?;
                        fs::write(&public_path, &json_str)
                            .map_err(|e| warp::reject::custom(CustomError::Io(e)))?;
                        info!("Registered user: {}", user_id);
                        if !wallets.lock().await.unwrap().contains_key(&user_id) {
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
                                    wallets.lock().await.unwrap().insert(user_id.clone(), (wallet, chain));
                                    let mut status = service_status.lock().await.unwrap();
                                    status.users_monitored = wallets.lock().await.unwrap().len() as u32;
                                }
                                Err(e) => warn!("Failed to init wallet for {}: {}", user_id, e),
                            }
                        }
                        Ok::<_, Rejection>(warp::reply::json(&json!({"status": "registered"})))
                    }
                }
            });

        let sync_utxos = warp::path!("sync_utxos" / String).and_then({
            let wallets = wallets_clone.clone();
            let price_info = price_info_clone.clone();
            let user_statuses = user_statuses_clone.clone();
            move |user_id: String| {
                let wallets = wallets.clone();
                let price_info = price_info.clone();
                let user_statuses = user_statuses.clone();
                async move {
                    let mut status_guard = user_statuses.lock().await.unwrap();
                    let status = status_guard.entry(user_id.clone()).or_insert_with(|| UserStatus::new(&user_id));
                    let addr = match Address::from_str(&status.current_deposit_address) {
                        Ok(a) => match a.require_network(Network::Testnet) {
                            Ok(addr) => addr,
                            Err(e) => return Err(warp::reject::custom(PulserError::InvalidInput(e.to_string()))),
                        },
                        Err(e) => return Err(warp::reject::custom(PulserError::InvalidInput(e.to_string()))),
                    };
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

        let routes = health.or(status).or(user_status).or(activity).or(sync_route).or(force_sync).or(register).or(sync_utxos);
        warp::serve(routes).run(([0, 0, 0, 0], http_port)).await;
    });

    tokio::spawn({
        let client = client_for_retry;
        let retry_queue = retry_queue.clone();
        let webhook_url = webhook_url_for_retry;

        async move {
            loop {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let retry = {
                    let mut queue = retry_queue.lock().await.unwrap();
                    queue.pop_front()
                };
                if let Some(retry) = retry {
                    if retry.next_attempt > now {
                        let mut queue = retry_queue.lock().await.unwrap();
                        queue.push_front(retry);
                    } else if retry.attempts >= DEFAULT_RETRY_MAX {
                        error!("Webhook for user {} failed after {} retries", retry.user_id, DEFAULT_RETRY_MAX);
                    } else {
                        let next_retry = WebhookRetry {
                            user_id: retry.user_id.clone(),
                            utxos: retry.utxos.clone(),
                            attempts: retry.attempts + 1,
                            next_attempt: now + (60 * 2u64.pow(retry.attempts)),
                        };
                        match notify_new_utxos(&client, &retry.user_id, &retry.utxos, &webhook_url, retry_queue.clone()).await {
                            Ok(_) => info!("Webhook retry succeeded for user {}", retry.user_id),
                            Err(_) => {
                                let mut queue = retry_queue.lock().await.unwrap();
                                queue.push_back(next_retry);
                                warn!("Webhook retry failed for user {}, queued again", retry.user_id);
                            }
                        }
                    }
                }
                sleep(Duration::from_secs(30)).await;
            }
        }
    });

    tokio::spawn({
        let wallets = wallets.clone();
        let user_statuses = user_statuses.clone();
        let last_activity_check = last_activity_check.clone();
        let data_dir = data_dir.clone();
        let sync_tx = sync_tx.clone();
        let service_status = service_status.clone();
        let esplora_urls = esplora_urls.clone();
        let retry_queue = retry_queue.clone();
        let client = client_for_periodic;
        let webhook_url = webhook_url_for_periodic;
        let price_info = price_info.clone();
        async move {
            loop {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let price_info_data = price_info.lock().await.unwrap().clone();
                let mut api_calls = 0;
                let mut errors = 0;

                {
                    let wallets_lock = wallets.lock().await.unwrap();
                    let mut statuses_lock = user_statuses.lock().await.unwrap();
                    let last_check_lock = last_activity_check.lock().await.unwrap();

                    for (user_id, wallet_chain) in wallets_lock.iter() {
                        let (wallet, chain) = wallet_chain;
                        let status = statuses_lock.get_mut(user_id).unwrap();
                        let last_deposit = status.last_deposit_time.unwrap_or(0);
                        let last_check = *last_check_lock.get(user_id).unwrap_or(&0);

                        if now - last_deposit < 24 * 3600 && now - last_check >= 3600 {
                            api_calls += 1;
                            let addr = match Address::from_str(&status.current_deposit_address) {
                                Ok(a) => match a.require_network(Network::Testnet) {
                                    Ok(addr) => addr,
                                    Err(e) => {
                                        errors += 1;
                                        warn!("Invalid network for address for user {}: {}", user_id, e);
                                        continue;
                                    }
                                },
                                Err(e) => {
                                    errors += 1;
                                    warn!("Invalid address for user {}: {}", user_id, e);
                                    continue;
                                }
                            };
                            match wallet.check_address(&addr, &price_info_data).await {
                                Ok(utxos) => {
                                    if utxos.iter().any(|u| !chain.utxos.iter().any(|c| c.txid == u.txid)) {
                                        info!("Pending deposit detected for user {}: {} UTXOs", user_id, utxos.len());
                                        sync_tx.send(user_id.clone()).await.ok();
                                    }
                                }
                                Err(e) => {
                                    errors += 1;
                                    warn!("Periodic check failed for user {}: {}", user_id, e);
                                }
                            }
                            let mut last_check_lock = last_activity_check.lock().await.unwrap();
                            last_check_lock.insert(user_id.clone(), now);
                        }
                    }
                }

                let mut status = service_status.lock().await.unwrap();
                status.api_calls += api_calls;
                status.error_rate = if api_calls > 0 { errors as f64 / api_calls as f64 } else { 0.0 };
                drop(status);

                sleep(Duration::from_secs(60)).await;
            }
        }
    });

    let mut last_user_scan = start_time;
    let (shutdown_tx, mut shutdown_rx) = broadcast::channel::<()>(1);

    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate())?;
            let mut sigint = signal(SignalKind::interrupt())?;
            tokio::select! {
                _ = sigterm.recv() => info!("Received SIGTERM, shutting down gracefully"),
                _ = sigint.recv() => info!("Received SIGINT, shutting down gracefully"),
            }
        }
        #[cfg(windows)]
        {
            use tokio::signal::windows;
            let mut ctrl_c = windows::ctrl_c()?;
            let mut ctrl_break = windows::ctrl_break()?;
            tokio::select! {
                _ = ctrl_c.recv() => info!("Received Ctrl+C, shutting down gracefully"),
                _ = ctrl_break.recv() => info!("Received Ctrl+Break, shutting down gracefully"),
            }
        }
        let _ = shutdown_tx.send(());
    });

    loop {
        if let Ok(()) = shutdown_rx.try_recv() {
            info!("Shutdown signal received, exiting");
            break;
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let price_info_data = fetch_btc_usd_price(&client).await.unwrap_or_else(|e| {
            error!("Failed to fetch price: {}, using default", e);
            let mut status = service_status.lock().await.unwrap();
            status.api_status.insert("price_feed".to_string(), false);
            status.last_update = now;
            status.price_cache_staleness_secs = DEFAULT_CACHE_DURATION_SECS;
            PriceInfo {
                raw_btc_usd: DEFAULT_PRICE_FALLBACK,
                timestamp: now_timestamp(),
                price_feeds: HashMap::new(),
            }
        });

        {
            let mut status = service_status.lock().await.unwrap();
            status.last_price = price_info_data.raw_btc_usd;
            status.api_status.insert("price_feed".to_string(), true);
            status.last_update = now;
            status.price_update_count += 1;
            status.price_cache_staleness_secs = if common::price_feed::is_price_cache_stale() {
                DEFAULT_CACHE_DURATION_SECS
            } else {
                0
            };
        }

        let mut forced_users: Vec<String> = Vec::new();
        while let Ok(user_id) = sync_rx.try_recv() {
            info!("Forced sync requested for user {}", user_id);
            forced_users.push(user_id);
        }
        let mut api_calls = 0;
        let mut errors = 0;

        for user_id in forced_users {
            if is_user_active(&user_id) {
                warn!("Skipping forced sync for user {} - already processing", user_id);
                continue;
            }
            mark_user_active(&user_id);
            {
                let mut status = service_status.lock().await.unwrap();
                status.active_syncs += 1;
            }
            api_calls += 1;

            let sync_result = sync_user(
                &user_id,
                wallets.clone(),
                user_statuses.clone(),
                price_info_data.clone(),
                esplora_urls.clone(),
                data_dir.clone(),
                webhook_url.clone(),
                client.clone(),
                retry_queue.clone(),
            ).await;
            let previous_utxos = wallets.lock().await.unwrap().get(&user_id).map(|(_, chain)| chain.utxos.len()).unwrap_or(0);

            match sync_result {
                Ok(new_funds_detected) => {
                    let mut status = service_status.lock().await.unwrap();
                    status.active_syncs = status.active_syncs.saturating_sub(1);
                    let current_utxos = wallets.lock().await.unwrap().get(&user_id).map(|(_, chain)| chain.utxos.len()).unwrap_or(0);
                    if !new_funds_detected && current_utxos == previous_utxos {
                        status.silent_failures += 1;
                    }
                }
                Err(e) => {
                    let mut status = service_status.lock().await.unwrap();
                    status.active_syncs = status.active_syncs.saturating_sub(1);
                    errors += 1;
                    warn!("Forced sync failed for user {}: {}", user_id, e);
                }
            }
            mark_user_inactive(&user_id);
        }

        if now - last_user_scan >= user_scan_interval_secs {
            info!("Scanning for new users...");
            for entry in fs::read_dir(&data_dir)? {
                let path = entry?.path();
                if path.extension().map_or(false, |e| e == "json")
                    && path.to_str().unwrap().contains("user_")
                    && path.to_str().unwrap().contains("_public")
                {
                    let file_name = path.file_stem().unwrap().to_str().unwrap();
                    if let Some(user_id) = file_name.strip_prefix("user_").and_then(|s| s.split('_').next()) {
                        if wallets.lock().await.unwrap().contains_key(user_id) {
                            continue;
                        }
                        info!("Found new user: {}", user_id);
                        let mut user_statuses_lock = user_statuses.lock().await.unwrap();
                        user_statuses_lock.insert(
                            user_id.to_string(),
                            UserStatus {
                                user_id: user_id.to_string(),
                                last_sync: 0,
                                sync_status: "initializing".to_string(),
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
                            },
                        );
                        match DepositWallet::from_config("config/service_config.toml", user_id).await {
                            Ok((wallet, deposit_info, chain)) => {
                                user_statuses_lock.get_mut(user_id).unwrap().current_deposit_address = deposit_info.address;
                                wallets.lock().await.unwrap().insert(user_id.to_string(), (wallet, chain));
                                let status_path = PathBuf::from(format!("{}/user_{}/status_{}.json", data_dir, user_id, user_id));
                                save_to_file(&user_statuses_lock[user_id], &status_path)?;
                            }
                            Err(e) => warn!("Failed to init wallet for user {}: {}", user_id, e),
                        }
                        sync_tx.send(user_id.to_string()).await?;
                    }
                }
            }
            let mut service_status_lock = service_status.lock().await.unwrap();
            service_status_lock.users_monitored = wallets.lock().await.unwrap().len() as u32;
            service_status_lock.last_update = now;
            last_user_scan = now;
            info!("User scan complete. Monitoring {} users.", wallets.lock().await.unwrap().len());
        }

        let user_ids: Vec<String> = wallets.lock().await.unwrap().keys().filter(|user_id| !is_user_active(user_id)).cloned().collect();
        if !user_ids.is_empty() {
            debug!("Monitoring {} users", user_ids.len());
        }

        if batch_users {
            for chunk in user_ids.chunks(max_concurrent_users) {
                for user_id in chunk {
                    if is_user_active(user_id) {
                        continue;
                    }
                    mark_user_active(user_id);
                    let mut status = service_status.lock().await.unwrap();
                    status.active_syncs += 1;
                    api_calls += 1;
                    let previous_utxos = wallets.lock().await.unwrap().get(user_id).map(|(_, chain)| chain.utxos.len()).unwrap_or(0);
                    let sync_result = sync_user(
                        user_id,
                        wallets.clone(),
                        user_statuses.clone(),
                        price_info_data.clone(),
                        esplora_urls.clone(),
                        data_dir.clone(),
                        webhook_url.clone(),
                        client.clone(),
                        retry_queue.clone(),
                    ).await;
                    match sync_result {
                        Ok(new_funds_detected) => {
                            status.active_syncs = status.active_syncs.saturating_sub(1);
                            let current_utxos = wallets.lock().await.unwrap().get(user_id).map(|(_, chain)| chain.utxos.len()).unwrap_or(0);
                            if !new_funds_detected && current_utxos == previous_utxos {
                                status.silent_failures += 1;
                            }
                        }
                        Err(e) => {
                            status.active_syncs = status.active_syncs.saturating_sub(1);
                            errors += 1;
                            warn!("Sync failed for user {}: {}", user_id, e);
                        }
                    }
                    mark_user_inactive(user_id);
                }
            }
        } else {
            for user_id in user_ids.iter() {
                if is_user_active(user_id) {
                    continue;
                }
                mark_user_active(user_id);
                let mut status = service_status.lock().await.unwrap();
                status.active_syncs += 1;
                api_calls += 1;
                let previous_utxos = wallets.lock().await.unwrap().get(user_id).map(|(_, chain)| chain.utxos.len()).unwrap_or(0);
                let sync_result = sync_user(
                    user_id,
                    wallets.clone(),
                    user_statuses.clone(),
                    price_info_data.clone(),
                    esplora_urls.clone(),
                    data_dir.clone(),
                    webhook_url.clone(),
                    client.clone(),
                    retry_queue.clone(),
                ).await;
                match sync_result {
                    Ok(new_funds_detected) => {
                        status.active_syncs = status.active_syncs.saturating_sub(1);
                        let current_utxos = wallets.lock().await.unwrap().get(user_id).map(|(_, chain)| chain.utxos.len()).unwrap_or(0);
                        if !new_funds_detected && current_utxos == previous_utxos {
                            status.silent_failures += 1;
                        }
                    }
                    Err(e) => {
                        status.active_syncs = status.active_syncs.saturating_sub(1);
                        errors += 1;
                        warn!("Sync failed for user {}: {}", user_id, e);
                    }
                }
                mark_user_inactive(user_id);
            }
        }

        {
            let wallets_lock = wallets.lock().await.unwrap();
            let mut total_utxos = 0;
            let mut total_value_btc = 0.0;
            let mut total_value_usd = 0.0;
            for (_, (_, chain)) in wallets_lock.iter() {
                total_utxos += chain.utxos.len() as u32;
                total_value_btc += chain.accumulated_btc.sats as f64 / 100_000_000.0;
                total_value_usd += chain.utxos.iter().map(|u| u.usd_value.as_ref().unwrap_or(&USD(0.0)).0).sum::<f64>();
            }
            let mut status = service_status.lock().await.unwrap();
            status.total_utxos = total_utxos;
            status.total_value_btc = total_value_btc;
            status.total_value_usd = total_value_usd;
            status.last_update = now;
            status.health = "healthy".to_string();
            status.api_calls += api_calls;
            status.error_rate = if api_calls > 0 { errors as f64 / api_calls as f64 } else { 0.0 };
        }

        debug!("Completed cycle, sleeping for {} seconds", sync_interval_secs);
        sleep(Duration::from_secs(sync_interval_secs)).await;
    }

    info!("Service shutting down");
    Ok(())
}

// Warp filter helpers
fn with_wallets(wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>) -> impl Filter<Extract = (Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || wallets.clone())
}

fn with_statuses(statuses: Arc<Mutex<HashMap<String, UserStatus>>>) -> impl Filter<Extract = (Arc<Mutex<HashMap<String, UserStatus>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || statuses.clone())
}

fn with_price(price: Arc<Mutex<PriceInfo>>) -> impl Filter<Extract = (Arc<Mutex<PriceInfo>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || price.clone())
}

fn with_esplora_urls(urls: Arc<Mutex<Vec<(String, u32)>>>) -> impl Filter<Extract = (Arc<Mutex<Vec<(String, u32)>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || urls.clone())
}

fn with_retry_queue(queue: Arc<Mutex<VecDeque<WebhookRetry>>>) -> impl Filter<Extract = (Arc<Mutex<VecDeque<WebhookRetry>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || queue.clone())
}

impl UserStatus {
    fn new(user_id: &str) -> Self {
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
