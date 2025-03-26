// deposit-service/src/main.rs
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, broadcast};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::fs;
use std::collections::{HashMap, VecDeque};
use log::{info, warn, error, debug};
use lazy_static::lazy_static;
use reqwest::Client;
use common::error::PulserError;
use common::price_feed::{fetch_btc_usd_price, PriceInfo};
use deposit_service::wallet::DepositWallet;
use deposit_service::types::{StableChain, UserStatus, ServiceStatus, WebhookRetry};
use deposit_service::storage::StateManager;
use deposit_service::webhook::{WebhookConfig, start_retry_task};
use deposit_service::monitor::{MonitorConfig, start_periodic_monitor};
use deposit_service::api;

lazy_static! {
    static ref ACTIVE_TASKS: Arc<Mutex<HashMap<String, bool>>> = Arc::new(Mutex::new(HashMap::new()));
}

pub fn is_user_active(user_id: &str) -> bool {
    ACTIVE_TASKS.lock().unwrap().contains_key(user_id)
}

pub fn mark_user_active(user_id: &str) {
    ACTIVE_TASKS.lock().unwrap().insert(user_id.to_string(), true);
}

pub fn mark_user_inactive(user_id: &str) {
    ACTIVE_TASKS.lock().unwrap().remove(user_id);
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

    let webhook_url = config.get("webhook_url").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let esplora_url = config.get("esplora_url").and_then(|v| v.as_str()).unwrap_or("https://blockstream.info/testnet/api").to_string();
    let fallback_url = config.get("fallback_esplora_url").and_then(|v| v.as_str()).unwrap_or("https://mempool.space/testnet/api").to_string();
    let http_port = config.get("listening_port").and_then(|v| v.as_integer()).unwrap_or(8081) as u16;
    let data_dir = config.get("data_dir").and_then(|v| v.as_str()).unwrap_or("data_lsp").to_string();
    let sync_interval_secs = config.get("sync_interval_secs").and_then(|v| v.as_integer()).unwrap_or(3600) as u64;
    let max_concurrent_users = config.get("max_concurrent_users").and_then(|v| v.as_integer()).unwrap_or(10) as usize;

    let esplora_urls = Arc::new(Mutex::new(vec![
        (esplora_url.clone(), 0),
        (fallback_url.clone(), 0),
    ]));
    let state_manager = Arc::new(StateManager::new(&data_dir));
    let wallets = Arc::new(Mutex::new(HashMap::<String, (DepositWallet, StableChain)>::new()));
    let user_statuses = Arc::new(Mutex::new(HashMap::<String, UserStatus>::new()));
    let retry_queue = Arc::new(Mutex::new(VecDeque::<WebhookRetry>::new()));
    let price_info = Arc::new(Mutex::new(PriceInfo { raw_btc_usd: 0.0, timestamp: 0, price_feeds: HashMap::new() }));
    let last_activity_check = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let (sync_tx, sync_rx) = mpsc::channel::<String>(100);
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
        last_price: 0.0,
        price_update_count: 0,
        active_syncs: 0,
        price_cache_staleness_secs: 0,
        silent_failures: 0,
        api_calls: 0,
        error_rate: 0.0,
    }));

    info!("Starting deposit monitor service");
    fs::create_dir_all(&data_dir)?;

    // Preload existing users
    info!("Preloading existing users from {}", data_dir);
    for entry in fs::read_dir(&data_dir)? {
        let path = entry?.path();
        if path.is_dir() {
            let dir_name = path.file_name().unwrap().to_str().unwrap();
            if let Some(user_id) = dir_name.strip_prefix("user_") {
                let public_path = path.join(format!("user_{}_public.json", user_id));
                let status_path = path.join(format!("status_{}.json", user_id));
                if public_path.exists() {
                    info!("Preloading user: {}", user_id);
                    match DepositWallet::from_config("config/service_config.toml", user_id, &state_manager).await {
                        Ok((wallet, deposit_info, chain)) => {
                            let mut statuses = user_statuses.lock().await.unwrap();
                            let status = if status_path.exists() {
                                state_manager.load::<UserStatus>(&status_path).await.unwrap_or_else(|e| {
                                    warn!("Failed to load status for user {}: {}", user_id, e);
                                    UserStatus::new(user_id)
                                })
                            } else {
                                let mut s = UserStatus::new(user_id);
                                s.current_deposit_address = deposit_info.address.clone();
                                s
                            };
                            statuses.insert(user_id.to_string(), status);
                            wallets.lock().await.unwrap().insert(user_id.to_string(), (wallet, chain));
                            info!("Successfully preloaded wallet for user {}", user_id);
                        }
                        Err(e) => warn!("Failed to preload wallet for user {}: {}", user_id, e),
                    }
                }
            }
        }
    }
    {
        let mut status = service_status.lock().await.unwrap();
        status.users_monitored = wallets.lock().await.unwrap().len() as u32;
    }
    info!("Preloaded {} users", wallets.lock().await.unwrap().len());

    // Signal handler
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("Failed to bind SIGTERM");
            let mut sigint = signal(SignalKind::interrupt()).expect("Failed to bind SIGINT");
            tokio::select! {
                _ = sigterm.recv() => info!("Received SIGTERM, shutting down gracefully"),
                _ = sigint.recv() => info!("Received SIGINT, shutting down gracefully"),
            }
        }
        #[cfg(windows)]
        {
            use tokio::signal::windows;
            let mut ctrl_c = windows::ctrl_c().expect("Failed to bind Ctrl+C");
            let mut ctrl_break = windows::ctrl_break().expect("Failed to bind Ctrl+Break");
            tokio::select! {
                _ = ctrl_c.recv() => info!("Received Ctrl+C, shutting down gracefully"),
                _ = ctrl_break.recv() => info!("Received Ctrl+Break, shutting down gracefully"),
            }
        }
        let _ = shutdown_tx.send(());
    });

    // PriceUpdater task
    let price_handle = tokio::spawn({
        let price_info = price_info.clone();
        let service_status = service_status.clone();
        let client = client.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("PriceUpdater shutting down");
                        break;
                    }
                    _ = sleep(Duration::from_secs(60)) => {
                        match fetch_btc_usd_price(&client).await {
                            Ok(new_price) => {
                                let mut price = price_info.lock().await.unwrap();
                                *price = new_price;
                                let mut status = service_status.lock().await.unwrap();
                                status.last_price = price.raw_btc_usd;
                                status.price_update_count += 1;
                                status.price_cache_staleness_secs = 0;
                                debug!("Price updated: ${}", price.raw_btc_usd);
                            }
                            Err(e) => {
                                warn!("Price update failed: {}", e);
                                let mut status = service_status.lock().await.unwrap();
                                status.price_cache_staleness_secs += 60;
                            }
                        }
                    }
                }
            }
            Ok::<(), PulserError>(())
        }
    });

    // Spawn other tasks
    let webhook_config = WebhookConfig::from_toml(&config);
    let retry_handle = tokio::spawn(start_retry_task(
        client.clone(),
        retry_queue.clone(),
        webhook_url.clone(),
        webhook_config.clone(),
        shutdown_rx.clone(),
    ));

    let monitor_config = MonitorConfig::from_toml(&config);
    let monitor_handle = tokio::spawn(start_periodic_monitor(
        wallets.clone(),
        user_statuses.clone(),
        last_activity_check.clone(),
        sync_tx.clone(),
        price_info.clone(),
        monitor_config,
        shutdown_rx.clone(),
    ));

    let routes = api::routes(
        service_status.clone(),
        wallets.clone(),
        user_statuses.clone(),
        price_info.clone(),
        esplora_urls.clone(),
        state_manager.clone(),
        sync_tx.clone(),
        retry_queue.clone(),
        webhook_url.clone(),
        webhook_config.clone(),
        client.clone(),
    );
    let server_handle = tokio::spawn(warp::serve(routes).run(([0, 0, 0, 0], http_port)));

    // SyncProcessor task with batch syncs
    let sync_handle = tokio::spawn({
        let wallets = wallets.clone();
        let user_statuses = user_statuses.clone();
        let price_info = price_info.clone();
        let esplora_urls = esplora_urls.clone();
        let state_manager = state_manager.clone();
        let retry_queue = retry_queue.clone();
        let webhook_url = webhook_url.clone();
        let webhook_config = webhook_config.clone();
        let client = client.clone();
        let mut sync_rx = sync_rx;
        let mut shutdown_rx = shutdown_rx.clone();
        let service_status = service_status.clone();

        async move {
            let mut pending_users = Vec::new();
            loop {
                tokio::select! {
                    Some(user_id) = sync_rx.recv() => {
                        pending_users.push(user_id);
                    }
                    _ = shutdown_rx.recv() => {
                        info!("SyncProcessor shutting down");
                        break;
                    }
                    _ = sleep(Duration::from_secs(5)), if !pending_users.is_empty() => {
                        info!("Processing batch sync for {} users", pending_users.len());
                        for chunk in pending_users.chunks(max_concurrent_users) {
                            let mut api_calls = 0;
                            let mut errors = 0;
                            for user_id in chunk {
                                if is_user_active(user_id) {
                                    warn!("Skipping sync for user {} - already processing", user_id);
                                    continue;
                                }
                                mark_user_active(user_id);
                                {
                                    let mut status = service_status.lock().await.unwrap();
                                    status.active_syncs += 1;
                                    api_calls += 1;
                                }
                                let price_info_clone = price_info.lock().await.unwrap().clone();
                                let result = api::sync_user(
                                    user_id,
                                    wallets.clone(),
                                    user_statuses.clone(),
                                    price_info_clone,
                                    esplora_urls.clone(),
                                    state_manager.clone(),
                                    retry_queue.clone(),
                                    &webhook_url,
                                    &webhook_config,
                                    client.clone(),
                                ).await;
                                {
                                    let mut status = service_status.lock().await.unwrap();
                                    status.active_syncs = status.active_syncs.saturating_sub(1);
                                    if result.is_err() {
                                        errors += 1;
                                    }
                                    status.api_calls += api_calls;
                                    status.error_rate = if api_calls > 0 { errors as f64 / api_calls as f64 } else { 0.0 };
                                }
                                mark_user_inactive(user_id);
                            }
                            // Throttle to avoid Esplora rate limits (e.g., 10 req/s)
                            sleep(Duration::from_millis(1000)).await;
                        }
                        pending_users.clear();
                    }
                }
            }
            Ok::<(), PulserError>(())
        }
    });

    // Main loop (totals update only)
    let mut shutdown_rx = shutdown_rx.clone();
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Main loop shutting down");
                break;
            }
            _ = sleep(Duration::from_secs(sync_interval_secs)) => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                let wallets_lock = wallets.lock().await.unwrap();
                let mut total_utxos = 0;
                let mut total_value_btc = 0.0;
                let mut total_value_usd = 0.0;
                for (_, (_, chain)) in wallets_lock.iter() {
                    total_utxos += chain.utxos.len() as u32;
                    total_value_btc += chain.accumulated_btc.to_btc();
                    total_value_usd += chain.stabilized_usd.0;
                }
                let mut status = service_status.lock().await.unwrap();
                status.total_utxos = total_utxos;
                status.total_value_btc = total_value_btc;
                status.total_value_usd = total_value_usd;
                status.health = "healthy".to_string();
                status.last_update = now;
                debug!("Updated totals: {} utxos, {} BTC, ${}", total_utxos, total_value_btc, total_value_usd);
            }
        }
    }

    // Shutdown cleanup
    info!("Initiating shutdown cleanup");
    let wallets_lock = wallets.lock().await.unwrap();
    for (user_id, (_, chain)) in wallets_lock.iter() {
        if let Err(e) = state_manager.save_stable_chain(user_id, chain).await {
            error!("Failed to save StableChain for user {}: {}", user_id, e);
        }
    }
    if let Err(e) = state_manager.save(&PathBuf::from("service_status.json"), &*service_status.lock().await.unwrap()).await {
        error!("Failed to save service status: {}", e);
    }

    // Await task completion
    let _ = tokio::join!(price_handle, retry_handle, monitor_handle, server_handle, sync_handle);
    info!("Service shutdown complete");
    Ok(())
}
