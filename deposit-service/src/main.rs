use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, broadcast};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::fs;
use std::collections::{HashMap, VecDeque};
use log::{info, warn, debug, error};
use reqwest::Client;
use common::error::PulserError;
use common::price_feed::{fetch_btc_usd_price, PriceFeed};
use common::types::PriceInfo;
use deposit_service::wallet::DepositWallet;
use common::{StableChain, UserStatus, ServiceStatus, WebhookRetry};
use deposit_service::webhook::{WebhookConfig, start_retry_task};
use deposit_service::monitor::{MonitorConfig, monitor_deposits};
use deposit_service::api;
use common::task_manager::UserTaskLock; // Fixed import
use deposit_service::wallet_init::Config;
use tokio::time::sleep;
use std::path::PathBuf;
use futures::future::join_all;
use log::LevelFilter;
use bdk_esplora::esplora_client;
use common::StateManager;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_str = fs::read_to_string("config/service_config.toml")?;
    let config: Config = toml::from_str(&config_str)?; // Single parse
    let config_value: toml::Value = toml::from_str(&config_str)?;

    env_logger::Builder::new()
        .filter_level(match config_value.get("log_level").and_then(|v| v.as_str()).unwrap_or("info").to_lowercase().as_str() {
            "error" => LevelFilter::Error,
            "warn" => LevelFilter::Warn,
            "info" => LevelFilter::Info,
            "debug" => LevelFilter::Debug,
            "trace" => LevelFilter::Trace,
            _ => LevelFilter::Info,
        })
        .filter_module("hyper", LevelFilter::Warn)
        .filter_module("reqwest", LevelFilter::Warn)
        .filter_module("tokio", LevelFilter::Warn)
        .init();

    info!("Starting Pulser Deposit Service");

    let esplora_url = config.esplora_url.clone();
    let fallback_url = config.fallback_esplora_url.clone();
    let blockchain = Arc::new(esplora_client::Builder::new(&esplora_url).build_async()?);
    let active_tasks_manager = Arc::new(UserTaskLock::new());
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10))
        .pool_idle_timeout(Some(Duration::from_secs(30)))
        .build()?;
    let start_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let webhook_url = config.webhook_url.clone();
    let http_port = config.listening_port;
    let data_dir = config.data_dir.clone();
    let sync_interval_secs = config.sync_interval_secs;
    let max_concurrent_users = config.max_concurrent_users;

    let esplora_urls = Arc::new(Mutex::new(vec![(esplora_url, 0), (fallback_url, 0)]));
    let state_manager = Arc::new(StateManager::new(&data_dir));

    let wallets = Arc::new(Mutex::new(HashMap::<String, (DepositWallet, StableChain)>::new()));
    let user_statuses = Arc::new(Mutex::new(HashMap::<String, UserStatus>::new()));
    let retry_queue = Arc::new(Mutex::new(VecDeque::<WebhookRetry>::new()));
    let price_feed = Arc::new(PriceFeed::new());
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    tokio::spawn({
        let price_feed = price_feed.clone();
        let mut shutdown_rx = shutdown_rx.resubscribe();
        async move {
            if let Err(e) = price_feed.start_deribit_feed(&mut shutdown_rx).await {
                warn!("Deribit feed failed to start: {}", e);
            }
        }
    });

    let price_info = Arc::new(Mutex::new(PriceInfo { raw_btc_usd: 0.0, timestamp: 0, price_feeds: HashMap::new() }));
    let last_activity_check = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let (sync_tx, sync_rx) = mpsc::channel::<String>(1000);
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
        users: HashMap::new(),
        websocket_active: true,
    }));

    fs::create_dir_all(&data_dir)?;

    info!("Preloading existing users from {}", data_dir);
    for entry in fs::read_dir(&data_dir)? {
        let path = entry?.path();
        if path.is_dir() {
            if let Some(dir_name) = path.file_name().and_then(|f| f.to_str()) {
                if let Some(user_id) = dir_name.strip_prefix("user_") {
                    let public_path = path.join(format!("user_{}_public.json", user_id));
                    let status_path = path.join(format!("status_{}.json", user_id));
                    if public_path.exists() {
                        info!("Preloading user: {}", user_id);
                        match DepositWallet::from_config("config/service_config.toml", user_id, &state_manager, price_feed.clone()).await {
                            Ok((mut wallet, deposit_info, chain)) => {
                                if let Ok(changeset) = state_manager.load_changeset(user_id).await {
                                    wallet.wallet.apply_update(changeset.into())?;
                                }
                                let mut statuses = user_statuses.lock().await;
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
                                wallets.lock().await.insert(user_id.to_string(), (wallet, chain));
                            }
                            Err(e) => error!("Failed to preload wallet for user {}: {}", user_id, e),
                        }
                    }
                }
            }
        }
    }
    {
        let mut status = service_status.lock().await;
        status.users_monitored = wallets.lock().await.len() as u32;
    }
    info!("Preloaded {} users", wallets.lock().await.len());

    let shutdown_tx_clone = shutdown_tx.clone();
tokio::spawn(async move {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(e) => {
                warn!("Failed to bind SIGTERM: {}", e);
                return;
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(signal) => signal,
            Err(e) => {
                warn!("Failed to bind SIGINT: {}", e);
                return;
            }
        };
        
        tokio::select! {
            _ = sigterm.recv() => info!("Received SIGTERM, shutting down gracefully"),
            _ = sigint.recv() => info!("Received SIGINT, shutting down gracefully"),
        }
        let _ = shutdown_tx_clone.send(());
    }
    
    #[cfg(windows)]
    {
        use tokio::signal::windows;
        let mut ctrl_c = match windows::ctrl_c() {
            Ok(signal) => signal,
            Err(e) => {
                warn!("Failed to bind Ctrl+C: {}", e);
                return;
            }
        };
        let mut ctrl_break = match windows::ctrl_break() {
            Ok(signal) => signal,
            Err(e) => {
                warn!("Failed to bind Ctrl+Break: {}", e);
                return;
            }
        };
        
        tokio::select! {
            _ = ctrl_c.recv() => info!("Received Ctrl+C, shutting down gracefully"),
            _ = ctrl_break.recv() => info!("Received Ctrl+Break, shutting down gracefully"),
        }
        let _ = shutdown_tx_clone.send(());
    }
});

    let price_handle = tokio::spawn({
        let price_info = price_info.clone();
        let service_status = service_status.clone();
        let client = client.clone();
        let price_feed = price_feed.clone();
        let mut shutdown_rx = shutdown_rx.resubscribe();
        async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => break,
                    _ = sleep(Duration::from_secs(21)) => {
                        match fetch_btc_usd_price(&client, &price_feed).await {
                            Ok(new_price) => {
                                let mut price = price_info.lock().await;
                                *price = new_price;
                                let mut status = service_status.lock().await;
                                status.last_price = price.raw_btc_usd;
                                status.price_update_count += 1;
                                status.price_cache_staleness_secs = 0;
                            }
                            Err(e) => {
                                warn!("Price update failed: {}", e);
                                let mut status = service_status.lock().await;
                                status.price_cache_staleness_secs += 21;
                            }
                        }
                    }
                }
            }
            Ok::<(), PulserError>(())
        }
    });

    let webhook_config = WebhookConfig::from_toml(&config_value);
    let retry_handle = tokio::spawn(start_retry_task(
        client.clone(),
        retry_queue.clone(),
        webhook_url.clone(),
        webhook_config.clone(),
        shutdown_rx.resubscribe(),
        state_manager.clone(),
    ));

    let monitor_config = MonitorConfig::from_toml(&config_value);
    let monitor_handle = tokio::spawn(monitor_deposits(
        wallets.clone(),
        user_statuses.clone(),
        last_activity_check.clone(),
        sync_tx.clone(),
        price_info.clone(),
        monitor_config,
        shutdown_rx.resubscribe(),
        price_feed.clone(),
        client.clone(),
        blockchain.clone(),
        state_manager.clone(),
        service_status.clone(),
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
        active_tasks_manager.clone(),
        price_feed.clone(),
        blockchain.clone(),
    );

    let listening_address = config.listening_address.parse::<std::net::IpAddr>()?; // Moved outside async
    let mut shutdown_rx_server = shutdown_rx.resubscribe();
    let server_handle = tokio::spawn(async move {
        warp::serve(routes)
            .bind_with_graceful_shutdown((listening_address, http_port), async move {
                shutdown_rx_server.recv().await.ok();
                info!("Server shutting down");
            })
            .1
            .await
    });

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
        let mut shutdown_rx = shutdown_rx.resubscribe();
        let service_status = service_status.clone();
        let active_tasks_manager = active_tasks_manager.clone();
        let price_feed = price_feed.clone();
        let blockchain = blockchain.clone();

        async move {
            let mut pending_users = Vec::new();
            loop {
                tokio::select! {
                    user_id_option = sync_rx.recv() => {
                        if let Some(user_id) = user_id_option {
                            debug!("Received sync request for user {}", user_id);
                            pending_users.push(user_id);
                        } else {
                            warn!("Sync channel closed unexpectedly");
                            break;
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("SyncProcessor shutting down");
                        break;
                    }
                    _ = sleep(Duration::from_millis(100)), if !pending_users.is_empty() => {
                        for chunk in pending_users.chunks(max_concurrent_users) {
                            for user_id in chunk {
                                if active_tasks_manager.is_user_active(user_id).await {
                                    warn!("Skipping sync for user {} - already active", user_id);
                                    continue;
                                }
                                active_tasks_manager.mark_user_active(user_id).await;
                                {
                                    let mut status = service_status.lock().await;
                                    status.active_syncs += 1;
                                }
                                let result = api::sync_user(
                                    user_id,
                                    wallets.clone(),
                                    user_statuses.clone(),
                                    price_info.clone(), // Pass Arc directly
                                    price_feed.clone(),
                                    esplora_urls.clone(),
                                    &blockchain,
                                    state_manager.clone(),
                                    retry_queue.clone(),
                                    &webhook_url,
                                    &webhook_config,
                                    client.clone(),
                                ).await;

                                let sc_path = PathBuf::from(format!("user_{}/stable_chain_{}.json", user_id, user_id));
                                let sc_full_path = state_manager.data_dir.join(&sc_path);
                                if sc_full_path.exists() {
                                    info!("StableChain file exists for user {} after sync", user_id);
                                } else {
                                    warn!("StableChain file not found for user {} after sync at {}", user_id, sc_full_path.display());
                                }

                                {
                                    let mut status = service_status.lock().await;
                                    status.active_syncs = status.active_syncs.saturating_sub(1);
                                    if let Err(e) = &result {
                                        warn!("Sync failed for user {}: {}", user_id, e);
                                    } else {
                                        info!("Sync completed successfully for user {}", user_id);
                                    }
                                }
                                active_tasks_manager.mark_user_inactive(user_id).await;
                            }
                            sleep(Duration::from_millis(1000)).await;
                        }
                        pending_users.clear();
                    }
                }
            }
            Ok::<(), PulserError>(())
        }
    });

    let mut shutdown_rx = shutdown_rx.resubscribe();
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => break,
            _ = sleep(Duration::from_secs(sync_interval_secs)) => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                let wallets_lock = wallets.lock().await;
                let mut status = service_status.lock().await;
                let mut total_utxos = 0;
                let mut total_value_btc = 0.0;
                let mut total_value_usd = 0.0;

                for (user_id, (wallet, chain)) in wallets_lock.iter() {
                    let balance = wallet.wallet.balance();
                    total_utxos += chain.utxos.len() as u32;
                    total_value_btc += balance.confirmed.to_sat() as f64 / 100_000_000.0;
                    total_value_usd += chain.stabilized_usd.0;

                    if let Some(user_status) = user_statuses.lock().await.get_mut(user_id) {
                        user_status.utxo_count = chain.utxos.len() as u32;
                        user_status.total_value_btc = balance.confirmed.to_sat() as f64 / 100_000_000.0;
                        user_status.total_value_usd = chain.stabilized_usd.0;
                        user_status.confirmations_pending = balance.untrusted_pending.to_sat() > 0;
                    }
                }

                status.total_utxos = total_utxos;
                status.total_value_btc = total_value_btc;
                status.total_value_usd = total_value_usd;
                status.users = user_statuses.lock().await.clone();
                status.health = "healthy".to_string();
                status.last_update = now;

                if let Err(e) = state_manager.save(&PathBuf::from("service_status.json"), &*status).await {
                    warn!("Failed to save service status update: {}", e);
                }
            }
        }
    }

    info!("Initiating shutdown cleanup");
    let wallets_lock = wallets.lock().await;
    let mut save_tasks = Vec::with_capacity(wallets_lock.len() + 1);
    for (user_id, (_, chain)) in wallets_lock.iter() {
        let state_manager = state_manager.clone();
        let user_id = user_id.to_string();
        let chain = chain.clone();
        save_tasks.push(tokio::spawn(async move {
            if let Err(e) = state_manager.save_stable_chain(&user_id, &chain).await {
                warn!("Failed to save stable chain for user {}: {}", user_id, e);
            }
        }));
    }
    let state_manager = state_manager.clone();
    let service_status = service_status.clone();
    save_tasks.push(tokio::spawn(async move {
        let status = service_status.lock().await;
        if let Err(e) = state_manager.save(&PathBuf::from("service_status.json"), &*status).await {
            warn!("Failed to save service status: {}", e);
        }
    }));
    join_all(save_tasks).await;

    drop(sync_tx);
    if let Err(e) = shutdown_tx.send(()) {
        warn!("Failed to broadcast shutdown signal: {}", e);
    }

    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            price_handle,
            retry_handle,
            monitor_handle,
            server_handle,
            sync_handle
        );
    }).await.map_err(|e| Box::<dyn std::error::Error>::from(e))?;

    info!("Shutdown process completed");
    Ok(())
}
