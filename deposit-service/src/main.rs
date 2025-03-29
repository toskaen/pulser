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
use common::task_manager::UserTaskLock;
use deposit_service::wallet_init::{Config, init_wallet_with_changeset};
use tokio::time::sleep;
use std::path::PathBuf;
use futures::future::join_all;
use log::LevelFilter;
use bdk_esplora::esplora_client;
use common::StateManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_str = fs::read_to_string("config/service_config.toml")?;
    let config: Config = toml::from_str(&config_str)?;
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

    let blockchain = Arc::new(esplora_client::Builder::new(&config.esplora_url).build_async()?);
    let active_tasks_manager = Arc::new(UserTaskLock::new());
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10))
        .pool_idle_timeout(Some(Duration::from_secs(30)))
        .build()?;
    let start_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let data_dir = config.data_dir.clone();
    let sync_interval_secs = config.sync_interval_secs;

    let state_manager = Arc::new(StateManager::new(&data_dir));
    let wallets = Arc::new(Mutex::new(HashMap::<String, (DepositWallet, StableChain)>::new()));
    let user_statuses = Arc::new(Mutex::new(HashMap::<String, UserStatus>::new()));
    let retry_queue = Arc::new(Mutex::new(VecDeque::<WebhookRetry>::new()));
    let price_feed = Arc::new(PriceFeed::new());
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let price_info = Arc::new(Mutex::new(PriceInfo { raw_btc_usd: 0.0, timestamp: 0, price_feeds: HashMap::new() }));
    let last_activity_check = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let (sync_tx, mut sync_rx) = mpsc::channel::<String>(1000);

    let service_status = Arc::new(Mutex::new(ServiceStatus {
        up_since: start_time,
        last_update: 0,
        version: env!("CARGO_PKG_VERSION").to_string(),
        users_monitored: 0,
        total_utxos: 0,
        total_value_btc: 0.0,
        total_value_usd: 0.0,
        health: "starting".to_string(),
        last_price: 0.0,
        price_update_count: 0,
        active_syncs: 0,
        websocket_active: true,
    }));

    fs::create_dir_all(format!("{}/data_lsp", data_dir))?;

    info!("Preloading existing users from {}", data_dir);
    for entry in fs::read_dir(&data_dir)? {
        let path = entry?.path();
        if path.is_dir() {
            if let Some(user_id) = path.file_name().and_then(|f| f.to_str()).and_then(|s| s.strip_prefix("user_")) {
                let public_path = path.join(format!("user_{}_public.json", user_id));
                let chain_path = path.join(format!("stable_chain_{}.json", user_id));
                if public_path.exists() || chain_path.exists() {
                    info!("Preloading user: {}", user_id);
                    match DepositWallet::from_config("config/service_config.toml", user_id, &state_manager, price_feed.clone()).await {
                        Ok((mut wallet, deposit_info, mut chain)) => {
                            if let Ok(changeset) = state_manager.load_changeset(user_id).await {
                                match init_wallet_with_changeset(&config, user_id, changeset) {
                                    Ok((new_wallet, _)) => wallet.wallet = new_wallet,
                                    Err(e) => warn!("Failed to init wallet with changeset for {}: {}", user_id, e),
                                }
                            }
                            if chain_path.exists() {
                                chain = state_manager.load::<StableChain>(&chain_path).await.unwrap_or(chain);
                            }
                            let mut statuses = user_statuses.lock().await;
                            let mut status = UserStatus::new(user_id);
                            status.current_deposit_address = deposit_info.address.clone();
                            statuses.insert(user_id.to_string(), status);
                            wallets.lock().await.insert(user_id.to_string(), (wallet, chain));
                        }
                        Err(e) => error!("Failed to preload wallet for user {}: {}", user_id, e),
                    }
                }
            }
        }
    }
    {
        let mut status = service_status.lock().await;
        status.users_monitored = wallets.lock().await.len() as u32;
        let status_path = PathBuf::from(format!("{}/service_status.json", data_dir));
        if !status_path.exists() {
            state_manager.save(&status_path, &*status).await?;
            info!("Created initial service_status.json at {:?}", status_path);
        }
    }
    info!("Preloaded {} users", wallets.lock().await.len());

    // Signal handler
    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                match signal(SignalKind::terminate()) {
                    Ok(mut sigterm) => match signal(SignalKind::interrupt()) {
                        Ok(mut sigint) => {
                            tokio::select! {
                                _ = sigterm.recv() => info!("Received SIGTERM"),
                                _ = sigint.recv() => info!("Received SIGINT"),
                            }
                            shutdown_tx.send(()).ok();
                        }
                        Err(e) => warn!("Failed to bind SIGINT: {}", e),
                    },
                    Err(e) => warn!("Failed to bind SIGTERM: {}", e),
                }
            }
            #[cfg(windows)]
            {
                use tokio::signal::windows;
                match windows::ctrl_c() {
                    Ok(mut ctrl_c) => match windows::ctrl_break() {
                        Ok(mut ctrl_break) => {
                            tokio::select! {
                                _ = ctrl_c.recv() => info!("Received Ctrl+C"),
                                _ = ctrl_break.recv() => info!("Received Ctrl+Break"),
                            }
                            shutdown_tx.send(()).ok();
                        }
                        Err(e) => warn!("Failed to bind Ctrl+Break: {}", e),
                    },
                    Err(e) => warn!("Failed to bind Ctrl+C: {}", e),
                }
            }
        }
    });

    // Price update task
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
                            }
                            Err(e) => warn!("Price update failed: {}", e),
                        }
                    }
                }
            }
            Ok::<(), PulserError>(())
        }
    });

    // Webhook retry task
    let retry_handle = tokio::spawn(start_retry_task(
        client.clone(),
        retry_queue.clone(),
        config.webhook_url.clone(),
        WebhookConfig::from_toml(&config_value),
        shutdown_rx.resubscribe(),
        state_manager.clone(),
    ));

    // Monitor task
    let monitor_handle = tokio::spawn(monitor_deposits(
        wallets.clone(),
        user_statuses.clone(),
        last_activity_check.clone(),
        sync_tx.clone(),
        price_info.clone(),
        MonitorConfig::from_toml(&config_value),
        shutdown_rx.resubscribe(),
        price_feed.clone(),
        client.clone(),
        blockchain.clone(),
        state_manager.clone(),
        service_status.clone(),
    ));

    // API server
let listening_address = config.listening_address.parse::<std::net::IpAddr>()?;
let server_handle = tokio::spawn({
    let routes = api::routes(
        service_status.clone(),
        wallets.clone(),
        user_statuses.clone(),
        price_info.clone(),
        Arc::new(Mutex::new(vec![(config.esplora_url.clone(), 0), (config.fallback_esplora_url.clone(), 0)])),
        state_manager.clone(),
        sync_tx.clone(),
        retry_queue.clone(),
        config.webhook_url.clone(),
        WebhookConfig::from_toml(&config_value),
        client.clone(),
        active_tasks_manager.clone(),
        price_feed.clone(),
        blockchain.clone(),
    );
    let mut shutdown_rx = shutdown_rx.resubscribe();
    async move {
        warp::serve(routes)
            .bind_with_graceful_shutdown((listening_address, config.listening_port), async move {
                shutdown_rx.recv().await.ok();
                info!("Server shutting down");
            })
            .1
            .await;
    }
});

    // Sync task
    let sync_handle = tokio::spawn({
        let wallets = wallets.clone();
        let user_statuses = user_statuses.clone();
        let price_info = price_info.clone();
        let state_manager = state_manager.clone();
        let retry_queue = retry_queue.clone();
        let client = client.clone();
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
                        for user_id in pending_users.drain(..) {
                            if active_tasks_manager.is_user_active(&user_id).await { continue; }
                            active_tasks_manager.mark_user_active(&user_id).await;
                            let mut status = service_status.lock().await;
                            status.active_syncs += 1;
                            drop(status);
                            let _ = api::sync_user(
                                &user_id,
                                wallets.clone(),
                                user_statuses.clone(),
                                price_info.clone(),
                                price_feed.clone(),
                                Arc::new(Mutex::new(vec![(config.esplora_url.clone(), 0), (config.fallback_esplora_url.clone(), 0)])),
                                &blockchain,
                                state_manager.clone(),
                                retry_queue.clone(),
                                &config.webhook_url,
                                &WebhookConfig::from_toml(&config_value),
                                client.clone(),
                            ).await;
                            let mut status = service_status.lock().await;
                            status.active_syncs = status.active_syncs.saturating_sub(1);
                            active_tasks_manager.mark_user_inactive(&user_id).await;
                        }
                    }
                }
            }
            Ok::<(), PulserError>(())
        }
    });

    // Periodic status update
    let mut shutdown_rx = shutdown_rx.resubscribe();
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => break,
            _ = sleep(Duration::from_secs(sync_interval_secs)) => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                let wallets_lock = wallets.lock().await;
                let mut status = service_status.lock().await;
                status.total_utxos = wallets_lock.values().map(|(_, chain)| chain.utxos.len() as u32).sum();
                status.total_value_btc = wallets_lock.values().map(|(wallet, _)| wallet.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0).sum();
                status.total_value_usd = wallets_lock.values().map(|(_, chain)| chain.stabilized_usd.0).sum(); // Extract f64
                status.health = "healthy".to_string();
                status.last_update = now;
                status.users_monitored = wallets_lock.len() as u32;
                let status_path = PathBuf::from(format!("{}/data_lsp/service_status.json", data_dir));
                if let Err(e) = state_manager.save(&status_path, &*status).await {
                    warn!("Failed to save service_status: {}", e);
                } else {
                    debug!("Saved service_status to {:?}", status_path);
                }
            }
        }
    }

    // Shutdown
    info!("Initiating shutdown");
    let mut save_tasks = Vec::new();
    save_tasks.push(tokio::spawn({
        let state_manager = state_manager.clone();
        let status = service_status.lock().await.clone();
        async move {
            let status_path = PathBuf::from(format!("{}/data_lsp/service_status.json", data_dir));
            if let Err(e) = state_manager.save(&status_path, &status).await {
                warn!("Failed to save service_status: {}", e);
            } else {
                info!("Saved service_status on shutdown");
            }
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
