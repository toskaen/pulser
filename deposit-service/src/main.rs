use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use tokio::sync::{mpsc, broadcast, Mutex};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::PathBuf;
use log::{info, warn, debug, error, trace, LevelFilter};
use reqwest::Client;
use bdk_esplora::esplora_client;
use common::error::PulserError;
use common::price_feed::{PriceFeed, fetch_btc_usd_price};
use common::storage::StateManager;
use common::task_manager::{UserTaskLock, ScopedTask};
use common::types::{PriceInfo, UserStatus, ServiceStatus, WebhookRetry, StableChain};
use deposit_service::api;
use deposit_service::config::Config;
use deposit_service::monitor::{monitor_deposits, MonitorConfig};
use deposit_service::wallet::DepositWallet;
use deposit_service::webhook::{start_retry_task, WebhookConfig};
use futures::future::join_all;
use bdk_wallet::KeychainKind;


// Constants
const STATUS_UPDATE_INTERVAL_SECS: u64 = 60;
const PRICE_UPDATE_INTERVAL_SECS: u64 = 600; // 10 minutes
const STALE_LOCK_CLEANUP_SECS: u64 = 300;
const MAX_SHUTDOWN_WAIT_SECS: u64 = 30;
const WEBSOCKET_HEALTH_CHECK_SECS: u64 = 30;

async fn preload_existing_users(
    data_dir: &str,
    config: &Config,
    state_manager: &Arc<StateManager>,
    price_feed: Arc<PriceFeed>,
    wallets: &Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>,
        sync_tx: mpsc::Sender<String>,  // Add this parameter
) -> Result<(usize, usize), PulserError> {
    let start_time = Instant::now();
    let mut loaded_count = 0;
    let mut error_count = 0;

    let entries = fs::read_dir(data_dir)?;
    for entry in entries {
        let path = entry?.path();
        if path.is_dir() {
            if let Some(user_id) = path.file_name().and_then(|f| f.to_str()).and_then(|s| s.strip_prefix("user_")) {
                debug!("Preloading user: {}", user_id);
match DepositWallet::from_config(config, user_id, state_manager, price_feed.clone()).await {
    Ok((wallet, deposit_info, chain, _recovery_doc)) => {
        let status = UserStatus {
            user_id: user_id.to_string(),
            last_sync: 0,
            sync_status: "pending".to_string(),  // Mark as unsynced
            utxo_count: chain.utxos.len() as u32,
            total_value_btc: 0.0,  // Defer to monitor
            total_value_usd: chain.stabilized_usd.0,
            confirmations_pending: false,
            last_update_message: "Preloaded from disk, awaiting sync".to_string(),
            sync_duration_ms: 0,
            last_error: None,
            last_success: 0,
            pruned_utxo_count: 0,
            current_deposit_address: deposit_info.address,
            last_deposit_time: None,
        };
        {
            let mut wallets_lock = wallets.lock().await;
            wallets_lock.insert(user_id.to_string(), (wallet, chain));
        }
        {
            let mut statuses_lock = user_statuses.lock().await;
            statuses_lock.insert(user_id.to_string(), status);
        }
        sync_tx.send(user_id.to_string()).await?;  // Queue for monitor
        loaded_count += 1;
    }
    Err(e) => {
        error!("Failed to preload user {}: {}", user_id, e);
        error_count += 1;
    }
}
            }
        }
    }
    info!("Preloaded {} users ({} failed) in {}ms", loaded_count, error_count, start_time.elapsed().as_millis());
    Ok((loaded_count, error_count))
}

#[tokio::main]
async fn main() -> Result<(), PulserError> {
    let config = Config::from_file("config/service_config.toml")?;
        let config = Arc::new(config); // Wrap in Arc once here
let data_lsp = config.data_dir.clone();
    fs::create_dir_all(&data_lsp)?;
    
  let webhook_config = WebhookConfig {
        max_retries: config.webhook_max_retries,
        timeout_secs: config.webhook_timeout_secs,
        max_retry_time_secs: config.webhook_max_retry_time_secs,
        retry_max_attempts: config.webhook_retry_max_attempts,
        base_backoff_ms: config.webhook_base_backoff_ms,
        retry_interval_secs: config.webhook_retry_interval_secs,
    };

    // Initialize logging
    let log_level = match config.log_level.to_lowercase().as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Info,
    };
    env_logger::Builder::new()
        .filter_level(log_level)
        .filter_module("hyper", LevelFilter::Warn)
        .filter_module("reqwest", LevelFilter::Warn)
        .filter_module("tokio", LevelFilter::Warn)
        .init();
    info!("Starting Pulser Deposit Service v{}", config.version);

    // Shared resources
    let state_manager = Arc::new(StateManager::new(&data_lsp));
    let service_status = Arc::new(Mutex::new(state_manager.load_service_status().await?));
    let wallets = Arc::new(Mutex::new(HashMap::<String, (DepositWallet, StableChain)>::new()));
    let user_statuses = Arc::new(Mutex::new(HashMap::<String, UserStatus>::new()));
    let retry_queue = Arc::new(Mutex::new(VecDeque::<WebhookRetry>::new()));
    let price_feed = Arc::new(PriceFeed::new());
    let active_tasks_manager = Arc::new(UserTaskLock::new());
    let price_info = Arc::new(Mutex::new(PriceInfo {
        raw_btc_usd: 0.0,
        timestamp: 0,
        price_feeds: HashMap::new(),
    }));
    let last_activity_check = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let esplora_urls = Arc::new(Mutex::new(vec![(config.esplora_url.clone(), 0), (config.fallback_esplora_url.clone(), 0)]));
    let client = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()?;
    let blockchain = Arc::new(esplora_client::Builder::new(&config.esplora_url).build_async()?);

    // Channels
    let (sync_tx, sync_rx) = mpsc::channel::<String>(config.max_concurrent_users * 2);
    let (shutdown_tx, _) = broadcast::channel::<()>(16);

           let deribit_feed_handle = tokio::spawn({
        let price_feed = price_feed.clone();
        let service_status = service_status.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
            let mut failures = 0;
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Deribit feed handle shutting down");
                        break;
                    }
                    _ = async { if failures > 0 { tokio::time::sleep(Duration::from_secs(5 * failures as u64)).await; } } => {
                        match price_feed.start_deribit_feed(&mut shutdown_rx).await {
                            Ok(_) => {
                                failures = 0;
                                service_status.lock().await.websocket_active = true;
                            }
                            Err(e) => {
                                failures += 1;
                                warn!("Deribit feed failed ({} attempts): {}", failures, e);
                                if failures > 5 {
                                    let mut status = service_status.lock().await;
                                    status.health = "websocket error".to_string();
                                    status.websocket_active = false;
                                }
                            }
                        }
                    }
                }
            }
            info!("Deribit feed handle completed");
            Ok::<(), PulserError>(())
        }
    });
       
        let price_handle = tokio::spawn({
        let price_info = price_info.clone();
        let service_status = service_status.clone();
        let client = client.clone();
        let price_feed = price_feed.clone();
        let state_manager = state_manager.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(PRICE_UPDATE_INTERVAL_SECS));
            let mut failures = 0;
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Price handle shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        match fetch_btc_usd_price(&client, &price_feed).await {
                            Ok(new_price) => {
                                failures = 0;
                                {
                                    let mut price = price_info.lock().await;
                                    *price = new_price.clone();
                                }
                                state_manager.update_price_cache(new_price.raw_btc_usd, new_price.timestamp).await?;
                                let mut status = service_status.lock().await;
                                status.last_price = new_price.raw_btc_usd;
                                status.price_update_count += 1;
                                trace!("Price updated: ${:.2}", new_price.raw_btc_usd);
                            }
                            Err(e) => {
                                failures += 1;
                                warn!("Price fetch failed ({} attempts): {}", failures, e);
                                if failures > 5 {
                                    let mut status = service_status.lock().await;
                                    status.health = "price feed error".to_string();
                                }
                            }
                        }
                    }
                }
            }
            info!("Price handle completed");
            Ok::<(), PulserError>(())
        }
    });
    
info!("Waiting for price data from any source...");
let mut price_ready = false;
for attempt in 0..30 {  // Wait up to 30 seconds
    // Check global price info first (which includes all sources)
    let global_price = price_info.lock().await.raw_btc_usd;
    if global_price > 0.0 {
        price_ready = true;
        info!("Price data ready from global cache: ${:.2}", global_price);
        break;
    }
    
    // Try to trigger fallback price fetching
    match fetch_btc_usd_price(&client, &price_feed).await {
        Ok(price_data) if price_data.raw_btc_usd > 0.0 => {
            // Update the global price info
            let mut price_guard = price_info.lock().await;
            *price_guard = price_data.clone();
            price_ready = true;
            info!("Price data ready from fallback: ${:.2} ({})", 
      price_data.raw_btc_usd,
      price_data.price_feeds.keys().collect::<Vec<&String>>().iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", "));
            break;
        }
        _ => {
            tokio::time::sleep(Duration::from_secs(1)).await;
            debug!("Waiting for any valid price source, attempt {}/30", attempt + 1);
        }
    }
}

if !price_ready {
    warn!("Proceeding without confirmed price data after 30 attempts; some operations may be delayed");
}
    
    

    match preload_existing_users(&data_lsp,&config, &state_manager, price_feed.clone(), &wallets, &user_statuses, sync_tx.clone(), ).await {
        Ok((loaded_count, error_count)) => {
            let mut status = service_status.lock().await;
            status.users_monitored = loaded_count as u32;
            info!("Preloaded {} users ({} failed)", loaded_count, error_count);
        }
        Err(e) => warn!("Preloading failed: {}", e),
    }

    // Signal handlers
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await?;
        info!("Received Ctrl+C, shutting down");
        shutdown_tx_clone.send(())?;
        Ok::<(), PulserError>(())
    });
    #[cfg(unix)]
    {
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
            tokio::select! {
                _ = sigterm.recv() => info!("Received SIGTERM"),
                _ = sigint.recv() => info!("Received SIGINT"),
            }
            shutdown_tx_clone.send(())?;
            Ok::<(), PulserError>(())
        });
    }

    // Tasks


    let ws_health_handle = tokio::spawn({
        let price_feed = price_feed.clone();
        let service_status = service_status.clone();
        let state_manager = state_manager.clone(); // Clone before move
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(WEBSOCKET_HEALTH_CHECK_SECS));
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("WebSocket health checker shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        let is_connected = price_feed.is_websocket_connected().await;
                        let mut status = service_status.lock().await;
                        let previous_state = status.websocket_active;
                        status.websocket_active = is_connected;
                        if previous_state != is_connected {
                            if is_connected {
                                info!("Deribit WebSocket connection restored");
                            } else {
                                warn!("Deribit WebSocket connection lost");
                            }
                            state_manager.save_service_status(&status).await.unwrap_or_else(|e| {
                                warn!("Failed to save service status after WebSocket state change: {}", e);
                            });
                        }
                    }
                }
            }
            info!("WebSocket health checker completed");
            Ok::<(), PulserError>(())
        }
    });

    let retry_handle = tokio::spawn({
        let retry_queue = retry_queue.clone();
        let state_manager = state_manager.clone();
        let client = client.clone(); // Clone before move
        let webhook_url = config.webhook_url.clone();
        let webhook_config = WebhookConfig::from_toml(&toml::from_str(&fs::read_to_string("config/service_config.toml")?)?);
        let shutdown_tx_clone = shutdown_tx.clone(); // Clone before move
        async move {
            start_retry_task(
                client,
                retry_queue,
                webhook_url,
                webhook_config,
                shutdown_tx_clone.subscribe(),
                state_manager,
            ).await?;
            info!("Retry handle completed");
            Ok::<(), PulserError>(())
        }
    });
    
    let monitor_handle = tokio::spawn({
        let config = config.clone();
        let wallets = wallets.clone();
        let user_statuses = user_statuses.clone();
        let last_activity_check = last_activity_check.clone();
        let sync_tx = sync_tx.clone();
        let price_info = price_info.clone();
        let price_feed = price_feed.clone();
        let client = client.clone();
        let blockchain = blockchain.clone();
        let state_manager = state_manager.clone();
        let service_status = service_status.clone();
        let shutdown_tx_clone = shutdown_tx.clone();

async move {
    monitor_deposits(
        wallets,
        user_statuses,
        last_activity_check,
        sync_tx,
        price_info,
    (*config).clone(), // Dereference Arc and clone the Config
        shutdown_tx_clone.subscribe(),
        price_feed,
        client,
        blockchain,
        state_manager,
        service_status,
    ).await?;
    info!("Monitor handle completed");
    Ok::<(), PulserError>(())
}
    });

let sync_handle = tokio::spawn({
    let mut sync_rx = sync_rx;
    let active_tasks_manager = active_tasks_manager.clone();
    let service_status = service_status.clone();
    let wallets = wallets.clone();
    let user_statuses = user_statuses.clone();
    let price_info = price_info.clone();
    let esplora_urls = esplora_urls.clone();
    let state_manager = state_manager.clone();
    let retry_queue = retry_queue.clone();
    let webhook_url = config.webhook_url.clone();
    let webhook_config = webhook_config.clone(); // Use outer webhook_config
    let client = client.clone();
    let price_feed = price_feed.clone();
    let blockchain = blockchain.clone();
    let shutdown_tx_clone = shutdown_tx.clone();
    let config = config.clone(); // Add config for sync_user

    async move {
        let mut pending_users = Vec::new();
        let mut cleanup_interval = tokio::time::interval(Duration::from_secs(STALE_LOCK_CLEANUP_SECS));
        let mut shutdown_rx = shutdown_tx_clone.subscribe();
        loop {
            tokio::select! {
                Some(user_id) = sync_rx.recv() => {
                    if !pending_users.contains(&user_id) {
                        pending_users.push(user_id);
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Sync handle shutting down");
                    break;
                }
                _ = cleanup_interval.tick() => {
                    let cleaned = active_tasks_manager.clean_stale_tasks().await;
                    if cleaned > 0 {
                        debug!("Cleaned {} stale tasks", cleaned);
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)), if !pending_users.is_empty() => {
                    let mut processed = Vec::new();
                    let max_concurrent = config.max_concurrent_users as u32;
                    let current_active = service_status.lock().await.active_syncs;
                    for (i, user_id) in pending_users.iter().enumerate() {
                        if current_active >= max_concurrent {
                            break;
                        }
                        if active_tasks_manager.is_user_active(user_id).await {
                            processed.push(i);
                            continue;
                        }
                        if let Some(_task) = ScopedTask::new(&active_tasks_manager, user_id, "sync").await {
                            {
                                let mut status = service_status.lock().await;
                                status.active_syncs += 1;
                            }
                            let result = api::sync_user(
                                user_id,
                                wallets.clone(),
                                user_statuses.clone(),
                                price_info.clone(),
                                price_feed.clone(),
                                esplora_urls.clone(),
                                &blockchain,
                                state_manager.clone(),
                                retry_queue.clone(),
                                &webhook_url,
                                &webhook_config,
                                client.clone(),
                                config.clone(), // Use cloned Arc
                            ).await;
                            if let Err(e) = result {
                                warn!("Sync failed for {}: {}", user_id, e);
                            }
                            {
                                let mut status = service_status.lock().await;
                                status.active_syncs = status.active_syncs.saturating_sub(1);
                            }
                            processed.push(i);
                        }
                    }
                    for idx in processed.into_iter().rev() {
                        pending_users.remove(idx);
                    }
                }
            }
        }
        info!("Sync handle completed");
        Ok::<(), PulserError>(())
    }
});

  let status_update_handle = tokio::spawn({
    let service_status = service_status.clone();
    let wallets = wallets.clone();
    let state_manager = state_manager.clone();
    let mut shutdown_rx = shutdown_tx.subscribe();
    async move {
        let mut interval = tokio::time::interval(Duration::from_secs(STATUS_UPDATE_INTERVAL_SECS));
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Status update handle shutting down");
                    break;
                }
                _ = interval.tick() => {
                    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                    let mut status = service_status.lock().await;
match tokio::time::timeout(Duration::from_secs(5), wallets.lock()).await {
    Ok(wallets_lock) => {
        status.users_monitored = wallets_lock.len() as u32;
        status.total_utxos = wallets_lock.values().map(|(_, chain)| chain.utxos.len() as u32).sum();
status.total_value_btc = wallets_lock.values().map(|(wallet, _)| {
    let btc = wallet.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0;
    // Optional: only log non-zero balances
    if btc > 0.0 {
        debug!("User balance: {:.8} BTC", btc);
    }
    btc
}).sum();
        status.total_value_usd = wallets_lock.values().map(|(_, chain)| chain.stabilized_usd.0).sum();
    }
    Err(_) => {
        warn!("Timeout locking wallets, using last values");
    }
};
                    status.last_update = now;
                    if status.health != "price feed error" && status.health != "websocket error" {
                        status.health = "healthy".to_string();
                    }
                    debug!("Updating ServiceStatus: {} users, {:.8} BTC, ${:.2} USD", 
                           status.users_monitored, status.total_value_btc, status.total_value_usd);
                    state_manager.save_service_status(&status).await?;
                    trace!("Status: {} users, {} UTXOs, {} BTC, ${} USD", 
                           status.users_monitored, status.total_utxos, status.total_value_btc, status.total_value_usd);
                }
            }
        }
        info!("Status update handle completed");
        Ok::<(), PulserError>(())
    }
});

    let server_handle = tokio::spawn({
        let config = config.clone();
    let webhook_config = webhook_config.clone(); // Requires Clone
        let routes = api::routes(
            service_status.clone(),
            wallets.clone(),
            user_statuses.clone(),
            price_info.clone(),
            esplora_urls.clone(),
            state_manager.clone(),
            sync_tx.clone(),
            retry_queue.clone(),
            config.webhook_url.clone(),
            webhook_config, // Pass constructed WebhookConfig
            client.clone(),
            active_tasks_manager.clone(),
            price_feed.clone(),
            blockchain.clone(),
                    config.clone(), // Use cloned Arc

        );
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
            let addr = config.listening_address.parse::<std::net::IpAddr>()?;
            warp::serve(routes)
                .bind_with_graceful_shutdown((addr, config.listening_port), async move {
                    shutdown_rx.recv().await.ok();
                })
                .1
                .await;
            info!("Server handle completed");
            Ok::<(), PulserError>(())
        }
    });

    let save_status_handle = tokio::spawn({
        let service_status = service_status.clone();
        let state_manager = state_manager.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
            shutdown_rx.recv().await?;
            info!("Saving final service status");
            let status = service_status.lock().await.clone();
            state_manager.save_service_status(&status).await?;
            info!("Final service status saved");
            Ok::<(), PulserError>(())
        }
    });

    // Shutdown handling
    shutdown_tx.subscribe().recv().await?;
    info!("Shutting down...");

    // Close WebSocket connections
    info!("Closing WebSocket connections...");
    match tokio::time::timeout(Duration::from_secs(5), price_feed.shutdown_websocket()).await {
        Ok(Some(_)) => info!("WebSocket closed successfully"),
        Ok(None) => info!("No active WebSocket to close"),
        Err(_) => warn!("Timeout closing WebSocket"),
    }

    // Save final service status
    info!("Saving final service status...");
    {
        let status = service_status.lock().await;
        if let Err(e) = state_manager.save_service_status(&status).await {
            warn!("Failed to save final service status: {}", e);
        }
    }

    let handles = vec![
        price_handle,
        deribit_feed_handle,
        ws_health_handle,
        retry_handle,
        monitor_handle,
        sync_handle,
        status_update_handle,
        server_handle,
        save_status_handle,
    ];

    // Give tasks time to shut down gracefully
    info!("Waiting for tasks to complete...");
    match tokio::time::timeout(Duration::from_secs(MAX_SHUTDOWN_WAIT_SECS), join_all(handles)).await {
        Ok(results) => {
            for result in results {
                if let Err(e) = result {
                    warn!("Task error during shutdown: {}", e);
                }
            }
            info!("All tasks shut down gracefully");
        }
        Err(_) => {
            warn!("Shutdown timed out after {}s, some tasks may not have completed", MAX_SHUTDOWN_WAIT_SECS);
        }
    }

    info!("Deposit service shutdown complete");
    Ok(())
}
