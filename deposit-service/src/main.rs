use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use tokio::sync::{mpsc, broadcast, Mutex};
use std::collections::{HashMap};
use std::fs;
use std::path::PathBuf;
use std::path::Path;
use std::io::Write; // Required for formatter
use log::{info, warn, debug, error, trace, LevelFilter};
use reqwest::Client;
use bdk_esplora::esplora_client;
use common::error::PulserError;
use common::price_feed::{PriceFeed, PriceFeedExtensions};
use common::price_feed::http_sources::fetch_btc_usd_price;
use common::storage::StateManager;
use common::task_manager::{UserTaskLock, ScopedTask};
use common::types::{PriceInfo, UserStatus, ServiceStatus, StableChain};
use deposit_service::api;
use deposit_service::config::Config;
use deposit_service::wallet::DepositWallet;
use futures::future::join_all;
use bdk_wallet::KeychainKind;
use common::webhook::{RetryQueue, WebhookConfig, WebhookManager};
use common::health::{
    HealthChecker, create_health_response, Component, 
    ComponentType, HealthCheck as CommonHealthCheck, HealthResult
};

use common::health::providers::{PriceFeedCheck, BlockchainCheck, WebSocketCheck, RedisCheck};
use common::utils; // Import utils module
use deposit_service::monitor::monitor_deposits;
use common::websocket::WebSocketManager;



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
    sync_tx: mpsc::Sender<String>,
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

fn create_price_info(price_data: f64, source: &str) -> PriceInfo {
    let now = utils::now_timestamp();
    let mut price_feeds = HashMap::new();
    price_feeds.insert(source.to_string(), price_data);
    
    PriceInfo {
        raw_btc_usd: price_data,
        timestamp: now,
        price_feeds,
    }
}

async fn perform_graceful_shutdown(
    service_status: Arc<Mutex<ServiceStatus>>,
    state_manager: Arc<StateManager>,
    price_feed: Arc<PriceFeed>,
    webhook_manager: WebhookManager,
    active_tasks_manager: Arc<UserTaskLock>,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
) -> Result<(), PulserError> {
    info!("Starting graceful shutdown...");
    
    // Step 1: Save final service status
    info!("Saving final service status...");
    match tokio::time::timeout(Duration::from_secs(5), service_status.lock()).await {
        Ok(status_guard) => {
            let status = status_guard.clone();
            if let Err(e) = state_manager.save_service_status(&status).await {
                warn!("Failed to save final service status: {}", e);
            }
        },
        Err(_) => warn!("Timeout acquiring service status for final save"),
    }
    
    // Step 2: Close WebSocket connections
    info!("Closing WebSocket connections...");
    match tokio::time::timeout(Duration::from_secs(5), price_feed.shutdown_websocket()).await {
        Ok(Some(Ok(_))) => info!("WebSocket closed successfully"),
        Ok(Some(Err(e))) => warn!("Error closing WebSocket: {}", e),
        Ok(None) => info!("No active WebSocket to close"),
        Err(_) => warn!("Timeout closing WebSocket"),
    }
    
    // Step 3: Save wallet state for all users
    info!("Saving wallet state for all users...");
    match tokio::time::timeout(Duration::from_secs(10), wallets.lock()).await {
        Ok(wallets_guard) => {
            let user_count = wallets_guard.len();
            info!("Saving state for {} users", user_count);
            
            let mut saved = 0;
            let mut failed = 0;
            
            for (user_id, (wallet, chain)) in wallets_guard.iter() {
                if let Some(changeset) = wallet.wallet.staged() {
                    if let Err(e) = state_manager.save_changeset(user_id, changeset).await {
                        warn!("Failed to save changeset for user {}: {}", user_id, e);
                        failed += 1;
                    } else {
                        saved += 1;
                    }
                }
                
                if let Err(e) = state_manager.save_stable_chain(user_id, chain).await {
                    warn!("Failed to save StableChain for user {}: {}", user_id, e);
                    failed += 1;
                } else {
                    saved += 1;
                }
            }
            
            info!("Saved state for {} operations, {} failed", saved, failed);
        },
        Err(_) => warn!("Timeout acquiring wallets lock for final save"),
    }
    
    // Step 4: Wait for active tasks to complete
    let active_tasks = active_tasks_manager.get_active_tasks().await;
    let active_count = active_tasks.len();
    if active_count > 0 {
        info!("Waiting for {} active tasks to complete", active_count);
        
        // Give tasks a chance to complete with timeout
        for i in 0..10 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let new_count = active_tasks_manager.get_active_tasks().await.len();
            if new_count < active_count {
                info!("Progress: {} of {} tasks completed", active_count - new_count, active_count);
                if new_count == 0 {
                    info!("All tasks completed successfully");
                    break;
                }
            }
            
            // After 5 seconds, clean up stale tasks
            if i == 5 {
                let cleaned = active_tasks_manager.clean_stale_tasks().await;
                if cleaned > 0 {
                    info!("Cleaned {} stale tasks", cleaned);
                }
            }
        }
        
        // Final check
        let remaining = active_tasks_manager.get_active_tasks().await.len();
        if remaining > 0 {
            warn!("{} tasks did not complete gracefully", remaining);
        }
    }
    
    // Step 5: Flush webhook queue
    info!("Finalizing webhook delivery...");
    match tokio::time::timeout(Duration::from_secs(5), webhook_manager.shutdown()).await {
        Ok(Ok(_)) => info!("Webhook manager shut down successfully"),
        Ok(Err(e)) => warn!("Error shutting down webhook manager: {}", e),
        Err(_) => warn!("Timeout shutting down webhook manager"),
    }
    
    info!("Graceful shutdown complete");
    Ok(())
}



#[tokio::main]
async fn main() -> Result<(), PulserError> {
    // Load and prepare configuration
    let config = Arc::new(Config::from_file("config/service_config.toml")?);
    let data_dir = config.data_dir.clone();
    fs::create_dir_all(&data_dir)?;
    
    // Load webhook configuration
    let webhook_config = WebhookConfig::from_toml(&toml::from_str(&fs::read_to_string("config/service_config.toml")?)?);

    // Initialize logging
    let log_level = match config.log_level.to_lowercase().as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Info,
    };
    // In deposit-service/src/main.rs
env_logger::Builder::new()
    .filter_level(log_level)
    .filter_module("hyper", LevelFilter::Warn)
    .filter_module("reqwest", LevelFilter::Warn)
    .filter_module("tokio", LevelFilter::Warn)
    .format(|buf, record| {
        writeln!(
            buf,
            "{} [{}] {} - {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            record.level(),
            record.module_path().unwrap_or("unknown"),
            record.args()
        )
    })
    .init();
    info!("Starting Pulser Deposit Service v{}", config.version);

    // Initialize HTTP client
    let client = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()?;

    // Initialize shared resources
    let state_manager = Arc::new(StateManager::new(&data_dir));
    let service_status = Arc::new(Mutex::new(state_manager.load_service_status().await?));
    let wallets = Arc::new(Mutex::new(HashMap::<String, (DepositWallet, StableChain)>::new()));
    let user_statuses = Arc::new(Mutex::new(HashMap::<String, UserStatus>::new()));
    let retry_queue = Arc::new(Mutex::new(RetryQueue::new_memory(webhook_config.retry_max_attempts)));
    let price_feed = Arc::new(PriceFeed::new());
    let active_tasks_manager = Arc::new(UserTaskLock::new());
    let price_info = Arc::new(Mutex::new(PriceInfo {
        raw_btc_usd: 0.0,
        timestamp: 0,
        price_feeds: HashMap::new(),
    }));
    let last_activity_check = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let esplora_urls = Arc::new(Mutex::new(vec![
        (config.esplora_url.clone(), 0), 
        (config.fallback_esplora_url.clone(), 0)
    ]));
    let blockchain = Arc::new(esplora_client::Builder::new(&config.esplora_url).build_async()?);
    
    // Initialize WebhookManager
    let webhook_manager = WebhookManager::new(
        client.clone(), 
        webhook_config.clone(), 
        retry_queue.clone()
    ).await;

    // Initialize Health Checker
    let mut health_checker = HealthChecker::new().with_cache_ttl(10);

    // Register health checks for critical components
    health_checker.register(
        PriceFeedCheck::new(price_feed.clone())
            .with_min_price(2121.0)
            .with_max_staleness(120) // 2 minutes
    );

    health_checker.register(
        BlockchainCheck::new(blockchain.clone())
            .with_max_tip_age(3600) // 1 hour
    );

health_checker.register(
    WebSocketCheck::new_with_manager(
        price_feed.get_websocket_manager(),
        WebSocketManager::get_exchange_url("deribit").to_string()
    )
    .with_name("deribit_websocket")
    .with_max_inactivity(60)
);

    // Add Redis check if Redis is configured
    if config.redis_enabled {
        health_checker.register(
            RedisCheck::new(&config.redis_url)
                .with_critical(false)
        );
    }

    health_checker.register(
        crate::api::health::ApiEndpointCheck::new(&config.channel_service_url)
            .with_name("channel_service_api")
            .with_critical(false)
    );

let health_checker = Arc::new(health_checker);

    // Channels
    let (sync_tx, sync_rx) = mpsc::channel::<String>(config.max_concurrent_users * 2);
    let (shutdown_tx, _) = broadcast::channel::<()>(16);

    // Start the Deribit feed handler
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
    
// Structured price update function
async fn update_price_info(
    client: &Client,
    price_info: &Arc<Mutex<PriceInfo>>,
    service_status: &Arc<Mutex<ServiceStatus>>,
    state_manager: &Arc<StateManager>
) -> Result<PriceInfo, PulserError> {
    match fetch_btc_usd_price(client).await {
        Ok(price_data) => {
            // Create the price info object
            let now = utils::now_timestamp();
            let new_price_info = PriceInfo {
                raw_btc_usd: price_data,
                timestamp: now,
                price_feeds: {
                    let mut feeds = HashMap::new();
                    feeds.insert("HTTP_Fallback".to_string(), price_data);
                    feeds
                },
            };
            
            // Update the price cache atomically
            {
                let mut price_guard = match tokio::time::timeout(
                    Duration::from_secs(3), 
                    price_info.lock()
                ).await {
                    Ok(guard) => guard,
                    Err(_) => {
                        warn!("Timeout acquiring price_info lock");
                        return Ok(new_price_info);
                    }
                };
                *price_guard = new_price_info.clone();
            }
            
            // Update the state manager
            if let Err(e) = state_manager.update_price_cache(
                new_price_info.raw_btc_usd, 
                new_price_info.timestamp
            ).await {
                warn!("Failed to update price cache: {}", e);
            }
            
            // Update service status
            {
                let mut status = match tokio::time::timeout(
                    Duration::from_secs(3),
                    service_status.lock()
                ).await {
                    Ok(guard) => guard,
                    Err(_) => {
                        warn!("Timeout acquiring service_status lock");
                        return Ok(new_price_info);
                    }
                };
                status.last_price = new_price_info.raw_btc_usd;
                status.price_update_count += 1;
            }
            
            trace!("Price updated: ${:.2}", new_price_info.raw_btc_usd);
            Ok(new_price_info)
        },
        Err(e) => {
            warn!("Price fetch failed: {}", e);
            Err(PulserError::PriceFeedError(format!("Price fetch failed: {}", e)))
        }
    }
}

// Fix for the main.rs price update section
match fetch_btc_usd_price(&client).await {
    Ok(price_data) => {
        // Convert f64 to PriceInfo
        let now = utils::now_timestamp();
        let new_price_info = PriceInfo {
            raw_btc_usd: price_data,
            timestamp: now,
            price_feeds: {
                let mut feeds = HashMap::new();
                feeds.insert("HTTP_Fallback".to_string(), price_data);
                feeds
            }
        };
        
        // Update the price cache
        {
            let mut price_guard = price_info.lock().await;
            *price_guard = new_price_info.clone();
        }
        
        // Update cache in state manager
        let price_data = {
            let price_guard = price_info.lock().await;
            (price_guard.raw_btc_usd, price_guard.timestamp)
        };
        state_manager.update_price_cache(price_data.0, price_data.1).await?;
        
        // Update service status
        {
            let mut status = service_status.lock().await;
            status.last_price = price_data.0;
            trace!("Price updated: ${:.2}", price_data.0);
        }
    },
    Err(e) => {
        // Handle error case
        warn!("Price fetch failed: {}", e);
    }
}
    
    // Preload existing users
    match preload_existing_users(
        &data_dir, 
        &config, 
        &state_manager, 
        price_feed.clone(), 
        &wallets, 
        &user_statuses, 
        sync_tx.clone()
    ).await {
        Ok((loaded_count, error_count)) => {
            let mut status = service_status.lock().await;
            status.users_monitored = loaded_count as u32;
            info!("Preloaded {} users ({} failed)", loaded_count, error_count);
        }
        Err(e) => warn!("Preloading failed: {}", e),
    }

    // Set up signal handlers
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

    // Start WebSocket health monitoring
    let ws_health_handle = tokio::spawn({
        let price_feed = price_feed.clone();
        let service_status = service_status.clone();
        let state_manager = state_manager.clone();
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

    // Start webhook retry handler
    let retry_handle = tokio::spawn({
        let retry_queue = retry_queue.clone();
        let client = client.clone();
        let webhook_config = webhook_config.clone();
        let webhook_manager = webhook_manager.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Retry task shutting down");
                        webhook_manager.shutdown().await?;
                        break;
                    }
                    _ = interval.tick() => {
                        retry_queue.lock().await.retry_failed(
                            &client, 
                            &webhook_config.secret, 
                            |a| webhook_config.calculate_backoff(a)
                        ).await?;
                    }
                }
            }
            info!("Retry handle completed");
            Ok::<(), PulserError>(())
        }
    });
    
    // Start deposit monitor
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

    // Start sync handler
    let sync_handle = tokio::spawn({
        let mut sync_rx = sync_rx;
        let active_tasks_manager = active_tasks_manager.clone();
        let service_status = service_status.clone();
        let wallets = wallets.clone();
        let user_statuses = user_statuses.clone();
        let price_info = price_info.clone();
        let esplora_urls = esplora_urls.clone();
        let state_manager = state_manager.clone();
        let webhook_url = config.webhook_url.clone();
        let webhook_manager = webhook_manager.clone();
        let client = client.clone();
        let price_feed = price_feed.clone();
        let blockchain = blockchain.clone();
        let shutdown_tx_clone = shutdown_tx.clone();
        let config = config.clone();

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
                                    webhook_manager.clone(),
                                    &webhook_url,
                                    client.clone(),
                                    config.clone(),
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

    // Start status update handler  
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

    // Start web server
    let server_handle = tokio::spawn({
        let config = config.clone();
        let routes = api::routes(
            service_status.clone(),
            wallets.clone(),
            user_statuses.clone(),
            price_info.clone(),
            esplora_urls.clone(),
            state_manager.clone(),
            sync_tx.clone(),
            webhook_manager.clone(),
            config.webhook_url.clone(),
            client.clone(),
            active_tasks_manager.clone(),
            price_feed.clone(),
            blockchain.clone(),
            config.clone(),
            health_checker.clone(), // Pass health checker to routes
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

    // Handle final cleanup and shutdown
let save_status_handle = tokio::spawn({
    let service_status = service_status.clone();
    let state_manager = state_manager.clone();
    let price_feed = price_feed.clone();
    let webhook_manager = webhook_manager.clone();
    let active_tasks_manager = active_tasks_manager.clone();
        let wallets = wallets.clone(); // Add this
    let mut shutdown_rx = shutdown_tx.subscribe();
    
    async move {
        shutdown_rx.recv().await?;
        info!("Shutdown signal received");
        
        if let Err(e) = perform_graceful_shutdown(
            service_status,
            state_manager,
            price_feed,
            webhook_manager,
            active_tasks_manager,
            wallets,
        ).await {
            warn!("Error during graceful shutdown: {}", e);
        }
        
        Ok::<(), PulserError>(())
    }
});

    // Wait for shutdown signal
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

    // Wait for all tasks to complete
    let handles = vec![
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
