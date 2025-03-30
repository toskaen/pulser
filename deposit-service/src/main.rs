use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, broadcast};
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use std::fs;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use log::{info, warn, debug, error, trace};
use reqwest::Client;
use common::error::PulserError;
use common::price_feed::{fetch_btc_usd_price, PriceFeed};
use common::types::PriceInfo;
use deposit_service::wallet::DepositWallet;
use common::{StableChain, UserStatus, ServiceStatus, WebhookRetry};
use deposit_service::webhook::{WebhookConfig, start_retry_task};
use deposit_service::monitor::{MonitorConfig, monitor_deposits};
use deposit_service::api;
use common::task_manager::{UserTaskLock, ScopedTask};
use deposit_service::wallet_init::{Config, init_wallet_with_changeset};
use tokio::time::sleep;
use futures::future::join_all;
use log::LevelFilter;
use bdk_esplora::esplora_client;
use common::StateManager;

// Global constants
const STATUS_UPDATE_INTERVAL_SECS: u64 = 60;
const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 30;
const MAX_SHUTDOWN_WAIT_SECS: u64 = 30;
const DERIBIT_RECONNECT_DELAY_SECS: u64 = 5;
const WEBSOCKET_HEALTH_CHECK_SECS: u64 = 30;
const PRICE_UPDATE_INTERVAL_SECS: u64 = 21;
const MAX_ESPLORA_ERRORS: u32 = 10;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // For error conversion
    impl From<PulserError> for Box<dyn std::error::Error> {
        fn from(e: PulserError) -> Self {
            Box::new(e)
        }
    }
    
    let start_time = Instant::now();
    
    // Load configuration
    let config_str = match fs::read_to_string("config/service_config.toml") {
        Ok(s) => s,
        Err(e) => {
            return Err(Box::new(PulserError::ConfigError(format!("Failed to read config file: {}", e))));
        }
    };
    
    let config: Config = match toml::from_str(&config_str) {
        Ok(c) => c,
        Err(e) => {
            return Err(Box::new(PulserError::ConfigError(format!("Failed to parse config: {}", e))));
        }
    };
    
    let config_value: toml::Value = match toml::from_str(&config_str) {
        Ok(v) => v,
        Err(e) => {
            return Err(Box::new(PulserError::ConfigError(format!("Failed to parse config as Value: {}", e))));
        }
    };
    
    // Initialize logging
    let log_level = match config_value.get("log_level").and_then(|v| v.as_str()).unwrap_or("info").to_lowercase().as_str() {
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
    
    info!("Starting Pulser Deposit Service v{}", env!("CARGO_PKG_VERSION"));
    
    // Create service status
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(e) => {
            return Err(Box::new(PulserError::from(e)));
        }
    };
    
    let service_status = Arc::new(Mutex::new(ServiceStatus {
        up_since: now,
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
        websocket_active: false, // Start as false until connection confirmed
    }));
    
    // Initialize directories
    let data_dir = config.data_dir.clone();
    let data_lsp = format!("{}/data_lsp", data_dir);
    let wallets_dir = format!("{}/wallets", data_dir);
    let events_dir = format!("{}/events", data_dir);
    
    for dir in [data_dir.as_str(), &data_lsp, &wallets_dir, &events_dir] {
        if !Path::new(dir).exists() {
            info!("Creating directory: {}", dir);
            if let Err(e) = fs::create_dir_all(dir) {
                return Err(Box::new(PulserError::StorageError(format!("Failed to create directory: {}", e))));
            }
        }
    }
    
    // Setup shared components
    let state_manager = Arc::new(StateManager::new(&data_dir));
    let wallets = Arc::new(Mutex::new(HashMap::<String, (DepositWallet, StableChain)>::new()));
    let user_statuses = Arc::new(Mutex::new(HashMap::<String, UserStatus>::new()));
    let retry_queue = Arc::new(Mutex::new(VecDeque::<WebhookRetry>::new()));
    let price_feed = Arc::new(PriceFeed::new());
    let active_tasks_manager = Arc::new(UserTaskLock::new());
    
    // Create shutdown channel with increased capacity
    let (shutdown_tx, _) = broadcast::channel::<()>(16);
    
    // Setup blockchain client
    let blockchain = match esplora_client::Builder::new(&config.esplora_url).build_async() {
        Ok(client) => {
            info!("Connected to Esplora API at {}", config.esplora_url);
            Arc::new(client)
        },
        Err(e) => {
            error!("Failed to connect to Esplora API: {}", e);
            return Err(Box::new(PulserError::NetworkError(format!("Failed to connect to Esplora: {}", e))));
        }
    };
    
    // Setup Esplora URLs tracking
    let esplora_urls = Arc::new(Mutex::new(vec![
        (config.esplora_url.clone(), 0),
        (config.fallback_esplora_url.clone().unwrap_or_default(), 0)
    ]));
    
    // Setup HTTP client with appropriate timeouts
    let timeout = config.request_timeout_secs.unwrap_or(DEFAULT_HTTP_TIMEOUT_SECS);
    let client = Client::builder()
        .timeout(Duration::from_secs(timeout))
        .connect_timeout(Duration::from_secs(timeout))
        .pool_idle_timeout(Some(Duration::from_secs(30)))
        .pool_max_idle_per_host(16) // Increased connection pool
        .build()
        .unwrap_or_else(|e| {
            warn!("Failed to build custom HTTP client: {}. Using default client.", e);
            Client::new()
        });
    
    // Setup price info
    let price_info = Arc::new(Mutex::new(PriceInfo { 
        raw_btc_usd: 0.0, 
        timestamp: 0, 
        price_feeds: HashMap::new() 
    }));
    
    // Setup last activity tracking
    let last_activity_check = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    
    // Setup sync channel with increased capacity
    let (sync_tx, sync_rx) = mpsc::channel::<String>(1000);
    
    // Preload existing users
    info!("Preloading existing users from {}", data_dir);
    match preload_existing_users(
        &data_dir,
        &state_manager,
        price_feed.clone(),
        &wallets,
        &user_statuses,
        &config
    ).await {
        Ok((loaded_count, error_count)) => {
            let wallets_count = wallets.lock().await.len();
            service_status.lock().await.users_monitored = wallets_count as u32;
            info!("Preloaded {} users ({} failed)", loaded_count, error_count);
        },
        Err(e) => {
            warn!("Error during user preloading: {}", e);
            // Continue execution rather than failing
        }
    }
    
    // Create initial service status file if it doesn't exist
    let status_path = PathBuf::from(format!("{}/service_status.json", data_dir));
    if !status_path.exists() {
        if let Err(e) = state_manager.save(&status_path, &*service_status.lock().await).await {
            warn!("Failed to create initial service_status.json: {}", e);
        } else {
            info!("Created initial service_status.json");
        }
    }
    
    // Setup signal handlers for graceful shutdown
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .unwrap_or_else(|e| {
                warn!("Failed to bind SIGTERM: {}", e);
                panic!("Failed to bind SIGTERM");
            });
            
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .unwrap_or_else(|e| {
                warn!("Failed to bind SIGINT: {}", e);
                panic!("Failed to bind SIGINT");
            });
            
        tokio::select! {
            _ = sigterm.recv() => info!("Received SIGTERM"),
            _ = sigint.recv() => info!("Received SIGINT"),
        }
        
        if let Err(e) = shutdown_tx_clone.send(()) {
            warn!("Failed to send shutdown signal: {}", e);
        }
    });
    
    // Start WebSocket health checker
    info!("Starting WebSocket health checker");
    let ws_health_handle = tokio::spawn({
        let price_feed = price_feed.clone();
        let service_status = service_status.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(WEBSOCKET_HEALTH_CHECK_SECS));
            
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("WebSocket health checker shutting down");
                        break;
                    },
                    _ = interval.tick() => {
                        let is_connected = price_feed.is_websocket_connected();
                        
                        // Update service status with connection state
                        let mut status = service_status.lock().await;
                        let previous_state = status.websocket_active;
                        status.websocket_active = is_connected;
                        
                        // Log state changes
                        if previous_state != is_connected {
                            if is_connected {
                                info!("WebSocket connection restored");
                            } else {
                                warn!("WebSocket connection lost");
                            }
                        }
                    }
                }
            }
            
            Ok::<(), PulserError>(())
        }
    });
    
    // Start price feed with robust error handling
    info!("Starting price feed");
    let price_handle = tokio::spawn({
        let price_info = price_info.clone();
        let service_status = service_status.clone();
        let client = client.clone();
        let price_feed = price_feed.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        
        async move {
            let mut update_interval = tokio::time::interval(Duration::from_secs(PRICE_UPDATE_INTERVAL_SECS));
            let mut consecutive_failures = 0;
            
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Price feed shutting down");
                        break;
                    },
                    _ = update_interval.tick() => {
                        match fetch_btc_usd_price(&client, &price_feed).await {
                            Ok(new_price) => {
                                consecutive_failures = 0;
                                let mut price = price_info.lock().await;
                                *price = new_price;
                                
                                // Also update the state manager's price cache
                                let _ = state_manager.update_price_cache(new_price.raw_btc_usd, new_price.timestamp).await;
                                
                                // Update service status
                                let mut status = service_status.lock().await;
                                status.last_price = price.raw_btc_usd;
                                status.price_update_count += 1;
                                
                                trace!("Updated price: ${:.2}", price.raw_btc_usd);
                            }
                            Err(e) => {
                                consecutive_failures += 1;
                                warn!("Price update failed (attempt {}): {}", consecutive_failures, e);
                                
                                if consecutive_failures > 5 {
                                    error!("Multiple price update failures: {}", e);
                                    let mut status = service_status.lock().await;
                                    status.health = "price feed error".to_string();
                                }
                            }
                        }
                    }
                }
            }
            
            info!("Price feed shutdown complete");
            Ok::<(), PulserError>(())
        }
    });
    
    // Start Deribit WebSocket feed with reconnection logic
    info!("Starting Deribit WebSocket feed");
    let deribit_feed_handle = tokio::spawn({
        let mut shutdown_rx = shutdown_tx.subscribe();
        let price_feed = price_feed.clone();
        let service_status = service_status.clone();
        
        async move {
            let mut consecutive_failures = 0;
            let max_failures = 5;
            
            loop {
                // Check for shutdown signal
                if shutdown_rx.try_recv().is_ok() {
                    info!("Deribit WebSocket feed shutting down");
                    break;
                }
                
                // Update status to indicate we're attempting connection
                {
                    let mut status = service_status.lock().await;
                    status.websocket_active = false;
                }
                
                info!("Connecting to Deribit WebSocket...");
                match price_feed.start_deribit_feed(&mut shutdown_rx).await {
                    Ok(_) => {
                        info!("Deribit WebSocket feed completed gracefully");
                        consecutive_failures = 0;
                        
                        // Update connection status
                        let mut status = service_status.lock().await;
                        status.websocket_active = true;
                        
                        // If we got a clean exit, check if we should gracefully stop
                        if shutdown_rx.try_recv().is_ok() {
                            info!("Shutdown received after WebSocket completion");
                            break;
                        }
                    },
                    Err(e) => {
                        consecutive_failures += 1;
                        error!("Deribit WebSocket feed error (attempt {}/{}): {}", 
                               consecutive_failures, max_failures, e);
                        
                        if consecutive_failures >= max_failures {
                            error!("Too many consecutive WebSocket failures, giving up");
                            // Update service status to indicate WebSocket failure
                            let mut status = service_status.lock().await;
                            status.websocket_active = false;
                            status.health = "websocket error".to_string();
                            
                            // Take a longer sleep before attempting again
                            sleep(Duration::from_secs(30)).await;
                            consecutive_failures = 0;
                        } else {
                            // Back off progressively
                            let backoff_secs = DERIBIT_RECONNECT_DELAY_SECS * 2u64.pow(consecutive_failures as u32);
                            info!("Waiting {} seconds before reconnecting...", backoff_secs);
                            sleep(Duration::from_secs(backoff_secs)).await;
                        }
                    }
                }
            }
            
            info!("Deribit WebSocket feed task complete");
            Ok::<(), PulserError>(())
        }
    });
    
    // Start webhook retry task
    info!("Starting webhook retry task");
    let retry_handle = tokio::spawn(start_retry_task(
        client.clone(),
        retry_queue.clone(),
        config.webhook_url.clone(),
        WebhookConfig::from_toml(&config_value),
        shutdown_tx.subscribe(),
        state_manager.clone(),
    ));
    
    // Start deposit monitor task
    info!("Starting deposit monitor task");
    let monitor_handle = tokio::spawn(monitor_deposits(
        wallets.clone(),
        user_statuses.clone(),
        last_activity_check.clone(),
        sync_tx.clone(),
        price_info.clone(),
        MonitorConfig::from_toml(&config_value),
        shutdown_tx.subscribe(),
        price_feed.clone(),
        client.clone(),
        blockchain.clone(),
        state_manager.clone(),
        service_status.clone(),
    ));
    
    // Start API server
    info!("Starting API server on {}:{}", config.listening_address, config.listening_port);
    let server_handle = tokio::spawn({
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
            WebhookConfig::from_toml(&config_value),
            client.clone(),
            active_tasks_manager.clone(),
            price_feed.clone(),
            blockchain.clone(),
        );
        
        let mut shutdown_rx = shutdown_tx.subscribe();
        let listening_address = match config.listening_address.parse::<std::net::IpAddr>() {
            Ok(addr) => addr,
            Err(e) => {
                error!("Invalid listening address {}: {}", config.listening_address, e);
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))
            }
        };
        
        async move {
            warp::serve(routes)
                .bind_with_graceful_shutdown((listening_address, config.listening_port), async move {
                    shutdown_rx.recv().await.ok();
                    info!("API server shutting down");
                })
                .1
                .await;
            info!("API server shutdown complete");
            Ok::<(), PulserError>(())
        }
    });
    
    // Start sync processor task with improved concurrency control
    info!("Starting sync processor task");
    let sync_handle = tokio::spawn({
        let wallets = wallets.clone();
        let user_statuses = user_statuses.clone();
        let price_info = price_info.clone();
        let state_manager = state_manager.clone();
        let retry_queue = retry_queue.clone();
        let client = client.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let service_status = service_status.clone();
        let active_tasks_manager = active_tasks_manager.clone();
        let price_feed = price_feed.clone();
        let blockchain = blockchain.clone();
        let mut sync_rx = sync_rx;
        let esplora_urls = esplora_urls.clone();
        
        async move {
            let mut pending_users = Vec::new();
            
            // Periodically clean up stale task locks
            let mut cleanup_interval = tokio::time::interval(Duration::from_secs(300)); // 5 minutes
            
            loop {
                tokio::select! {
                    user_id_option = sync_rx.recv() => {
                        if let Some(user_id) = user_id_option {
                            debug!("Received sync request for user {}", user_id);
                            // Deduplicate user IDs in queue
                            if !pending_users.contains(&user_id) {
                                pending_users.push(user_id);
                            }
                        } else {
                            warn!("Sync channel closed unexpectedly");
                            break;
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Sync processor shutting down");
                        break;
                    }
                    _ = cleanup_interval.tick() => {
                        // Clean up stale task locks
                        let cleaned = active_tasks_manager.clean_stale_tasks().await;
                        if cleaned > 0 {
                            debug!("Cleaned up {} stale task locks", cleaned);
                        }
                    }
                    _ = sleep(Duration::from_millis(100)), if !pending_users.is_empty() => {
                        let mut processed = Vec::new();
                        
                        for (i, user_id) in pending_users.iter().enumerate() {
                            // Limit concurrent syncs
                            let max_concurrent = config.max_concurrent_users.unwrap_or(5) as u32;
                            if service_status.lock().await.active_syncs >= max_concurrent {
                                debug!("Max concurrent syncs reached ({}), waiting...", max_concurrent);
                                break;
                            }
                            
                            // Skip if user is already being processed
                            if active_tasks_manager.is_user_active(user_id).await {
                                debug!("User {} already being synced, skipping", user_id);
                                continue;
                            }
                            
                            // Use scoped task guard to ensure cleanup
                            if let Some(_task_guard) = ScopedTask::new(&active_tasks_manager, user_id, "sync").await {
                                // Update service status for active task
                                {
                                    let mut status = service_status.lock().await;
                                    status.active_syncs += 1;
                                }
                                
                                // Execute the sync
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
                                    &config.webhook_url,
                                    &WebhookConfig::from_toml(&config_value),
                                    client.clone(),
                                ).await;
                                
                                // Log results
                                match &result {
                                    Ok(new_funds) => {
                                        if *new_funds {
                                            info!("Sync completed with new funds for user {}", user_id);
                                        } else {
                                            debug!("Sync completed for user {}", user_id);
                                        }
                                    },
                                    Err(e) => {
                                        warn!("Sync failed for user {}: {}", user_id, e);
                                    }
                                }
                                
                                // Update service status
                                {
                                    let mut status = service_status.lock().await;
                                    status.active_syncs = status.active_syncs.saturating_sub(1);
                                }
                                
                                // Mark user as processed
                                processed.push(i);
                            } else {
                                debug!("Failed to acquire task lock for user {}", user_id);
                            }
                        }
                        
                        // Remove processed users in reverse order to maintain index validity
                        for idx in processed.into_iter().rev() {
                            pending_users.remove(idx);
                        }
                    }
                }
            }
            
            info!("Sync processor shutdown complete");
            Ok::<(), PulserError>(())
        }
    });
    
    // Start status update task
    info!("Starting status update task");
    let status_update_handle = tokio::spawn({
        let service_status = service_status.clone();
        let wallets = wallets.clone();
        let state_manager = state_manager.clone();
        let data_dir = data_dir.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(STATUS_UPDATE_INTERVAL_SECS));
            
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Status update task shutting down");
                        break;
                    },
                    _ = interval.tick() => {
                        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
                        
                        // Periodically check for stale locks
                        let _ = active_tasks_manager.clean_stale_tasks().await;
                        
                        // Only take the wallet lock briefly
                        let (total_utxos, total_value_btc, total_value_usd, users_count) = match tokio::time::timeout(
                            Duration::from_secs(5),
                            wallets.lock()
                        ).await {
                            Ok(wallets_lock) => (
                                wallets_lock.values().map(|(_, chain)| chain.utxos.len() as u32).sum(),
                                wallets_lock.values().map(|(wallet, _)| wallet.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0).sum(),
                                wallets_lock.values().map(|(_, chain)| chain.stabilized_usd.0).sum(),
                                wallets_lock.len() as u32
                            ),
                            Err(_) => {
                                warn!("Timeout acquiring wallets lock for status update");
                                (0, 0.0, 0.0, 0)
                            }
                        };
                        
                        // Update service status atomically
                        let updated_status = match tokio::time::timeout(
                            Duration::from_secs(5),
                            service_status.lock()
                        ).await {
                            Ok(mut status) => {
                                status.total_utxos = total_utxos;
                                status.total_value_btc = total_value_btc;
                                status.total_value_usd = total_value_usd;
                                status.health = "healthy".to_string();
                                status.last_update = now;
                                status.users_monitored = users_count;
                                status.clone()
                            },
                            Err(_) => {
                                warn!("Timeout acquiring service status lock for update");
                                continue;
                            }
                        };
                        
                        // Save status to disk
                        let status_path = PathBuf::from(format!("{}/data_lsp/service_status.json", data_dir));
                        if let Err(e) = state_manager.save(&status_path, &updated_status).await {
                            warn!("Failed to save service_status: {}", e);
                        } else {
                            trace!("Saved service_status to {:?}", status_path);
                        }
                    }
                }
            }
            
            info!("Status update task shutdown complete");
            Ok::<(), PulserError>(())
        }
    });
    
    info!("Service startup completed in {}ms", start_time.elapsed().as_millis());
    
    // Wait for shutdown signal
    match tokio::signal::ctrl_c().await {
        Ok(()) => info!("Received shutdown signal, initiating graceful shutdown..."),
        Err(e) => warn!("Failed to listen for shutdown signal: {}", e),
    }
    
    // Initiate shutdown
    info!("Initiating shutdown sequence");
    
    // Send shutdown signal to all tasks
    if let Err(e) = shutdown_tx.send(()) {
        warn!("Failed to broadcast shutdown signal: {}", e);
    }
    
    // Save final status
    info!("Saving final service status");
    let save_status_handle = tokio::spawn({
        let state_manager = state_manager.clone();
        let status = match tokio::time::timeout(
            Duration::from_secs(5),
            service_status.lock()
        ).await {
            Ok(status) => status.clone(),
            Err(_) => {
                warn!("Timeout acquiring service status for final save");
                ServiceStatus {
                    up_since: now,
                    last_update: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    users_monitored: 0,
                    total_utxos: 0,
                    total_value_btc: 0.0,
                    total_value_usd: 0.0,
                    health: "shutting down".to_string(),
                    last_price: 0.0,
                    price_update_count: 0,
                    active_syncs: 0,
                    websocket_active: false,
                }
            }
        };
        
        let status_path = PathBuf::from(format!("{}/data_lsp/service_status.json", data_dir));
        
        async move {
            if let Err(e) = state_manager.save(&status_path, &status).await {
                warn!("Failed to save final service_status: {}", e);
            } else {
                info!("Saved final service_status");
            }
            Ok::<(), PulserError>(())
        }
    });
    
    // Wait for all tasks to complete with timeout
    info!("Waiting for all tasks to shut down gracefully (timeout: {}s)", MAX_SHUTDOWN_WAIT_SECS);
    
    match tokio::time::timeout(
        Duration::from_secs(MAX_SHUTDOWN_WAIT_SECS),
        join_all(vec![
            price_handle,
            deribit_feed_handle,
            ws_health_handle,
            retry_handle,
            monitor_handle,
            server_handle,
            sync_handle,
            status_update_handle,
            save_status_handle,
        ])
    ).await {
        Ok(_) => info!("All tasks shut down gracefully"),
        Err(_) => warn!("Some tasks did not shut down within the timeout period"),
    }
    
  info!("Shutdown process completed");
    
    Ok(())
}

// Helper function to preload existing users
async fn preload_existing_users(
    data_dir: &str,
    state_manager: &Arc<StateManager>,
    price_feed: Arc<PriceFeed>,
    wallets: &Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>,
    config: &Config
) -> Result<(usize, usize), PulserError> {
    let start_time = Instant::now();
    let mut loaded_count = 0;
    let mut error_count = 0;
    
    let entries = match fs::read_dir(data_dir) {
        Ok(entries) => entries,
        Err(e) => {
            warn!("Failed to read data directory {}: {}", data_dir, e);
            return Err(PulserError::StorageError(format!("Failed to read data directory: {}", e)));
        }
    };
    
    for entry in entries {
        let path = match entry {
            Ok(entry) => entry.path(),
            Err(e) => {
                warn!("Failed to read directory entry: {}", e);
                continue;
            }
        };
        
        if path.is_dir() {
            if let Some(dir_name) = path.file_name().and_then(|f| f.to_str()) {
                if let Some(user_id) = dir_name.strip_prefix("user_") {
                    debug!("Found user directory: {}", dir_name);
                    
                    let public_path = path.join(format!("user_{}_public.json", user_id));
                    let chain_path = path.join(format!("stable_chain_{}.json", user_id));
                    
                    if public_path.exists() || chain_path.exists() {
                        info!("Preloading user: {}", user_id);
                        
                        match preload_single_user(user_id, state_manager, price_feed.clone(), wallets, user_statuses, config).await {
                            Ok(_) => {
                                loaded_count += 1;
                                debug!("Successfully preloaded user {}", user_id);
                            },
                            Err(e) => {
                                error!("Failed to preload user {}: {}", user_id, e);
                                error_count += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    
    info!("Preloaded {} users ({} failed) in {}ms", 
          loaded_count, error_count, start_time.elapsed().as_millis());
    
    if loaded_count == 0 && error_count > 0 {
        Err(PulserError::InternalError("Failed to preload any users".to_string()))
    } else {
        Ok((loaded_count, error_count))
    }
}

// Helper to preload a single user
async fn preload_single_user(
    user_id: &str,
    state_manager: &Arc<StateManager>,
    price_feed: Arc<PriceFeed>,
    wallets: &Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>,
    config: &Config
) -> Result<(), PulserError> {
    let start_time = Instant::now();
    
    // Try to initialize wallet
    match DepositWallet::from_config("config/service_config.toml", user_id, state_manager, price_feed).await {
        Ok((mut wallet, deposit_info, mut chain)) => {
            // Try to load changeset if available
            if let Ok(changeset) = state_manager.load_changeset(user_id).await {
                match init_wallet_with_changeset(config, user_id, changeset) {
                    Ok((new_wallet, _)) => {
                        debug!("Initialized wallet with changeset for user {}", user_id);
                        wallet.wallet = new_wallet;
                    },
                    Err(e) => warn!("Failed to init wallet with changeset for {}: {}", user_id, e),
                }
            }
            
            // Try to load stable chain if available
            let chain_path = PathBuf::from(format!("{}/user_{}/stable_chain_{}.json", 
                                                   config.data_dir, user_id, user_id));
            if chain_path.exists() {
                match state_manager.load::<StableChain>(&chain_path).await {
                    Ok(loaded_chain) => {
                        debug!("Loaded stable chain for user {}", user_id);
                        chain = loaded_chain;
                    },
                    Err(e) => warn!("Failed to load stable chain for {}: {}", user_id, e),
                }
            }
            
            // Update storage maps with timeouts
            match tokio::time::timeout(
                Duration::from_secs(5),
                user_statuses.lock()
            ).await {
                Ok(mut statuses) => {
                    let mut status = UserStatus::new(user_id);
                    status.current_deposit_address = deposit_info.address.clone();
                    status.utxo_count = chain.utxos.len() as u32;
                    status.total_value_btc = wallet.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0;
                    status.total_value_usd = chain.stabilized_usd.0;
                    status.sync_status = "initialized".to_string();
                    status.last_update_message = "User preloaded".to_string();
                    statuses.insert(user_id.to_string(), status);
                },
                Err(_) => {
                    warn!("Timeout acquiring user_statuses lock for user {}", user_id);
                    return Err(PulserError::InternalError(
                        format!("Timeout acquiring user_statuses lock for user {}", user_id)));
                }
            }
            
            match tokio::time::timeout(
                Duration::from_secs(5),
                wallets.lock()
            ).await {
                Ok(mut wallets_lock) => {
                    wallets_lock.insert(user_id.to_string(), (wallet, chain));
                },
                Err(_) => {
                    warn!("Timeout acquiring wallets lock for user {}", user_id);
                    return Err(PulserError::InternalError(
                        format!("Timeout acquiring wallets lock for user {}", user_id)));
                }
            }
            
            debug!("Preloaded user {} in {}ms", user_id, start_time.elapsed().as_millis());
            Ok(())
        },
        Err(e) => {
            error!("Failed to initialize wallet for user {}: {}", user_id, e);
            Err(e)
        }
    }
}
