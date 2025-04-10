// Fixed version of deposit-service/src/main.rs
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use tokio::sync::{mpsc, broadcast, Mutex};
use std::collections::{HashMap};
use std::fs;
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
use common::types::{PriceInfo, UserStatus, ServiceStatus, StableChain, ResourceUsage, PerformanceMetrics};
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
use tokio::sync::Semaphore;
use common::price_feed::http_sources;
use std::sync::atomic::{AtomicU64, Ordering};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

// Constants
const STATUS_UPDATE_INTERVAL_SECS: u64 = 60;
const PRICE_UPDATE_INTERVAL_SECS: u64 = 600; // 10 minutes
const STALE_LOCK_CLEANUP_SECS: u64 = 300;
const MAX_SHUTDOWN_WAIT_SECS: u64 = 30;
const WEBSOCKET_HEALTH_CHECK_SECS: u64 = 30;
const GLOBAL_PRICE_UPDATE_INTERVAL_SECS: u64 = 210; // Update global price once per minute

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
                    Ok((mut wallet, deposit_info, chain, _recovery_doc)) => {
                            wallet.cleanup_old_addresses().await.ok();
                    if let Err(e) = wallet.cleanup_old_addresses().await {
                        warn!("Failed to clean up old addresses for user {}: {}", user_id, e);
                    }
                   
                        // Calculate total_value_btc from unspent historical UTXOs
                        let total_value_btc = chain.history.iter()
                            .filter(|entry| !entry.spent && entry.confirmations >= config.min_confirmations)
                            .map(|entry| entry.amount_sat as f64 / 100_000_000.0)
                            .sum::<f64>();

                        let status = UserStatus {
                            user_id: user_id.to_string(),
                            last_sync: 0,
                            sync_status: "pending".to_string(),
                            utxo_count: chain.utxos.len() as u32,
                            total_value_btc,  // Use calculated value instead of 0.0
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
                        sync_tx.send(user_id.to_string()).await?;
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

// In deposit-service/src/main.rs - Replace existing perform_graceful_shutdown with this improved version

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
let ws_manager = price_feed.get_websocket_manager();
match tokio::time::timeout(Duration::from_secs(3), async {
    if let Some(Ok(_)) = price_feed.shutdown_websocket().await {
        info!("WebSocket closed successfully");
        true
    } else {
        warn!("Error during WebSocket shutdown");
        // Attempt shutdown with timeout
        if let Err(_) = tokio::time::timeout(Duration::from_secs(1), ws_manager.shutdown()).await {
            warn!("WebSocket shutdown timed out");
        }
        false
    }
}).await {
    Ok(closed) => {
        info!("WebSocket connections {} closed", 
              if closed { "successfully" } else { "partially" });
    },
    Err(_) => {
        warn!("Timeout during WebSocket shutdown");
    }
};
   
    // Step 3: Save wallet state for all users with optimized locking
    info!("Saving wallet state for all users...");
    let user_data = match tokio::time::timeout(Duration::from_secs(10), async {
        let wallets_lock = wallets.lock().await;
        // Extract just what we need for saving
        wallets_lock.iter()
            .map(|(user_id, (wallet, chain))| {
                (
                    user_id.clone(),
                    chain.clone(),
                    wallet.wallet.staged().cloned()
                )
            })
            .collect::<Vec<_>>()
    }).await {
        Ok(data) => data,
        Err(_) => {
            warn!("Timeout acquiring wallets lock for final save");
            vec![] // Empty vector if we can't get the lock
        }
    };
    
    // Now save each user's data without holding the lock
    let mut saved = 0;
    let mut failed = 0;
    let user_count = user_data.len();
    
    info!("Saving state for {} users", user_count);
    
    // Use a semaphore to limit concurrent saves
    let semaphore = Arc::new(Semaphore::new(5));
    let futures = user_data.into_iter().map(|(user_id, chain, changeset)| {
        let state_manager = state_manager.clone();
        let semaphore = semaphore.clone();
        
        async move {
            let _permit = semaphore.acquire().await?;
            
            let mut user_saved = false;
            
            // Save chain state
            if let Err(e) = state_manager.save_stable_chain(&user_id, &chain).await {
                warn!("Failed to save StableChain for user {}: {}", user_id, e);
            } else {
                user_saved = true;
            }
            
            // Save changeset if available
            if let Some(changeset) = changeset {
                if let Err(e) = state_manager.save_changeset(&user_id, &changeset).await {
                    warn!("Failed to save changeset for user {}: {}", user_id, e);
                } else {
                    user_saved = true;
                }
            }
            
            Ok::<bool, PulserError>(user_saved)
        }
    }).collect::<Vec<_>>();
    
    // Wait for all saves to complete or timeout
    match tokio::time::timeout(Duration::from_secs(20), join_all(futures)).await {
        Ok(results) => {
            for result in results {
                match result {
                    Ok(true) => saved += 1,
                    _ => failed += 1,
                }
            }
            info!("Saved state for {} users, {} failed", saved, failed);
        },
        Err(_) => {
            warn!("Timeout waiting for user state saves to complete");
        }
    }
    
    // Step 4: Wait for active tasks to complete
    let active_tasks = active_tasks_manager.get_active_tasks().await;
    let active_count = active_tasks.len();
    if active_count > 0 {
        info!("Waiting for {} active tasks to complete", active_count);
        
        // Wait with timeout
        match tokio::time::timeout(Duration::from_secs(10), async {
            let mut remaining = active_count;
            for i in 0..10 {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let new_count = active_tasks_manager.get_active_tasks().await.len();
                if new_count < remaining {
                    info!("Progress: {} of {} tasks completed", remaining - new_count, active_count);
                    remaining = new_count;
                    if remaining == 0 {
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
        }).await {
            Ok(_) => {},
            Err(_) => warn!("Timeout waiting for tasks to complete"),
        }
        
        // Final check
        let remaining = active_tasks_manager.get_active_tasks().await.len();
        if remaining > 0 {
            warn!("{} tasks did not complete gracefully", remaining);
        }
    }
    
    // Step 5: Flush webhook queue
    info!("Finalizing webhook delivery...");
    match tokio::time::timeout(Duration::from_secs(5), webhook_manager.flush_batches()).await {
        Ok(Ok(_)) => info!("Webhook queue flushed successfully"),
        Ok(Err(e)) => warn!("Error flushing webhook queue: {}", e),
        Err(_) => warn!("Timeout flushing webhook queue"),
    }
    
    // Try to shut down webhook manager
    match tokio::time::timeout(Duration::from_secs(3), webhook_manager.shutdown()).await {
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
        .filter_module("common::webhook", LevelFilter::Debug)
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
let price_feed = Arc::new(PriceFeed::new_with_auth(
    config.deribit_id.clone(),
    config.deribit_secret.clone()
));
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
                    .with_timeout(Duration::from_secs(30)) // Add explicit timeout

    );

    health_checker.register(
        BlockchainCheck::new(blockchain.clone())
            .with_max_tip_age(3600) // 1 hour
    );

let ws_manager = price_feed.get_websocket_manager();
health_checker.register(
    WebSocketCheck::new_with_manager(
        ws_manager.clone(),
        ws_manager.get_exchange_url("deribit").to_string()
    )
    .with_name("deribit_websocket")
    .with_max_inactivity(60)
);

// In main.rs startup sequence
info!("WebhookManager initialized with URL: {}", config.webhook_url);
if config.webhook_enabled {
    info!("Webhook notifications enabled");
} else {
    warn!("Webhook notifications disabled in config");
}

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
                    info!("Futures feed handle shutting down");
                    break;
                }
                _ = async { if failures > 0 { tokio::time::sleep(Duration::from_secs(5 * failures as u64)).await; } } => {
                    match price_feed.start_futures_feed(&mut shutdown_rx).await {
                        Ok(_) => {
                            failures = 0;
                            service_status.lock().await.websocket_active = true;
                        }
                        Err(e) => {
                            failures += 1;
                            warn!("Futures feed failed ({} attempts): {}", failures, e);
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
        info!("Futures feed handle completed");
        Ok::<(), PulserError>(())
    }
});
    
// Structured price update function
async fn update_prices(
    client: &Client,
    price_info: &Arc<Mutex<PriceInfo>>,
    service_status: &Arc<Mutex<ServiceStatus>>,
    state_manager: &Arc<StateManager>,
) -> Result<(), PulserError> {
    // Fetch price with timeout
    let price_data = match tokio::time::timeout(
        Duration::from_secs(10),
        fetch_btc_usd_price(client)
    ).await {
        Ok(Ok(price)) => price,
        Ok(Err(e)) => {
            warn!("Price fetch failed: {}", e);
            
            // Update service status to indicate price feed error
            match tokio::time::timeout(Duration::from_secs(2), service_status.lock()).await {
                Ok(mut status) => {
                    status.health = "price feed error".to_string();
                },
                Err(_) => warn!("Timeout acquiring service status lock for price error update"),
            }
            
            return Err(PulserError::PriceFeedError(format!("Price fetch failed: {}", e)));
        },
        Err(_) => {
            warn!("Price fetch timed out");
            return Err(PulserError::PriceFeedError("Price fetch timed out".to_string()));
        }
    };
    
    // Create new price info object
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
    
    // Update price cache with timeout
    match tokio::time::timeout(Duration::from_secs(2), price_info.lock()).await {
        Ok(mut price_guard) => {
            *price_guard = new_price_info.clone();
        },
        Err(_) => {
            warn!("Timeout acquiring price_info lock for update");
            // Continue anyway - we can still try to update the state manager
        }
    }
    
    // Update state manager (this doesn't need a lock)
    if let Err(e) = state_manager.update_price_cache(price_data, now).await {
        warn!("Failed to update price cache in state manager: {}", e);
    }
    
    // Update service status with timeout
    match tokio::time::timeout(Duration::from_secs(2), service_status.lock()).await {
        Ok(mut status) => {
            status.last_price = price_data;
            if status.health == "price feed error" {
                status.health = "healthy".to_string();
            }
            trace!("Price updated: ${:.2}", price_data);
        },
        Err(_) => {
            warn!("Timeout acquiring service_status lock for price update");
        }
    }
    
    Ok(())
}

// Then your update_global_price function:
async fn update_global_price(
    client: &Client,
    price_info: &Arc<Mutex<PriceInfo>>,
    price_feed: &Arc<PriceFeed>,
        active_tasks_manager: &Arc<UserTaskLock>, // Add this parameter

) -> Result<f64, PulserError> {
    // Get the price with appropriate error handling
    let price_result = match price_feed.get_price().await {
        Ok(info) => info,
        Err(_) => {
            // Fallback to HTTP source
            let price = http_sources::fetch_btc_usd_price(client).await?;
            let now = utils::now_timestamp();
            let mut feeds = HashMap::new();
            feeds.insert("HTTP_Fallback".to_string(), price);
            PriceInfo {
                raw_btc_usd: price,
                timestamp: now,
                price_feeds: feeds,
            }
        }
    };
    
    // Update the shared price info
    {
        let mut lock = price_info.lock().await;
        *lock = price_result.clone();
    }
    
    let duration_ms = start_time.elapsed().as_millis() as u32;
    active_tasks_manager.record_price_fetch_time(duration_ms);
    
    Ok(price_result.raw_btc_usd)
}

// And in your price update handler:
let price_update_handle = tokio::spawn({
    let price_feed = price_feed.clone();
    let price_info = price_info.clone();
    let client = client.clone();
    let service_status = service_status.clone();
    let mut shutdown_rx = shutdown_tx.subscribe();
    let mut interval = tokio::time::interval(Duration::from_secs(GLOBAL_PRICE_UPDATE_INTERVAL_SECS));
    
    async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Price update task shutting down");
                    break;
                }
                _ = interval.tick() => {
                    match update_global_price(&client, &price_info, &price_feed).await {
                        Ok(price) => {
                            debug!("Updated global price: ${:.2}", price);
                            let mut status = service_status.lock().await;
                            status.last_price = price;
                            status.price_update_count += 1;
                        },
                        Err(e) => warn!("Failed to update global price: {}", e),
                    }
                }
            }
        }
        info!("Price update task completed");
        Ok::<(), PulserError>(())
    }
});
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

let webhook_manager_for_monitor = webhook_manager.clone();

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
        let webhook_manager = webhook_manager_for_monitor; // Use the cloned variable here

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
                webhook_manager,  // Add this parameter
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
    
let price_info_for_status = price_info.clone();

let status_update_handle = tokio::spawn({
    let service_status = service_status.clone();
    let wallets = wallets.clone();
    let state_manager = state_manager.clone();
    let mut shutdown_rx = shutdown_tx.subscribe();
    let price_feed = price_feed.clone();
    let blockchain = blockchain.clone();
    let health_checker = health_checker.clone();
    let active_tasks_manager = active_tasks_manager.clone(); // Make sure this is cloned
    
    async move {
        let mut interval = tokio::time::interval(Duration::from_secs(STATUS_UPDATE_INTERVAL_SECS));
        
        // Initialize the performance tracking
        let mut last_update_time = Instant::now();
        
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Status update handle shutting down");
                    break;
                }
                _ = interval.tick() => {
                    // REPLACE THE EXISTING STATISTICS GATHERING CODE WITH THIS NEW CODE:
                    // -----------------------------------------------------------------
                    // First gather statistics with our enhanced collector
                    let statistics = {
                        let wallets_lock = wallets.lock().await;
                        
                        // Create a Vec containing the stats for each user
                        let mut stats: Vec<(u32, f64, f64)> = wallets_lock.values()
                            .map(|(wallet, chain)| {
                                let utxo_count = chain.utxos.len() as u32;
                                let btc_value = wallet.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0;
                                let usd_value = chain.stabilized_usd.0;
                                (utxo_count, btc_value, usd_value)
                            })
                            .collect();
                        
                        // Collect price statistics
                        let mut min_price = f64::MAX;
                        let mut max_price = 0.0;
                        let mut total_price = 0.0;
                        let mut price_count = 0;
                        let mut price_sources = HashMap::new();
                        
                        for (_, chain) in wallets_lock.values() {
                            for utxo in &chain.utxos {
                                if let Some(price) = utxo.stabilization_price {
                                    if price > 0.0 {
                                        min_price = min_price.min(price);
                                        max_price = max_price.max(price);
                                        total_price += price;
                                        price_count += 1;
                                        
                                        // Count price sources
                                        if let Some(source) = &utxo.stabilization_source {
                                            *price_sources.entry(source.clone()).or_insert(0) += 1;
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Calculate average price
                        let avg_price = if price_count > 0 { total_price / price_count as f64 } else { 0.0 };
                        
                        // If min_price wasn't updated, set it to 0
                        if min_price == f64::MAX {
                            min_price = 0.0;
                        }
                        
                        // Calculate the totals
                        let total_users = wallets_lock.len() as u32;
                        let total_utxos = stats.iter().map(|(utxo, _, _)| *utxo).sum();
                        let total_btc = stats.iter().map(|(_, btc, _)| *btc).sum();
                        let total_usd = stats.iter().map(|(_, _, usd)| *usd).sum();
                        
                        // Create price statistics
                        let price_stats = PriceStatistics {
                            min_stabilization_price: min_price,
                            max_stabilization_price: max_price,
                            avg_stabilization_price: avg_price,
                            price_sources,
                            total_stabilized_utxos: price_count,
                        };
                        
                        (total_users, total_utxos, total_btc, total_usd, price_stats)
                    }; // Lock released here
                    
                    // REPLACE THE EXISTING STATUS UPDATE CODE WITH THIS NEW CODE:
                    // -----------------------------------------------------------
                    // Update service status with the enhanced data
                    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                    let updated = match tokio::time::timeout(
                        Duration::from_secs(3),
                        async {
                            let (users_monitored, total_utxos, total_value_btc, total_value_usd, price_stats) = statistics;
                            let mut status = service_status.lock().await;
                            
                            // First check if we need to update basic stats
                            let stats_changed = status.users_monitored != users_monitored || 
                                               status.total_utxos != total_utxos ||
                                               (status.total_value_btc - total_value_btc).abs() > 0.00001 ||
                                               (status.total_value_usd - total_value_usd).abs() > 0.01;
                                               
                            if stats_changed {
                                // Update the basic stats
                                status.users_monitored = users_monitored;
                                status.total_utxos = total_utxos;
                                status.total_value_btc = total_value_btc;
                                status.total_value_usd = total_value_usd;
                            }
                            
                            // Always update the timestamp
                            status.last_update = timestamp;
                            
                            // Update price sources if we have them
                            if let Ok(price_guard) = tokio::time::timeout(Duration::from_secs(1), price_info_for_status.lock()).await {
                                status.last_price = price_guard.raw_btc_usd;
                                status.price_update_count += 1;
                            }
                            
                            // Update or create utxo_stats with price data
                            if status.utxo_stats.is_none() {
                                status.utxo_stats = Some(UtxoStatistics::default());
                            }
                            
                            if let Some(stats) = status.utxo_stats.as_mut() {
                                stats.total_confirmed_value_btc = total_value_btc;
                                stats.total_stable_value_usd = total_value_usd;
                                stats.price_data = price_stats;
                            }
                            
                            // Update WebSocket status
                            let ws_active = price_feed.is_websocket_connected().await;
                            if status.websocket_active != ws_active {
                                status.websocket_active = ws_active;
                                
                                // Update component health for WebSocket
                                let ws_status = if ws_active { "healthy" } else { "degraded" };
                                let ws_reason = if ws_active { 
                                    None 
                                } else { 
                                    Some("WebSocket disconnected".to_string()) 
                                };
                                
                                // Note: We need to handle WebSocket issues differently
                                if !ws_active {
                                    status.health = "websocket_issue".to_string();
                                }
                            }
                            
                            // Update resource usage metrics (actual implementation)
                            if status.resource_usage.is_none() {
                                status.resource_usage = Some(ResourceUsage {
                                    cpu_percent: 0.0,
                                    memory_mb: 0.0,
                                    disk_operations_per_min: 0,
                                });
                            }
                            
                            // Update with real data
                            if let Some(usage) = status.resource_usage.as_mut() {
                                // Get a simple approximation of CPU usage from process time
                                let start_cpu = std::time::Instant::now();
                                for _ in 0..10000 { let _ = total_value_btc * 1.01; } // Small CPU work
                                let elapsed_cpu = start_cpu.elapsed();
                                usage.cpu_percent = elapsed_cpu.as_micros() as f64 / 100.0;
                                
                                // Collect memory info from process if available
                                #[cfg(target_os = "linux")]
                                if let Ok(mem_info) = std::fs::read_to_string("/proc/self/statm") {
                                    if let Some(mem_val) = mem_info.split_whitespace().next() {
                                        if let Ok(pages) = mem_val.parse::<f64>() {
                                            // Convert pages to MB (pagesize is typically 4KB)
                                            usage.memory_mb = pages * 4.0 / 1024.0;
                                        }
                                    }
                                }
                            }
                            
                            // Update performance metrics
                            if status.performance.is_none() {
                                status.performance = Some(PerformanceMetrics {
                                    avg_sync_time_ms: 0,
                                    avg_price_fetch_time_ms: 0,
                                    max_sync_time_ms: 0,
                                });
                            }
                            
                            // Update with real performance data
                            if let Some(perf) = status.performance.as_mut() {
                                // Get metrics from task manager
                                perf.avg_sync_time_ms = active_tasks_manager.get_avg_sync_time();
                                perf.avg_price_fetch_time_ms = active_tasks_manager.get_avg_price_fetch_time();
                                perf.max_sync_time_ms = active_tasks_manager.get_max_sync_time();
                                
                                // Set reasonable defaults if no data yet
                                if perf.avg_sync_time_ms == 0 {
                                    let elapsed = last_update_time.elapsed().as_millis() as u32;
                                    perf.avg_sync_time_ms = elapsed.min(500); // Use the status update time as a proxy, capped at 500ms
                                }
                            }
                            
                            // Update status based on health checker components
                            let health_status = health_checker.check_all();
                            for (component_name, status_str) in &health_status.components {
                                let reason = health_status.details.get(component_name)
                                    .and_then(|details| details.get("reason").and_then(|r| r.as_str()))
                                    .map(|s| s.to_string());
                                    
                                // Update health status based on component health
                                if component_name == "blockchain" && status_str != "healthy" {
                                    status.health = "blockchain_issue".to_string();
                                } else if component_name == "price_feed" && status_str != "healthy" {
                                    status.health = "price_feed_issue".to_string();
                                } else if component_name == "websocket" && status_str != "healthy" {
                                    status.health = "websocket_issue".to_string();
                                }
                            }
                            
                            debug!("Updated ServiceStatus: {} users, {:.8} BTC, ${:.2} USD, health: {}", 
                                   users, total_value_btc, total_value_usd, status.health);
                                   
                            true
                        }
                    ).await {
                        Ok(true) => true,
                        Ok(false) => {
                            warn!("Failed to update service status");
                            false
                        },
                        Err(_) => {
                            warn!("Timeout updating service status");
                            false
                        }
                    };
                    
                    // *** REVISED SAVE CONDITION STARTS HERE ***
                    // If update was successful, check if we need to save to disk
                    if updated {
                        let status_copy = match tokio::time::timeout(
                            Duration::from_secs(2),
                            async {
                                let status = service_status.lock().await;
                                status.clone()
                            }
                        ).await {
                            Ok(status) => status,
                            Err(_) => {
                                warn!("Timeout getting status copy");
                                continue;
                            }
                        };
                        
                        // Create a hash of important fields
                        let hash_input = format!("{:.8}{:.2}{}{}", 
                            status_copy.total_value_btc,
                            status_copy.total_value_usd,
                            status_copy.users_monitored,
                            status_copy.health
                        );
                        
                        let mut hasher = DefaultHasher::new();
                        hash_input.hash(&mut hasher);
                        let hash = hasher.finish();
                        
                        static LAST_SAVED_HASH: AtomicU64 = AtomicU64::new(0);
                        static LAST_SAVE_TIME: AtomicU64 = AtomicU64::new(0);
                        let last_hash = LAST_SAVED_HASH.load(Ordering::SeqCst);
                        let last_save = LAST_SAVE_TIME.load(Ordering::SeqCst);

                        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

                        // MODIFIED SAVE CONDITION - MORE FREQUENT SAVES
                        let should_save = hash != last_hash || 
                                         (now - last_save) > 60 ||  // Save at least every minute
                                         (status_copy.total_value_btc > 0.0 && (now - last_save) > 30); // More frequent for active users
                        
                        if should_save {
                            if let Err(e) = state_manager.save_service_status(&status_copy).await {
                                warn!("Failed to save service status: {}", e);
                            } else {
                                // Update the atomic trackers after successful save
                                LAST_SAVED_HASH.store(hash, Ordering::SeqCst);
                                LAST_SAVE_TIME.store(now, Ordering::SeqCst);
                                
                                // Only log at INFO level if health changed or significant changes
                                if hash != last_hash {
                                    info!("Saved ServiceStatus: {} users, {:.8} BTC, ${:.2} USD, health: {}", 
                                         status_copy.users_monitored, 
                                         status_copy.total_value_btc, 
                                         status_copy.total_value_usd, 
                                         status_copy.health);
                                } else {
                                    debug!("Saved ServiceStatus after timeout interval");
                                }
                            }
                        } else {
                            trace!("Skipping save for unchanged ServiceStatus");
                        }
                    }
                    // *** REVISED SAVE CONDITION ENDS HERE ***
                    
                    // Measure time since last update and update tracking
                    last_update_time = Instant::now();
                }
            }
        }
        
        info!("Status update handle completed");
        Ok::<(), PulserError>(())
    }
});

// Start cache flush task
let cache_flush_handle = tokio::spawn({
    let state_manager = state_manager.clone();
    let mut shutdown_rx = shutdown_tx.subscribe();
    async move {
        let mut interval = tokio::time::interval(Duration::from_secs(120)); // 2 minutes
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Cache flush task shutting down");
                    break;
                }
                _ = interval.tick() => {
                    match state_manager.flush_dirty_chains().await {
                        Ok(flushed) => {
                            if flushed > 0 {
                                debug!("Flushed {} dirty chains", flushed);
                            }
                        },
                        Err(e) => warn!("Error flushing dirty chains: {}", e),
                    }
                }
            }
        }
        info!("Cache flush task completed");
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

shutdown_tx.send(())?;

tokio::time::sleep(Duration::from_millis(500)).await;

info!("Waiting for tasks to complete...");

// Wait for tasks to complete (they will handle their own cleanup)
let handles = vec![
    deribit_feed_handle,
    ws_health_handle,
    retry_handle,
    monitor_handle,
    sync_handle,
    status_update_handle,
    cache_flush_handle,
    server_handle,
    save_status_handle, // This one runs perform_graceful_shutdown
];

// Give tasks time to shut down gracefully
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
