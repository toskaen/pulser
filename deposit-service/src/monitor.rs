use tokio::sync::{Mutex, mpsc, broadcast, Semaphore};
use std::sync::Arc;
use std::time::Duration;
use std::str::FromStr;
use log::{info, warn, debug, error};
use bdk_wallet::bitcoin::{Address, Amount};
use bdk_esplora::esplora_client::AsyncClient;
use bdk_esplora::EsploraAsyncExt;
use common::error::PulserError;
use common::types::{PriceInfo, ServiceStatus, StableChain, UserStatus};
use crate::wallet::DepositWallet;
use tokio::time::{sleep, interval, timeout};
use std::collections::HashMap;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;
use futures_util::{FutureExt, StreamExt, SinkExt, future::join_all};
use common::price_feed::PriceFeed;
use reqwest::Client;
use serde_json::Value;
use common::StateManager;
use bdk_wallet::{KeychainKind, Wallet};
use bdk_chain::ChainPosition;
use common::wallet_sync::sync_and_stabilize_utxos;
use crate::config::{Config, MonitorConfig}; // Replace existing Config import
use backoff::{ExponentialBackoff, backoff::Backoff};
use common::utils::now_timestamp;
use serde_json::json;
use common::webhook::WebhookPayload;
use common::webhook::WebhookManager;
// In monitor.rs - add this import
use bdk_chain::spk_client::SyncResponse as UpdatedSyncState;




use bdk_chain::spk_client::SyncRequest;
use common::UtxoInfo;

// Constants
const MAX_CONCURRENT_SYNCS: usize = 5;
const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 30_000;
const BACKOFF_FACTOR: f64 = 1.5;
const SERVICE_STATUS_UPDATE_INTERVAL_SECS: u64 = 15;

// Retry utility
async fn with_backoff<F, Fut, T>(operation: F, label: &str) -> Result<T, PulserError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, PulserError>>,
{
    let mut backoff = ExponentialBackoff {
        initial_interval: Duration::from_millis(INITIAL_BACKOFF_MS),
        max_interval: Duration::from_millis(MAX_BACKOFF_MS),
        multiplier: BACKOFF_FACTOR,
        max_elapsed_time: Some(Duration::from_secs(60)),
        ..Default::default()
    };
    loop {
        match timeout(Duration::from_secs(30), operation()).await {
            Ok(Ok(result)) => return Ok(result),
            Ok(Err(e)) => {
                if let Some(delay) = backoff.next_backoff() {
                    warn!("{} failed: {}, retrying in {:?}", label, e, delay);
                    sleep(delay).await;
                } else {
                    return Err(PulserError::NetworkError(format!("Max retries exceeded for {}: {}", label, e)));
                }
            }
            Err(_) => {
                if let Some(delay) = backoff.next_backoff() {
                    warn!("{} timed out, retrying in {:?}", label, delay);
                    sleep(delay).await;
                } else {
                    return Err(PulserError::NetworkError(format!("Max retries exceeded for {} due to timeouts", label)));
                }
            }
        }
    }
}

// WebSocket connection
async fn connect_websocket(url: &str) -> Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, PulserError> {
    with_backoff(|| async { connect_async(url).await.map(|(ws, _)| ws).map_err(PulserError::from) }, "WebSocket connect").await
}

// Subscribe to blocks
async fn subscribe_to_blocks(ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> Result<(), PulserError> {
    let msg = Message::Text(r#"{"action":"want","data":["blocks"]}"#.to_string());
    debug!("Sending block subscription: {}", msg);
    timeout(Duration::from_secs(10), ws.send(msg)).await??;
    debug!("Waiting for subscription response...");
    if let Some(Ok(response)) = ws.next().await {
        debug!("Subscription response: {:?}", response);
    }
    debug!("Subscribed to blocks");
    Ok(())
}

// Send ping
async fn send_ping(ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> Result<(), PulserError> {
    timeout(Duration::from_secs(5), ws.send(Message::Ping(vec![1, 2, 3, 4]))).await??;
    Ok(())
}

// In monitor.rs, implement your own simplified version of the lock retry
async fn acquire_lock_with_retry<'a, T>(
    mutex: &'a Arc<Mutex<T>>,
    user_id: &str,
    operation: &str,
    timeout_ms: u64,
    retries: u32
) -> Result<tokio::sync::MutexGuard<'a, T>, PulserError> {
    for attempt in 0..=retries {
        match tokio::time::timeout(
            Duration::from_millis(timeout_ms), 
            mutex.lock()
        ).await {
            Ok(guard) => return Ok(guard),
            Err(_) => {
                if attempt == retries {
                    return Err(PulserError::Timeout(format!(
                        "Timeout acquiring {} lock for user {}", operation, user_id
                    )));
                }
                
                let backoff = timeout_ms * (attempt as u64 + 1);
                warn!("Lock timeout for {} (user: {}), retrying in {}ms", 
                      operation, user_id, backoff);
                tokio::time::sleep(Duration::from_millis(backoff)).await;
            }
        }
    }
    
    // This shouldn't be reached due to the loop structure, but compiler needs it
    Err(PulserError::InternalError("Lock acquisition failed".to_string()))
}

// Get blockchain tip
async fn get_blockchain_tip(client: &Client, esplora_url: &str) -> Result<u64, PulserError> {
    let url = format!("{}/blocks/tip/height", esplora_url);
    let text = with_backoff(|| async { client.get(&url).send().await?.text().await.map_err(PulserError::from) }, "Get blockchain tip").await?;
    text.trim().parse().map_err(|e| PulserError::ApiError(format!("Failed to parse tip: {}", e)))
}

// Check mempool for 0-conf
pub async fn check_mempool_for_address(client: &Client, esplora_url: &str, address: &str) 
    -> Result<Vec<bitcoin::Transaction>, PulserError> {
    
    let url = format!("{}/address/{}/txs/mempool", esplora_url, address);
    
    // Function to attempt the API call with detailed error handling
    let fetch_mempool = || async {
            debug!("Checking mempool for address: {}", url);

        let response = client.get(&url).send().await?;
        
        // Check if response is successful
        if !response.status().is_success() {
            // Include status code and response body in error
            let status = response.status();
            let body = response.text().await.unwrap_or_else(|_| "No response body".to_string());
            
            // Handle common error scenarios
            if status.as_u16() == 429 {
                return Err(PulserError::ApiError(format!("Rate limited by API: {}", status)));
            }
            
            return Err(PulserError::ApiError(format!("API error: {} - {}", status, body)));
        }
        
        // Clone the response bytes to allow both text and json parsing if needed
        let bytes = response.bytes().await?;
        
        // Parse JSON with detailed error information
        match serde_json::from_slice::<Vec<bitcoin::Transaction>>(&bytes) {
            Ok(txs) => Ok(txs),
            Err(e) => {
                // If JSON parsing fails, convert the bytes to a string for debugging
                let raw_body = String::from_utf8_lossy(&bytes);
                warn!("Failed to parse JSON response: {} - Response: {}", e, raw_body);
                Err(PulserError::ApiError(format!("Failed to parse response: {}", e)))
            }
        }
    };
    
    // Use with_backoff to retry with exponential backoff
    with_backoff(fetch_mempool, "Check mempool").await
}

// Prune inactive users
async fn prune_inactive_users(
    activity: &mut HashMap<String, u64>,
    prune_threshold: u64,
    state_manager: &StateManager,
) -> Result<(), PulserError> {
    let now = now_timestamp() as u64;
    let mut to_remove = Vec::new();
    for (user_id, &ts) in activity.iter() {
        if now - ts > prune_threshold {
            debug!("Pruning user {} (inactive since {})", user_id, ts);
            if let Ok(mut chain) = state_manager.load_stable_chain(user_id).await {
                chain.timestamp = now as i64;
                state_manager.save_stable_chain(user_id, &chain).await?;
            }
            to_remove.push(user_id.clone());
        }
    }
    for user_id in to_remove {
        activity.remove(&user_id);
    }
    Ok(())
}

// Process wallets batch

async fn process_wallets_batch(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_ids: Vec<String>,
    price_info: &PriceInfo,
    price_feed: Arc<PriceFeed>,
    state_manager: &StateManager,
    blockchain: &AsyncClient,
    config: &MonitorConfig,
) -> Result<(), PulserError> {
    // IMPROVEMENT 1: Enhanced semaphore with tracking for completed operations
    let semaphore = Arc::new(Semaphore::new(config.max_concurrent_syncs));
    let results = Arc::new(Mutex::new(Vec::with_capacity(user_ids.len())));
    
    // IMPROVEMENT 2: Prepare all futures with better error handling
    let futures = user_ids.into_iter().map(|user_id| {
        let semaphore = semaphore.clone();
        let price_feed = price_feed.clone();
        let blockchain = blockchain.clone();
        let wallets = wallets.clone();
        let state_manager = state_manager.clone();
        let price_info = price_info.clone();
        let results = results.clone();
        
        async move {
            // IMPROVEMENT 3: Acquire semaphore with timeout to avoid blocking indefinitely
            let permit = match timeout(Duration::from_secs(10), semaphore.acquire()).await {
                Ok(Ok(permit)) => permit,
                Ok(Err(e)) => {
                    warn!("Failed to acquire semaphore for user {}: {}", user_id, e);
                    return;
                },
                Err(_) => {
                    warn!("Timeout acquiring semaphore for user {}", user_id);
                    return;
                }
            };
            
            // Track result status
            let mut success = false;
            let mut error_msg = None;
            
            // IMPROVEMENT 4: Extract address information with minimal lock time
let (deposit_addr, change_addr, sync_request) = match extract_wallet_info(&wallets, &user_id, &blockchain).await {
                Ok(info) => info,
                Err(e) => {
                    warn!("Failed to extract wallet info for user {}: {}", user_id, e);
                    drop(permit); // Explicitly release permit
                    return;
                }
            };

            // IMPROVEMENT 5: Perform sync operation outside of lock
            match timeout(
                Duration::from_secs(30), 
                blockchain.sync(sync_request, 5)
            ).await {
                Ok(Ok(update)) => {
                    // IMPROVEMENT 6: Apply update with optimized lock usage
                    match apply_sync_update(
                        &wallets,
                        &user_id,
                        update,
                        &deposit_addr,
                        &change_addr,
                        price_feed,
                        &price_info,
                        &state_manager,
                        config.min_confirmations,
                        &blockchain
                    ).await {
                        Ok(new_utxos) => {
                            if !new_utxos.is_empty() {
                                debug!("Processed batch for user {}: {} new UTXOs", user_id, new_utxos.len());
                            }
                            success = true;
                        },
                        Err(e) => {
                            warn!("Failed to apply sync for user {}: {}", user_id, e);
                            error_msg = Some(e.to_string());
                        }
                    }
                },
                Ok(Err(e)) => {
                    warn!("Blockchain sync failed for user {}: {}", user_id, e);
                    error_msg = Some(e.to_string());
                },
                Err(_) => {
                    warn!("Timeout during blockchain sync for user {}", user_id);
                    error_msg = Some("Blockchain sync timeout".to_string());
                }
            }
            
            // Record result
            let mut results_lock = results.lock().await;
            results_lock.push((user_id.clone(), success, error_msg));
            
            // Permit is automatically dropped here, releasing the semaphore
        }
    }).collect::<Vec<_>>();
    
    // IMPROVEMENT 7: Use timeout for the entire batch
    match timeout(Duration::from_secs(config.sync_batch_timeout_secs), join_all(futures)).await {
        Ok(_) => {
            let results_lock = results.lock().await;
            let success_count = results_lock.iter().filter(|(_, success, _)| *success).count();
            let error_count = results_lock.len() - success_count;
            
            // Log a summary
            if error_count > 0 {
                warn!("Batch processing completed with {}/{} errors", error_count, results_lock.len());
            } else {
                debug!("Batch processing completed successfully for {} users", results_lock.len());
            }
            
            Ok(())
        },
        Err(_) => {
            warn!("Timeout processing wallet batch after {} seconds", config.sync_batch_timeout_secs);
            Err(PulserError::Timeout("Batch processing timeout".to_string()))
        }
    }
}

// IMPROVEMENT 8: Helper function to extract wallet info with minimal lock time
// In monitor.rs - modify the function signature and implementation
async fn extract_wallet_info(
    wallets: &Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_id: &str,
    blockchain: &AsyncClient 
) -> Result<(Address, Address, SyncRequest), PulserError> {
    // Acquire lock with retry and timeout
    let mut wallets_lock = match timeout(
        Duration::from_secs(5),
        acquire_lock_with_retry(wallets, user_id, "wallets", 2000, 3)
    ).await {
        Ok(Ok(lock)) => lock,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(PulserError::Timeout("Timeout acquiring wallets lock".to_string())),
    };
    
    if let Some((wallet, chain)) = wallets_lock.get_mut(user_id) {  // Change get() to get_mut()
        let deposit_addr = Address::from_str(&chain.multisig_addr)?.assume_checked();
        
        // Now wallet is a &mut reference, so we can call reveal_next_address
        let change_addr = wallet.wallet.reveal_next_address(KeychainKind::Internal).address;
        
        // Create a SyncRequest directly with revealed SPKs
        let mut spks = vec![deposit_addr.script_pubkey()];
        
        // Add all old addresses
        for old_addr in &chain.old_addresses {
            if let Ok(addr) = Address::from_str(old_addr) {
                let script_pubkey = addr.assume_checked().script_pubkey();
                if !spks.contains(&script_pubkey) {
                    spks.push(script_pubkey);
                }
            }
        }
        
        // Create the SyncRequest directly
        let sync_request = SyncRequest::builder()
            .spks(spks.into_iter())
            .build();
        
        Ok((deposit_addr, change_addr, sync_request))
    } else {
        Err(PulserError::UserNotFound(format!("Wallet not found for user {}", user_id)))
    }
}

// IMPROVEMENT 9: Helper function to apply sync updates
async fn apply_sync_update(
    wallets: &Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_id: &str,
    update: UpdatedSyncState,
    deposit_addr: &Address,
    change_addr: &Address,
    price_feed: Arc<PriceFeed>,
    price_info: &PriceInfo,
    state_manager: &StateManager,
    min_confirmations: u32,
        blockchain: &AsyncClient // Add blockchain parameter

) -> Result<Vec<UtxoInfo>, PulserError> {
    // Acquire lock with retry and timeout
    let mut wallets_lock = match timeout(
        Duration::from_secs(5),
        acquire_lock_with_retry(wallets, user_id, "wallets", 2000, 3)
    ).await {
        Ok(Ok(lock)) => lock,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(PulserError::Timeout("Timeout acquiring wallets lock".to_string())),
    };
    
    if let Some((wallet, chain)) = wallets_lock.get_mut(user_id) {
        // Apply the update
        wallet.wallet.apply_update(update)?;
        
        // Now sync and stabilize UTXOs
        let new_utxos = sync_and_stabilize_utxos(
            user_id,
            &mut wallet.wallet,
            &blockchain,
            chain,
            price_feed,
            &price_info,
            &deposit_addr,
            &change_addr,
            &state_manager,
            min_confirmations,
        ).await?;
        
        Ok(new_utxos)
    } else {
        Err(PulserError::UserNotFound(format!("Wallet disappeared for user {}", user_id)))
    }
}


// Main deposit monitor
pub async fn monitor_deposits(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    last_activity_check: Arc<Mutex<HashMap<String, u64>>>,
    sync_tx: mpsc::Sender<String>,
    price_info: Arc<Mutex<PriceInfo>>,
    config: Config,
    mut shutdown_rx: broadcast::Receiver<()>,
    price_feed: Arc<PriceFeed>,
    client: Client,
    blockchain: Arc<AsyncClient>,
    state_manager: Arc<StateManager>,
    service_status: Arc<Mutex<ServiceStatus>>,
        webhook_manager: WebhookManager,  // Add this parameter
) -> Result<(), PulserError> {
    info!("Starting deposit monitor with batch_size: {}", config.monitor.batch_size);

    let mut ws: Option<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> = None;
    let mut use_fallback = false;
    let mut reconnect_attempts = 0;
    let mut last_block_height = get_blockchain_tip(&client, &config.esplora_url).await.unwrap_or(0);
    let mut last_fallback_sync = 0;
    let mut mempool_interval = interval(Duration::from_secs(130)); // 2min10s
let last_sync = Arc::new(Mutex::new(HashMap::<String, u64>::new()));

    let mut fallback_interval = interval(Duration::from_secs(config.monitor.fallback_sync_interval_secs));
    let ping_interval = interval(Duration::from_secs(config.monitor.websocket_ping_interval_secs));
    let mut status_interval = interval(Duration::from_secs(SERVICE_STATUS_UPDATE_INTERVAL_SECS));
    let mut prune_interval = interval(Duration::from_secs(config.monitor.fallback_sync_interval_secs / 2));

 let mut ws = if let Ok(mut websocket) = connect_websocket(&config.monitor.websocket_url).await {
        subscribe_to_blocks(&mut websocket).await?;
        service_status.lock().await.websocket_active = true;
        Some(websocket)
    } else {
        use_fallback = true;
        None
    };

    loop {
        let active_users = wallets.lock().await.keys().cloned().collect::<Vec<_>>();

        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Shutting down deposit monitor");
                if let Some(mut ws) = ws.take() {
                    ws.close(None).await.ok();
                }
                break;
            }

msg = async {
    debug!("Polling WebSocket");
    match ws.as_mut() {
        Some(w) => w.next().await.map(|res| res.map_err(PulserError::from)),
        None => None,
    }
} => {
    match msg {
        Some(Ok(Message::Text(text))) => {
            debug!("Received WebSocket message: {}", text);
            match serde_json::from_str::<Value>(&text) {
                Ok(value) => {
                    debug!("Parsed WebSocket message: {:?}", value);
                    let mut new_block_detected = false;

                    // Handle initial "blocks" array (subscription response)
                    if let Some(blocks) = value.get("blocks").and_then(|b| b.as_array()) {
                        for block in blocks {
                            if let Some(height) = block.get("height").and_then(|h| h.as_u64()) {
                                if height > last_block_height {
                                    info!("Initial block update at height {}", height);
                                    last_block_height = height;
                                    new_block_detected = true;
                                }
                            }
                        }
                    }
                    // Handle subsequent "block" updates (new blocks)
                    else if let Some(height) = value.get("block").and_then(|b| b.get("height")).and_then(|h| h.as_u64()) {
                        if height > last_block_height {
                            info!("New block at height {}", height);
                            last_block_height = height;
                            new_block_detected = true;
                        }
                    }
                    // Handle unexpected formats (e.g., "loadingIndicators")
                    else if !value.get("loadingIndicators").is_some() {
                        debug!("Ignoring non-block message: {:?}", value);
                    }

                    // Process wallet batch if a new block was detected
                    if new_block_detected {
                        let price_info_lock = match timeout(Duration::from_secs(5), price_info.lock()).await {
                            Ok(lock) => lock,
                            Err(_) => {
                                warn!("Timeout acquiring price_info lock for block sync at height {}", last_block_height);
                                continue;
                            }
                        };
                        match process_wallets_batch(
                            wallets.clone(),
                            active_users.clone(),
                            &price_info_lock,
                            price_feed.clone(),
                            &state_manager,
                            &blockchain,
                            &config.monitor,
                        ).await {
                            Ok(()) => {
                                debug!("Successfully processed wallets for block {}", last_block_height);
                                for user_id in &active_users {
                                    last_activity_check.lock().await.insert(user_id.clone(), now_timestamp() as u64);
                                    if let Err(e) = sync_tx.send(user_id.clone()).await {
                                        warn!("Failed to send sync signal for user {}: {}", user_id, e);
                                    }
                                }
                                if let Err(e) = prune_inactive_users(
                                    &mut *last_activity_check.lock().await,
                                    config.monitor.deposit_window_hours * 3600,
                                    &state_manager,
                                ).await {
                                    warn!("Failed to prune inactive users after block {}: {}", last_block_height, e);
                                }
                            }
                            Err(e) => {
                                warn!("Failed to process wallets batch for block {}: {}", last_block_height, e);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to parse WebSocket message '{}': {}", text, e);
                }
            }
        }
    Some(Ok(Message::Ping(data))) => {
        debug!("Received ping, sending pong");
        if let Some(ws_inner) = ws.as_mut() {
            if let Err(e) = ws_inner.send(Message::Pong(data)).await {
                warn!("Failed to send pong: {}, reconnecting", e);
                ws = None;
                use_fallback = true;
                service_status.lock().await.websocket_active = false;
            }
        }
    }
    Some(Ok(Message::Pong(_))) => {
        debug!("Received pong, connection healthy");
    }
    Some(Ok(Message::Close(frame))) => {
        warn!("WebSocket closed by server: {:?}", frame);
        ws = None;
        use_fallback = true;
        service_status.lock().await.websocket_active = false;
    }
    Some(Ok(Message::Binary(_))) => {
        debug!("Received binary message, ignoring as not expected for block updates");
    }
    Some(Ok(Message::Frame(_))) => {
        debug!("Received frame message, ignoring as not expected for block updates");
    }
    Some(Err(e)) => {
        warn!("WebSocket error: {}, switching to fallback", e);
        ws = None;
        use_fallback = true;
        service_status.lock().await.websocket_active = false;
    }
    None => {
        warn!("WebSocket stream ended, switching to fallback");
        ws = None;
        use_fallback = true;
        service_status.lock().await.websocket_active = false;
    }
}

    // Attempt reconnection if WebSocket is disconnected
if ws.is_none() && reconnect_attempts < config.monitor.websocket_reconnect_max_attempts {
    debug!("Attempting WebSocket reconnection ({}/{})", reconnect_attempts + 1, config.monitor.websocket_reconnect_max_attempts);
    let delay = config.monitor.websocket_reconnect_base_delay_secs * (1u64 << reconnect_attempts.min(3));
    sleep(Duration::from_secs(delay)).await;
    match connect_websocket(&config.monitor.websocket_url).await {
        Ok(mut websocket) => {
            if subscribe_to_blocks(&mut websocket).await.is_ok() {
                ws = Some(websocket);
                use_fallback = false;
                reconnect_attempts = 0;
                service_status.lock().await.websocket_active = true;
                info!("WebSocket reconnected successfully");
            } else {
                warn!("Failed to resubscribe after reconnection");
                reconnect_attempts += 1;
            }
        }
        Err(e) => {
            warn!("WebSocket reconnection failed: {}", e);
            reconnect_attempts += 1;
        }
    }
}
}

            _ = fallback_interval.tick() => {
                let now = now_timestamp() as u64;
                if ws.is_none() && reconnect_attempts < config.monitor.websocket_reconnect_max_attempts {
                    let delay = config.monitor.websocket_reconnect_base_delay_secs * (1u64 << reconnect_attempts.min(3));
                    sleep(Duration::from_secs(delay)).await;
                    match connect_websocket(&config.monitor.websocket_url).await {
                        Ok(mut websocket) => {
                            subscribe_to_blocks(&mut websocket).await?;
                            ws = Some(websocket);
                            use_fallback = false;
                            reconnect_attempts = 0;
                            service_status.lock().await.websocket_active = true;
                            info!("WebSocket reconnected after {} attempts", reconnect_attempts + 1);
                        }
                        Err(e) => {
                            reconnect_attempts += 1;
                            warn!("WebSocket reconnect attempt {}/{} failed: {}", reconnect_attempts, config.monitor.websocket_reconnect_max_attempts, e);
                        }
                    }
                }
                if use_fallback && now - last_fallback_sync >= config.monitor.fallback_sync_interval_secs {
                    last_fallback_sync = now;
                    let price_info_lock = price_info.lock().await;
                    process_wallets_batch(
                        wallets.clone(),
                        active_users.clone(),
                        &price_info_lock,
                        price_feed.clone(),
                        &state_manager,
                        &blockchain,
                        &config.monitor,
                    ).await?;
                    for user_id in &active_users {
                        last_activity_check.lock().await.insert(user_id.clone(), now);
                    }
                }
            }

            _ = status_interval.tick() => {
    // Collect all the data we need in one go
    let statistics = {
        let wallets_lock = wallets.lock().await;
        
        // Create a Vec containing the stats for each user
        let stats: Vec<(u32, f64, f64)> = wallets_lock.values()
            .map(|(wallet, chain)| {
                let utxo_count = chain.utxos.len() as u32;
                let btc_value = wallet.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0;
                let usd_value = chain.stabilized_usd.0;
                (utxo_count, btc_value, usd_value)
            })
            .collect();
        
        // Calculate the totals
        let total_users = wallets_lock.len() as u32;
        let total_utxos = stats.iter().map(|(utxo, _, _)| *utxo).sum();
        let total_btc = stats.iter().map(|(_, btc, _)| *btc).sum();
        let total_usd = stats.iter().map(|(_, _, usd)| *usd).sum();
        
        (total_users, total_utxos, total_btc, total_usd)
    }; // Lock released here
    
    // Now update the status with a separate lock
    let (users_monitored, total_utxos, total_value_btc, total_value_usd) = statistics;
    {
        let mut status = service_status.lock().await;
        status.users_monitored = users_monitored;
        status.total_utxos = total_utxos;
        status.total_value_btc = total_value_btc;
        status.total_value_usd = total_value_usd;
        status.last_update = now_timestamp() as u64;
        status.health = if status.websocket_active { "healthy" } else { "degraded" }.to_string();
    }
}

            _ = prune_interval.tick() => {
                prune_inactive_users(&mut *last_activity_check.lock().await, config.monitor.deposit_window_hours * 3600, &state_manager).await?;
            }

_ = mempool_interval.tick() => {
    // Clone wallets Arc BEFORE creating futures to avoid ownership issues
    let wallets_for_check = wallets.clone();
    
    // Step 1: First collect all addresses we need to check with a short-lived lock
    let addresses_to_check: Vec<(String, String)> = {
        let wallets_lock = wallets_for_check.lock().await;
        wallets_lock.iter()
            .map(|(user_id, (_, chain))| 
                (user_id.clone(), chain.multisig_addr.clone())
            )
            .collect()
    }; // Lock released here
    
    // Step 2: Process each user's address concurrently with a semaphore to limit concurrent requests
    let semaphore = Arc::new(Semaphore::new(5)); // Limit to 5 concurrent mempool checks
    
    // Create futures for all address checks
    let futures = addresses_to_check.into_iter().map(|(user_id, addr)| {
        // Clone all the values that will be moved into the closure
        let client = client.clone();
        let esplora_url = config.esplora_url.clone();
        let semaphore = semaphore.clone();
        let sync_tx = sync_tx.clone();
        let last_sync = last_sync.clone();
        let webhook_manager = webhook_manager.clone();
        let webhook_url = config.webhook_url.clone();
        let webhook_enabled = config.webhook_enabled;
        
        async move {
            // Acquire permit to limit concurrent requests
            let _permit = match semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => return Err(PulserError::InternalError(
                    "Semaphore was closed".to_string()
                )),
            };
            
            // First check if we should even bother with this user based on last sync time
            let should_check = {
                let last_sync_guard = last_sync.lock().await;
                last_sync_guard.get(&user_id)
                    .map_or(true, |&t| now_timestamp() as u64 - t > 130)
            };
            
            if !should_check {
                return Ok(false); // Skip this user, too soon since last sync
            }
            
            // Check mempool
            match check_mempool_for_address(&client, &esplora_url, &addr).await {
                Ok(mempool_txs) => {
                    if !mempool_txs.is_empty() {
                        let txids: Vec<_> = mempool_txs.iter()
                            .map(|tx| tx.txid().to_string())
                            .collect();
                            
                        info!("0-conf deposit for user {}: {} txs, TXIDs: {:?}", 
                              user_id, mempool_txs.len(), txids);
                        
                        // Optional webhook notification
                        if !webhook_url.is_empty() && webhook_enabled {
                            let total_amount: u64 = mempool_txs.iter()
                                .flat_map(|tx| tx.output.iter())
                                .map(|o| o.value.to_sat())
                                .sum();
                                
                            let webhook_data = json!({
                                "event": "deposit_mempool",
                                "user_id": user_id,
                                "amount_btc": (total_amount as f64) / 100_000_000.0,
                                "amount_sats": total_amount,
                                "tx_count": mempool_txs.len(),
                                "txids": txids,
                                "timestamp": now_timestamp()
                            });
                            
                            let payload = WebhookPayload {
                                event: "deposit_mempool".to_string(),
                                user_id: user_id.clone(),
                                data: webhook_data,
                                timestamp: now_timestamp() as u64,
                            };
                            
                            // Send webhook in a separate task
                            tokio::spawn({
                                let webhook_url = webhook_url.clone();
                                let webhook_manager = webhook_manager.clone();
                                let user_id = user_id.clone();
                                
                                async move {
                                    if let Err(e) = webhook_manager.send(&webhook_url, payload).await {
                                        warn!("Failed to send mempool deposit webhook for user {}: {}", 
                                              user_id, e);
                                    }
                                }
                            });
                        }
                        
                        // Update last sync time
                        {
                            let mut last_sync_guard = last_sync.lock().await;
                            last_sync_guard.insert(user_id.clone(), now_timestamp() as u64);
                        }
                        
                        // Trigger a sync
                        if let Err(e) = sync_tx.send(user_id.clone()).await {
                            warn!("Failed to send sync request for user {}: {}", user_id, e);
                        }
                        
                        return Ok(true); // New deposit found
                    }
                },
                Err(e) => {
                    warn!("Error checking mempool for user {}: {}", user_id, e);
                    return Err(e);
                }
            }
            
            Ok(false) // No action needed
        }
    }).collect::<Vec<_>>();
    
    // Execute all futures with a timeout
    match tokio::time::timeout(
        Duration::from_secs(30), // Overall timeout for mempool checks
        futures::future::join_all(futures)
    ).await {
        Ok(results) => {
            // Count successes and failures
            let mut deposits = 0;
            let mut errors = 0;
            
            for result in results {
                match result {
                    Ok(true) => deposits += 1,
                    Ok(false) => {}, // No action needed
                    Err(_) => errors += 1,
                }
            }
            
            if deposits > 0 || errors > 0 {
                debug!("Mempool check summary: {} deposits, {} errors",
                      deposits, errors);
            }
        },
        Err(_) => {
            warn!("Mempool check timed out after 30 seconds");
        }
    }
}
        }
    } // Close loop
    Ok(())
} // Close function
