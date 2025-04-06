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
    let semaphore = Arc::new(Semaphore::new(config.max_concurrent_syncs));
    let futures = user_ids.into_iter().map(|user_id| {
        let semaphore = semaphore.clone();
        let price_feed = price_feed.clone();
        let blockchain = blockchain.clone();
        let wallets = wallets.clone();
        let state_manager = state_manager.clone();
        let price_info = price_info.clone();
        async move {
            let _permit = semaphore.acquire().await?;
            // Step 1: Fetch addresses and sync request under lock
            let (deposit_addr, change_addr, sync_request) = {
                let mut wallets_lock = wallets.lock().await;
                if let Some((wallet, _)) = wallets_lock.get_mut(&user_id) {
                    let deposit_addr = Address::from_str(&wallet.stable_chain.multisig_addr)?.assume_checked();
                    let change_addr = wallet.wallet.reveal_next_address(KeychainKind::Internal).address;
                    let sync_request = wallet.wallet.start_sync_with_revealed_spks();
                    (deposit_addr, change_addr, sync_request)
                } else {
                    return Ok(());
                }
            }; // Lock released here

            // Step 2: Sync outside lock
            timeout(Duration::from_secs(30), blockchain.sync(sync_request, 5)).await??;

            // Step 3: Sync and stabilize with a new lock
            let mut wallets_lock = wallets.lock().await;
            if let Some((wallet, chain)) = wallets_lock.get_mut(&user_id) {
                let new_utxos = sync_and_stabilize_utxos(
                    &user_id,
                    &mut wallet.wallet,
                    &blockchain,
                    chain,
                    price_feed,
                    &price_info,
                    &deposit_addr,
                    &change_addr,
                    &state_manager,
                    config.min_confirmations,
                ).await?;
                if !new_utxos.is_empty() {
                    debug!("Processed batch for user {}: {} new UTXOs", user_id, new_utxos.len());
                }
            }
            Ok::<(), PulserError>(())
        }
    }).collect::<Vec<_>>();
    let results = timeout(Duration::from_secs(config.sync_batch_timeout_secs), join_all(futures)).await?;
    for result in results {
        result?;
    }
    Ok(())
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
) -> Result<(), PulserError> {
    info!("Starting deposit monitor with batch_size: {}", config.monitor.batch_size);

    let mut ws: Option<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> = None;
    let mut use_fallback = false;
    let mut reconnect_attempts = 0;
    let mut last_block_height = get_blockchain_tip(&client, &config.esplora_url).await.unwrap_or(0);
    let mut last_fallback_sync = 0;
    let mut mempool_interval = interval(Duration::from_secs(130)); // 2min10s
    let mut last_sync = HashMap::new(); // Place here, before the loop

    let mut fallback_interval = interval(Duration::from_secs(config.monitor.fallback_sync_interval_secs));
    let mut ping_interval = interval(Duration::from_secs(config.monitor.websocket_ping_interval_secs));
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
                let mut status = service_status.lock().await;
                let wallets_lock = wallets.lock().await;
                status.total_utxos = wallets_lock.values().map(|(_, c)| c.utxos.len() as u32).sum();
                status.total_value_btc = wallets_lock.values().map(|(w, _)| w.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0).sum();
                status.total_value_usd = wallets_lock.values().map(|(_, c)| c.stabilized_usd.0).sum();
                status.last_update = now_timestamp() as u64;
                status.health = if status.websocket_active { "healthy" } else { "degraded" }.to_string();
            }

            _ = prune_interval.tick() => {
                prune_inactive_users(&mut *last_activity_check.lock().await, config.monitor.deposit_window_hours * 3600, &state_manager).await?;
            }

           _ = mempool_interval.tick() => {
                let wallets_lock = wallets.lock().await;
                for (user_id, (wallet, chain)) in wallets_lock.iter() {
                    let mempool_txs = check_mempool_for_address(&client, &config.esplora_url, &chain.multisig_addr).await?;
                    if !mempool_txs.is_empty() && last_sync.get(user_id).map_or(true, |&t| now_timestamp() as u64 - t > 130) {
                        let txids: Vec<_> = mempool_txs.iter().map(|tx| tx.txid().to_string()).collect();
                        info!("0-conf deposit for user {}: {} txs, TXIDs: {:?}", user_id, mempool_txs.len(), txids);
                        sync_tx.send(user_id.clone()).await?;
                        last_sync.insert(user_id.clone(), now_timestamp() as u64);
                    }
                    let unspent = wallet.wallet.list_unspent().collect::<Vec<_>>();
                    if unspent.iter().any(|u| u.txout.value == Amount::ZERO && matches!(u.chain_position, ChainPosition::Confirmed { .. })) 
                        && last_sync.get(user_id).map_or(true, |&t| now_timestamp() as u64 - t > 130) {
                        warn!("Withdrawal detected for user {}", user_id);
                        sync_tx.send(user_id.clone()).await?;
                        last_sync.insert(user_id.clone(), now_timestamp() as u64);
                    }
                }
            }
        }
    } // Close loop
    Ok(())
} // Close function
