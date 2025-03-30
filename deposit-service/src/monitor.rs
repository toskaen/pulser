// deposit-service/src/monitor.rs
use tokio::sync::{Mutex, mpsc, broadcast};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use std::str::FromStr;
use log::{info, warn, debug, error, trace};
use bdk_wallet::bitcoin::{Address, Network};
use bdk_esplora::esplora_client::AsyncClient;
use bdk_esplora::EsploraAsyncExt;
use common::error::PulserError;
use common::types::PriceInfo;
use crate::wallet::DepositWallet;
use common::{StableChain, UserStatus, ServiceStatus};
use tokio::time::{sleep, interval, timeout};
use std::collections::HashMap;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;
use futures_util::{StreamExt, SinkExt};
use common::price_feed::PriceFeed;
use reqwest::Client;
use serde_json::Value;
use common::StateManager;
use bdk_wallet::KeychainKind;
use common::wallet_sync;
use std::fs;

#[derive(Clone)]
pub struct MonitorConfig {
    pub deposit_window_hours: u64,
    pub batch_size: usize,
    pub esplora_url: String,
    pub websocket_url: String,
    pub fallback_esplora_url: String,
    pub fallback_sync_interval_secs: u64,
    pub websocket_ping_interval_secs: u64,
    pub websocket_reconnect_max_attempts: u32,
    pub websocket_reconnect_base_delay_secs: u64,
}

impl MonitorConfig {
    pub fn from_toml(config: &toml::Value) -> Self {
        Self {
            deposit_window_hours: config.get("monitor_deposit_window_hours").and_then(|v| v.as_integer()).unwrap_or(24) as u64,
            batch_size: config.get("monitor_batch_size").and_then(|v| v.as_integer()).unwrap_or(15) as usize,
            esplora_url: config.get("esplora_url").and_then(|v| v.as_str()).unwrap_or("https://blockstream.info/testnet/api").to_string(),
            websocket_url: config.get("websocket_url").and_then(|v| v.as_str()).unwrap_or("wss://mempool.space/testnet/api/v1/ws").to_string(),
            fallback_esplora_url: config.get("fallback_esplora_url").and_then(|v| v.as_str()).unwrap_or("https://mempool.space/testnet/api").to_string(),
            fallback_sync_interval_secs: config.get("fallback_sync_interval_secs").and_then(|v| v.as_integer()).unwrap_or(60) as u64,
            websocket_ping_interval_secs: config.get("websocket_ping_interval_secs").and_then(|v| v.as_integer()).unwrap_or(30) as u64,
            websocket_reconnect_max_attempts: config.get("websocket_reconnect_max_attempts").and_then(|v| v.as_integer()).unwrap_or(5) as u32,
            websocket_reconnect_base_delay_secs: config.get("websocket_reconnect_base_delay_secs").and_then(|v| v.as_integer()).unwrap_or(2) as u64,
        }
    }
}

async fn connect_websocket(url: &str) -> Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, PulserError> {
    info!("Attempting to connect to WebSocket: {}", url);
    match timeout(Duration::from_secs(30), connect_async(url)).await {
        Ok(Ok((ws, _))) => {
            info!("Successfully connected to WebSocket: {}", url);
            Ok(ws)
        }
        Ok(Err(e)) => {
            error!("WebSocket connection failed: {}", e);
            Err(PulserError::NetworkError(e.to_string()))
        }
        Err(_) => {
            error!("WebSocket connection timed out: {}", url);
            Err(PulserError::NetworkError(format!("Connection to {} timed out", url)))
        }
    }
}

async fn subscribe_to_blocks(ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> Result<(), PulserError> {
    let subscribe_msg = Message::Text(r#"{"action":"want","data":["blocks"]}"#.to_string());
    
    match timeout(Duration::from_secs(10), ws.send(subscribe_msg)).await {
        Ok(Ok(_)) => {
            debug!("Successfully subscribed to block notifications");
            Ok(())
        },
        Ok(Err(e)) => {
            warn!("Failed to subscribe to blocks: {}", e);
            Err(PulserError::NetworkError(format!("Failed to subscribe: {}", e)))
        },
        Err(_) => {
            warn!("Timeout subscribing to blocks");
            Err(PulserError::NetworkError("Subscription timeout".to_string()))
        }
    }
}

async fn send_ping(ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> Result<(), PulserError> {
    let ping_msg = Message::Ping(vec![1, 2, 3, 4]);
    
    match timeout(Duration::from_secs(5), ws.send(ping_msg)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => {
            warn!("Failed to send ping: {}", e);
            Err(PulserError::NetworkError(format!("Failed to ping: {}", e)))
        },
        Err(_) => {
            warn!("Timeout sending ping");
            Err(PulserError::NetworkError("Ping timeout".to_string()))
        }
    }
}

async fn get_blockchain_tip(client: &Client, esplora_url: &str) -> Result<u64, PulserError> {
    let url = format!("{}/blocks/tip/height", esplora_url);
    
    match timeout(Duration::from_secs(10), client.get(&url).send()).await {
        Ok(Ok(response)) => {
            if response.status().is_success() {
                match timeout(Duration::from_secs(5), response.text()).await {
                    Ok(Ok(text)) => {
                        match text.trim().parse::<u64>() {
                            Ok(height) => {
                                debug!("Current blockchain tip: {}", height);
                                Ok(height)
                            },
                            Err(e) => {
                                warn!("Failed to parse blockchain height: {}", e);
                                Err(PulserError::ApiError(format!("Failed to parse height: {}", e)))
                            }
                        }
                    },
                    _ => Err(PulserError::ApiError("Failed to read response".to_string()))
                }
            } else {
                Err(PulserError::ApiError(format!("Non-success status: {}", response.status())))
            }
        },
        _ => Err(PulserError::NetworkError("Blockchain tip request failed".to_string()))
    }
}

pub async fn monitor_deposits(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    last_activity_check: Arc<Mutex<HashMap<String, u64>>>,
    sync_tx: mpsc::Sender<String>,
    price_info: Arc<Mutex<PriceInfo>>,
    config: MonitorConfig,
    mut shutdown_rx: broadcast::Receiver<()>,
    price_feed: Arc<PriceFeed>,
    client: Client,
    blockchain: Arc<AsyncClient>,
    state_manager: Arc<StateManager>,
    service_status: Arc<Mutex<ServiceStatus>>,
) -> Result<(), PulserError> {
    info!("Starting deposit monitor task with batch_size: {}", config.batch_size);
    
    // WebSocket connection state
    let mut ws: Option<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> = None;
    let mut use_fallback = false;
    let mut reconnect_attempts: u32 = 0;
    let mut last_block_height: u64 = 0;
    let mut last_fallback_sync: u64 = 0;
    let mut last_websocket_activity: Instant = Instant::now();
    let mut last_ping_time: Instant = Instant::now();
    
    // Intervals
    let mut check_interval = interval(Duration::from_secs(60));
    let mut fallback_interval = interval(Duration::from_secs(config.fallback_sync_interval_secs));
    let mut ping_interval = interval(Duration::from_secs(config.websocket_ping_interval_secs));
    
    // Try to get the current blockchain height on startup
    if let Ok(height) = get_blockchain_tip(&client, &config.esplora_url).await {
        last_block_height = height;
        info!("Initial blockchain height: {}", last_block_height);
    }
    
    // Try to establish initial WebSocket connection
    match connect_websocket(&config.websocket_url).await {
        Ok(mut websocket) => {
            match subscribe_to_blocks(&mut websocket).await {
                Ok(_) => {
                    info!("Successfully connected to WebSocket and subscribed to blocks");
                    ws = Some(websocket);
                    service_status.lock().await.websocket_active = true;
                    last_websocket_activity = Instant::now();
                },
                Err(e) => {
                    warn!("Initial WebSocket subscription failed: {}", e);
                    use_fallback = true;
                    service_status.lock().await.websocket_active = false;
                }
            }
        },
        Err(e) => {
            warn!("Initial WebSocket connection failed: {}. Using fallback method.", e);
            use_fallback = true;
            service_status.lock().await.websocket_active = false;
        }
    }
    
    // Start the monitoring loop
    loop {
        // Get the list of active users to monitor
        let active_user_data = {
            let wallets_lock = wallets.lock().await;
            let statuses_lock = user_statuses.lock().await;
            let mut active_users = Vec::new();
            
            for (id, (_, chain)) in wallets_lock.iter() {
                let address = statuses_lock.get(id)
                    .map(|s| s.current_deposit_address.clone())
                    .unwrap_or_else(|| chain.multisig_addr.clone());
                active_users.push((id.clone(), address));
            }
            
            debug!("Active users to scan: {}", active_users.len());
            trace!("User details: {:?}", active_users.iter().map(|(id, addr)| format!("{}:{}", id, addr)).collect::<Vec<_>>());
            active_users
        };
        
        tokio::select! {
            // Handle shutdown signal
            _ = shutdown_rx.recv() => {
                info!("Monitor task shutting down");
                
                // Gracefully close WebSocket if open
                if let Some(mut websocket) = ws.take() {
                    let _ = timeout(Duration::from_secs(5), websocket.close(None)).await;
                }
                
                break;
            }
            
            // Handle WebSocket messages when connected
            Some(msg_result) = async {
                if let Some(ws_ref) = ws.as_mut() {
                    ws_ref.next().await
                } else {
                    futures_util::future::ready(None).await
                }
            }, if !use_fallback && ws.is_some() => {
                match msg_result {
                    Ok(msg) => {
                        // Reset reconnection counter on successful message
                        reconnect_attempts = 0;
                        last_websocket_activity = Instant::now();
                        
                        match msg {
                            Message::Text(text) => {
                                match serde_json::from_str::<Value>(&text) {
                                    Ok(value) => {
                                        // Try to parse block height from the message
                                        if let Some(height) = value.get("block")
                                            .and_then(|b| b.get("height"))
                                            .and_then(|h| h.as_u64()) 
                                        {
                                            if height > last_block_height {
                                                info!("New block detected (WebSocket): {}", height);
                                                last_block_height = height;
                                                
                                                // Schedule sync for all active users
                                                for (user_id, _) in active_user_data.iter() {
                                                    match sync_tx.try_send(user_id.clone()) {
                                                        Ok(_) => debug!("Scheduled sync for user {} after block {}", user_id, height),
                                                        Err(e) => warn!("Failed to schedule sync for user {}: {}", user_id, e),
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        warn!("Failed to parse WebSocket JSON message: {}", e);
                                        trace!("Raw message: {}", text);
                                    }
                                }
                            },
                            Message::Ping(_) => {
                                // Automatically respond with pong
                                if let Some(ws_ref) = ws.as_mut() {
                                    if let Err(e) = ws_ref.send(Message::Pong(vec![])).await {
                                        warn!("Failed to send pong: {}", e);
                                    }
                                }
                            },
                            Message::Pong(_) => {
                                trace!("Received pong response");
                            },
                            Message::Close(_) => {
                                info!("WebSocket closed by server. Will reconnect.");
                                ws = None;
                                use_fallback = true;
                                service_status.lock().await.websocket_active = false;
                            },
                            _ => {
                                trace!("Received other WebSocket message type");
                            }
                        }
                    },
                    Err(e) => {
                        warn!("WebSocket error: {}. Will reconnect.", e);
                        ws = None;
                        use_fallback = true;
                        service_status.lock().await.websocket_active = false;
                    }
                }
            }
            
            // Periodically check WebSocket connection health
            _ = ping_interval.tick(), if ws.is_some() && !use_fallback => {
                let now = Instant::now();
                let idle_time = now.duration_since(last_websocket_activity).as_secs();
                
                // If no activity for a while, send a ping to check connection
                if idle_time >= config.websocket_ping_interval_secs {
                    debug!("WebSocket idle for {}s, sending ping", idle_time);
                    
                    if let Some(ws_ref) = ws.as_mut() {
                        if let Err(e) = send_ping(ws_ref).await {
                            warn!("WebSocket ping failed: {}. Will reconnect.", e);
                            ws = None;
                            use_fallback = true;
                            service_status.lock().await.websocket_active = false;
                        } else {
                            last_ping_time = now;
                        }
                    }
                }
                
                // If we haven't received any response since the last ping in 2x the interval,
                // assume the connection is stale
                if now.duration_since(last_ping_time).as_secs() >= 2 * config.websocket_ping_interval_secs &&
                   now.duration_since(last_websocket_activity).as_secs() >= 2 * config.websocket_ping_interval_secs 
                {
                    warn!("WebSocket connection appears stale. Reconnecting.");
                    ws = None;
                    use_fallback = true;
                    service_status.lock().await.websocket_active = false;
                }
            }
            
            // Handle reconnection and fallback syncing
            _ = fallback_interval.tick(), if use_fallback || ws.is_none() => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                
                // Try to reconnect if not using too many attempts
                if ws.is_none() && reconnect_attempts < config.websocket_reconnect_max_attempts {
                    // Exponential backoff with jitter
                    let backoff_secs = config.websocket_reconnect_base_delay_secs * 
                        (1u64 << reconnect_attempts.min(10)) + 
                        (now % 3); // Simple jitter
                    
                    info!("Attempting WebSocket reconnect (attempt {}/{}), backoff: {}s", 
                        reconnect_attempts + 1, config.websocket_reconnect_max_attempts, backoff_secs);
                    
                    // Wait for backoff period
                    sleep(Duration::from_secs(backoff_secs)).await;
                    
                    match connect_websocket(&config.websocket_url).await {
                        Ok(mut websocket) => {
                            match subscribe_to_blocks(&mut websocket).await {
                                Ok(_) => {
                                    info!("Successfully reconnected to WebSocket");
                                    ws = Some(websocket);
                                    use_fallback = false;
                                    service_status.lock().await.websocket_active = true;
                                    last_websocket_activity = Instant::now();
                                    reconnect_attempts = 0; // Reset on success
                                },
                                Err(e) => {
                                    warn!("WebSocket subscription failed during reconnect: {}", e);
                                    reconnect_attempts += 1;
                                }
                            }
                        },
                        Err(e) => {
                            warn!("WebSocket reconnection attempt {} failed: {}", reconnect_attempts + 1, e);
                            reconnect_attempts += 1;
                        }
                    }
                }
                
                // Always perform fallback polling if it's been long enough
                if now - last_fallback_sync >= config.fallback_sync_interval_secs {
                    info!("Running fallback sync for {} users", active_user_data.len());
                    last_fallback_sync = now;
                    
                    // Try to fetch latest blockchain height
                    if let Ok(height) = get_blockchain_tip(&client, &config.esplora_url).await {
                        if height > last_block_height {
                            info!("New block detected (fallback polling): {} (was {})", height, last_block_height);
                            last_block_height = height;
                        }
                    }
                    
                    // Sync all users
                    for (user_id, _) in active_user_data.iter() {
                        if let Err(e) = sync_tx.try_send(user_id.clone()) {
                            warn!("Failed to send fallback sync request for {}: {}", user_id, e);
                        } else {
                            debug!("Scheduled fallback sync for user {}", user_id);
                        }
                    }
                }
            }
            
            // Regular interval to check for new deposits directly
            _ = check_interval.tick() => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                debug!("Running deposit check at time: {}", now);
                
                // Process users in batches to avoid holding locks for too long
                for (user_id, deposit_address) in active_user_data {
                    let deposit_addr = match Address::from_str(&deposit_address) {
                        Ok(addr) => addr.assume_checked(),
                        Err(e) => {
                            warn!("Invalid address {} for user {}: {}", deposit_address, user_id, e);
                            continue;
                        }
                    };
                    
                    // Acquire wallet lock for this user only
                    let mut wallets_lock = wallets.lock().await;
                    if let Some((wallet, chain)) = wallets_lock.get_mut(&user_id) {
                        let previous_balance = wallet.wallet.balance().confirmed.to_sat();
                        let previous_utxo_count = chain.utxos.len();
                        
                        // Get latest price data
                        let price_data = price_info.lock().await.clone();
                        
                        // Update wallet state and check for new UTXOs
                        match wallet.update_stable_chain(&price_data).await {
                            Ok(new_utxos) => {
                                let new_balance = wallet.wallet.balance().confirmed.to_sat();
                                
                                if !new_utxos.is_empty() {
                                    info!("New deposits detected for user {}: {} UTXOs, balance change: {} → {} sats", 
                                        user_id, new_utxos.len(), previous_balance, new_balance);
                                    
                                    // Generate new address after receiving deposit
                                    match wallet.reveal_new_address().await {
                                        Ok(new_deposit_addr) => {
                                            info!("Generated new deposit address for user {}: {}", user_id, new_deposit_addr);
                                            
                                            // Update user status
                                            let mut statuses = user_statuses.lock().await;
                                            if let Some(status) = statuses.get_mut(&user_id) {
                                                status.current_deposit_address = new_deposit_addr.to_string();
                                                status.last_deposit_time = Some(now);
                                                status.total_value_btc = new_balance as f64 / 100_000_000.0;
                                                status.confirmations_pending = wallet.wallet.balance().untrusted_pending.to_sat() > 0;
                                                status.utxo_count = chain.utxos.len() as u32;
                                            }
                                        },
                                        Err(e) => warn!("Failed to generate new address for user {}: {}", user_id, e),
                                    }
                                    
                                    // Trigger a full sync to process the deposit
                                    drop(wallets_lock); // Release lock before async operation
                                    if let Err(e) = sync_tx.try_send(user_id.clone()) {
                                        warn!("Failed to send sync request after deposit for {}: {}", user_id, e);
                                    }
                                } else if previous_utxo_count != chain.utxos.len() {
                                    // UTXO count changed but no new deposits (could be spends)
                                    debug!("UTXO count changed for user {}: {} → {}", 
                                        user_id, previous_utxo_count, chain.utxos.len());
                                }
                                
                                // Update last activity timestamp
                                last_activity_check.lock().await.insert(user_id.clone(), now);
                            },
                            Err(e) => {
                                warn!("Failed to update stable chain for user {}: {}", user_id, e);
                            }
                        }
                    }
                }
                
                // Update service status with summary statistics
                let mut service_status_lock = service_status.lock().await;
                let wallets_lock = wallets.lock().await;
                service_status_lock.total_utxos = wallets_lock.values()
                    .map(|(_, chain)| chain.utxos.len() as u32)
                    .sum();
                service_status_lock.total_value_btc = wallets_lock.values()
                    .map(|(wallet, _)| wallet.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0)
                    .sum();
                service_status_lock.total_value_usd = wallets_lock.values()
                    .map(|(_, chain)| chain.stabilized_usd.0)
                    .sum();
                service_status_lock.last_update = now;
                service_status_lock.health = "healthy".to_string();
                
                debug!("Updated service status: {} users, {} UTXOs, {} BTC, ${} USD",
                    wallets_lock.len(), service_status_lock.total_utxos, 
                    service_status_lock.total_value_btc, service_status_lock.total_value_usd);
            }
        }
    }
    
    info!("Monitor task shutdown complete");
    Ok(())
}
