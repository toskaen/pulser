use tokio::sync::{Mutex, mpsc, broadcast};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use std::str::FromStr;
use log::{info, warn, debug, error, trace};
use bdk_wallet::bitcoin::{Address, Network, Amount};
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
use bdk_wallet::{KeychainKind, Wallet};
use bdk_chain::ChainPosition;
use common::wallet_sync::sync_and_stabilize_utxos;
use bdk_chain::spk_client::SyncRequest;
use crate::config::Config;
use bitcoin::Transaction;
use bdk_wallet::bitcoin::Block; // Add to imports
use common::wallet_sync::resync_full_history;


// MonitorConfig: Holds configuration for the deposit monitor, parsed from TOML.
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
    pub max_concurrent_users: usize,
}

impl MonitorConfig {
    // from_toml: Creates a MonitorConfig from a TOML value with defaults.
    pub fn from_toml(config: &toml::Value) -> Self {
        Self {
            deposit_window_hours: config.get("monitor_deposit_window_hours").and_then(|v| v.as_integer()).unwrap_or(24) as u64,
            batch_size: config.get("monitor_batch_size").and_then(|v| v.as_integer()).unwrap_or(15) as usize,
            esplora_url: config.get("esplora_url").and_then(|v| v.as_str()).unwrap_or("https://blockstream.info/testnet/api").to_string(),
            websocket_url: config.get("websocket_url").and_then(|v| v.as_str()).unwrap_or("wss://mempool.space/testnet/api/v1/ws").to_string(),
            fallback_esplora_url: config.get("fallback_esplora_url").and_then(|v| v.as_str()).unwrap_or("https://mempool.space/testnet/api").to_string(),
            fallback_sync_interval_secs: config.get("fallback_sync_interval_secs").and_then(|v| v.as_integer()).unwrap_or(900) as u64,
            websocket_ping_interval_secs: config.get("websocket_ping_interval_secs").and_then(|v| v.as_integer()).unwrap_or(30) as u64,
            websocket_reconnect_max_attempts: config.get("websocket_reconnect_max_attempts").and_then(|v| v.as_integer()).unwrap_or(5) as u32,
            websocket_reconnect_base_delay_secs: config.get("websocket_reconnect_base_delay_secs").and_then(|v| v.as_integer()).unwrap_or(2) as u64,
            max_concurrent_users: config.get("max_concurrent_users").and_then(|v| v.as_integer()).unwrap_or(25) as usize,
        }
    }
}

pub async fn check_mempool_for_address(client: &Client, esplora_url: &str, address: &str) -> Result<Vec<Transaction>, PulserError> {
    let url = format!("{}/address/{}/mempool/txs", esplora_url, address);
    match client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                Ok(response.json().await?)
            } else {
                Ok(Vec::new())
            }
        },
        Err(e) => Err(PulserError::ApiError(format!("Mempool query failed: {}", e)))
    }
}

async fn get_block(client: &Client, esplora_url: &str, height: u64) -> Result<Block, PulserError> {
    let url = format!("{}/block-height/{}", esplora_url, height);
    let response = timeout(Duration::from_secs(10), client.get(&url).send()).await??;
    if response.status().is_success() {
        let block_hash = response.text().await?;
        let block_url = format!("{}/block/{}/raw", esplora_url, block_hash);
        let block_response = timeout(Duration::from_secs(10), client.get(&block_url).send()).await??;
        if block_response.status().is_success() {
            let block_bytes = block_response.bytes().await?;
            Ok(bitcoin::consensus::deserialize(&block_bytes)?)
        } else {
            Err(PulserError::ApiError(format!("Failed to fetch block at height {}: {}", height, block_response.status())))
        }
    } else {
        Err(PulserError::ApiError(format!("Failed to fetch block hash at height {}: {}", height, response.status())))
    }
}

// connect_websocket: Establishes a WebSocket connection with a timeout, logging success/failure.
async fn connect_websocket(url: &str) -> Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, PulserError> {
    info!("Attempting to connect to WebSocket: {}", url);
    match timeout(Duration::from_secs(30), connect_async(url)).await {
        Ok(Ok((ws, _))) => {
            info!("Successfully connected to WebSocket: {}", url);
            Ok(ws)  // Just return the stream
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(e.to_string())),
        Err(_) => Err(PulserError::NetworkError(format!("Connection to {} timed out", url))),
    }
}

// subscribe_to_blocks: Subscribes to block notifications via WebSocket with a timeout.
async fn subscribe_to_blocks(ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> Result<(), PulserError> {
    let subscribe_msg = Message::Text(r#"{"action":"want","data":["blocks"]}"#.to_string());
    timeout(Duration::from_secs(10), ws.send(subscribe_msg)).await??;
    debug!("Successfully subscribed to block notifications");
    Ok(())
}

// send_ping: Sends a ping message to keep the WebSocket alive, with a timeout.
async fn send_ping(ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> Result<(), PulserError> {
    let ping_msg = Message::Ping(vec![1, 2, 3, 4]);
    timeout(Duration::from_secs(5), ws.send(ping_msg)).await??;
    Ok(())
}

// get_blockchain_tip: Fetches the current blockchain tip height from an Esplora URL.
async fn get_blockchain_tip(client: &Client, esplora_url: &str) -> Result<u64, PulserError> {
    let url = format!("{}/blocks/tip/height", esplora_url);
    let response = timeout(Duration::from_secs(10), client.get(&url).send()).await??;
    if response.status().is_success() {
        let text = timeout(Duration::from_secs(5), response.text()).await??;
        Ok(text.trim().parse::<u64>()?)
    } else {
        Err(PulserError::ApiError(format!("Non-success status: {}", response.status())))
    }
}

// prune_inactive_users: Removes users inactive beyond a threshold, saving their StableChain state.
async fn prune_inactive_users(
    activity: &mut HashMap<String, u64>,
    prune_threshold: u64,
    state_manager: &StateManager,
) -> Result<(), PulserError> {
    let now = now_timestamp();
    let before = activity.len();
    let mut to_remove = Vec::new();

    for (user_id, &ts) in activity.iter() {
        let keep = now - ts <= prune_threshold;
        if !keep {
            debug!("Pruning inactive user {} (last active: {})", user_id, ts);
            if let Ok(mut chain) = state_manager.load_stable_chain(user_id).await {
                chain.timestamp = now as i64;
                for attempt in 0..3 {
                    if state_manager.save_stable_chain(user_id, &chain).await.is_ok() {
                        break;
                    }
                    warn!("Retry {} saving StableChain for {}", attempt + 1, user_id);
                    sleep(Duration::from_millis(500)).await;
                }
            }
            to_remove.push(user_id.clone());
        }
    }

    for user_id in to_remove {
        activity.remove(&user_id);
    }
    debug!("Pruned {} users, {} remain", before - activity.len(), activity.len());
    Ok(())
}

// monitor_deposits: Main task to monitor blockchain for deposits, triggering syncs as needed.
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
    info!("Starting deposit monitor task with batch_size: {}", config.monitor_batch_size);
    
    let mut ws: Option<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> = None;
    let mut use_fallback = false;
    let mut reconnect_attempts: u32 = 0;
    let mut last_block_height: u64 = 0;
    let mut last_fallback_sync: u64 = 0;
    let mut last_websocket_activity: Instant = Instant::now();
    let mut last_ping_time: Instant = Instant::now();
    let mut last_sync: HashMap<String, Instant> = HashMap::new();

   if let Ok(height) = get_blockchain_tip(&client, &config.esplora_url).await {
    let mut wallets_lock = wallets.lock().await;
    for (user_id, (wallet, chain)) in wallets_lock.iter_mut() {
        debug!("User {} tip after init: {}", user_id, wallet.wallet.local_chain().tip().height());
        if wallet.wallet.latest_checkpoint().height() == 0 {
            if let Ok(changeset) = state_manager.load_changeset(user_id).await {
                // Log the blockchain tip and wallet tip for comparison
                debug!(
                    "User {}: wallet tip {}, blockchain tip {}, changeset loaded - forcing resync",
                    user_id,
                    wallet.wallet.latest_checkpoint().height(),
                    height
                );
                info!("Found stored changeset for user {}, but need to do full resync", user_id);
                resync_full_history(
                    user_id,
                    &mut wallet.wallet,
                    &wallet.blockchain,
                    chain,
                    &*price_info.lock().await,
                    &state_manager,
                    config.min_confirmations
                ).await?;
            } else if wallet.wallet.latest_checkpoint().height() == 0 {
                debug!(
                    "User {}: wallet tip {}, blockchain tip {}, no changeset - forcing resync",
                    user_id,
                    wallet.wallet.latest_checkpoint().height(),
                    height
                );
                // No changeset or couldn't apply it, and still at height 0, do full resync
                resync_full_history(
                    user_id,
                    &mut wallet.wallet,
                    &wallet.blockchain,
                    chain,
                    &*price_info.lock().await,
                    &state_manager,
                    config.min_confirmations
                ).await?;
            }
        }
    }
}

    // WebSocket connect
match connect_websocket(&config.websocket_url).await {
    Ok(mut websocket) => {
        subscribe_to_blocks(&mut websocket).await?;
        ws = Some(websocket);
        service_status.lock().await.websocket_active = true;
        last_websocket_activity = Instant::now();
    }
    Err(e) => {
        warn!("Initial WebSocket connection failed: {}. Using fallback.", e);
        use_fallback = true;
        service_status.lock().await.websocket_active = false;
    }
}

    let mut fallback_interval = interval(Duration::from_secs(config.fallback_sync_interval_secs));
    let mut ping_interval = interval(Duration::from_secs(config.websocket_ping_interval_secs));

    loop {
tokio::select! {
    // Check if shutdown signal received
    _ = shutdown_rx.recv() => {
        info!("Shutdown signal received for deposit monitor");
        break;
    }
    
    Some(msg_result) = async {
        if let Some(ws_ref) = ws.as_mut() {
            ws_ref.next().await
        } else {
            futures_util::future::ready(None).await
        }
    }, if !use_fallback && ws.is_some() => {
        if let Ok(Message::Text(text)) = msg_result {
            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                if let Some(height) = value.get("block").and_then(|b| b.get("height")).and_then(|h| h.as_u64()) {
                    if height > last_block_height {
                        info!("New block detected (WebSocket): {}", height);
                        last_block_height = height;
                        let mut wallets_lock = wallets.lock().await;
                        for (user_id, (wallet, chain)) in wallets_lock.iter_mut() {
                            let sync_request = wallet.wallet.start_sync_with_revealed_spks();
                            wallet.blockchain.sync(sync_request, 5).await?;
                            let deposit_addr = Address::from_str(&chain.multisig_addr)?.assume_checked();
                            let change_addr = wallet.wallet.peek_address(KeychainKind::Internal, 0).address;
                            let price_info_guard = price_info.lock().await;
                            sync_and_stabilize_utxos(
                                user_id,
                                &mut wallet.wallet,
                                &wallet.blockchain,
                                chain,
                                price_info_guard.raw_btc_usd,
                                &*price_info_guard,
                                &deposit_addr,
                                &change_addr,
                                &state_manager,
                                config.min_confirmations,
                            ).await?;
                        }
                    }
                }
            }
        }
    }
    
    _ = fallback_interval.tick(), if use_fallback || ws.is_none() => {
        let mut wallets_lock = wallets.lock().await;
        for (user_id, (wallet, chain)) in wallets_lock.iter_mut() {
            let deposit_addr = Address::from_str(&chain.multisig_addr)?.assume_checked();
            let change_addr = wallet.wallet.peek_address(KeychainKind::Internal, 0).address;
let price_info_guard = price_info.lock().await;
sync_and_stabilize_utxos(
    user_id,
    &mut wallet.wallet,
    &wallet.blockchain,
    chain,
    price_info_guard.raw_btc_usd,
    &*price_info_guard,  // Dereference here
    &deposit_addr,
    &change_addr,
    &state_manager,
    config.min_confirmations,
).await?;
        }
    }
    
    _ = ping_interval.tick(), if !use_fallback && ws.is_some() => {
        if let Some(ref mut websocket) = ws {
            if let Err(e) = send_ping(websocket).await {
                warn!("WebSocket ping failed: {}", e);
                use_fallback = true;
                ws = None;
            }
        }
    }
}

        // Periodic service status update
        let now = now_timestamp();
        let mut service_status_lock = service_status.lock().await;
        {
            let wallets_lock = wallets.lock().await;
            service_status_lock.total_value_btc = wallets_lock.values().map(|(w, _)| w.wallet.balance().total().to_sat() as f64 / 100_000_000.0).sum();
            service_status_lock.total_utxos = wallets_lock.values().map(|(_, c)| c.utxos.len() as u32).sum();
            service_status_lock.total_value_usd = wallets_lock.values().map(|(_, c)| c.stabilized_usd.0).sum();
        }
        service_status_lock.last_update = now;
        service_status_lock.health = if service_status_lock.websocket_active { "healthy" } else { "degraded" }.to_string();
    }

    info!("Monitor task shutdown complete");
    Ok(())
}

// now_timestamp: Returns the current Unix timestamp in seconds.
fn now_timestamp() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}
