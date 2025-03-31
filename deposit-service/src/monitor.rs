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
Ok(mut websocket) => {
    info!("WebSocket connected to {}", config.websocket_url);
    subscribe_to_blocks(&mut websocket).await?;
    ws = Some(websocket);
    service_status.lock().await.websocket_active = true;
    last_websocket_activity = Instant::now();
}
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

    let mut fallback_interval = interval(Duration::from_secs(config.fallback_sync_interval_secs));
    let mut ping_interval = interval(Duration::from_secs(config.websocket_ping_interval_secs));

    if let Ok(height) = get_blockchain_tip(&client, &config.esplora_url).await {
        last_block_height = height;
        info!("Initial blockchain height: {}", last_block_height);
    }

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

    loop {
        let active_user_data = {
            let wallets_lock = match tokio::time::timeout(Duration::from_secs(5), wallets.lock()).await {
                Ok(lock) => lock,
                Err(_) => {
                    warn!("Timeout acquiring wallets lock for user scan");
                    return Ok(());
                }
            };
            let statuses_lock = match tokio::time::timeout(Duration::from_secs(5), user_statuses.lock()).await {
                Ok(lock) => lock,
                Err(_) => {
                    warn!("Timeout acquiring user_statuses lock for user scan");
                    return Ok(());
                }
            };
            let mut active_users = Vec::new();
            for (id, (_, chain)) in wallets_lock.iter() {
                let address = statuses_lock.get(id).map(|s| s.current_deposit_address.clone()).unwrap_or_else(|| chain.multisig_addr.clone());
                active_users.push((id.clone(), address));
            }
            debug!("Active users to scan: {}", active_users.len());
            active_users
        };

        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Monitor task shutting down");
                if let Some(mut websocket) = ws.take() {
                    timeout(Duration::from_secs(5), websocket.close(None)).await.ok();
                }
                break;
            }

            Some(msg_result) = async {
    if let Some(ws_ref) = ws.as_mut() {
        ws_ref.next().await
    } else {
        futures_util::future::ready(None).await
    }
}, if !use_fallback && ws.is_some() => {
    match msg_result {
        Ok(msg) => {
            reconnect_attempts = 0;
            last_websocket_activity = Instant::now();
            match msg {
                Message::Text(text) => {
                    if let Ok(value) = serde_json::from_str::<Value>(&text) {
                        if let Some(height) = value.get("block").and_then(|b| b.get("height")).and_then(|h| h.as_u64()) {
                            if height > last_block_height {
                                info!("New block detected (WebSocket): {}", height);
                                last_block_height = height;
                                
match get_block(&client, &config.esplora_url, height).await {
                                    Ok(block) => {
                                        let mut wallets_lock = wallets.lock().await;
                                        let mut users_to_sync: Vec<(String, String)> = Vec::new();
                                        
                                        for (user_id, (wallet, chain)) in wallets_lock.iter_mut() {
                                            match wallet.wallet.apply_block(&block, height as u32) {
                                                Ok(_) => debug!("Applied block {} to wallet for user {}", height, user_id),
                                                Err(e) => warn!("Failed to apply block {} for user {}: {}", height, user_id, e),
                                            }
                                            
                                            match check_mempool_for_address(&client, &config.esplora_url, &chain.multisig_addr).await {
                                                Ok(mempool_txs) if !mempool_txs.is_empty() => {
                                                    let now = now_timestamp();

wallet.wallet.apply_unconfirmed_txs(
    mempool_txs.iter().map(|tx| (Arc::new(tx.clone()), now as u64))
);
                                                    debug!("Applied {} mempool transactions for user {}", mempool_txs.len(), user_id);
                                                },
                                                Ok(_) => {},
                                                Err(e) => warn!("Mempool check failed for user {}: {}", user_id, e),
                                            }
                                            
                                            let utxos = wallet.wallet.list_unspent().collect::<Vec<_>>();
                                            let has_unconfirmed = utxos.iter().any(|u| 
                                                matches!(u.chain_position, ChainPosition::Unconfirmed { .. })
                                            );
                                            
if has_unconfirmed {
    info!("User {} needs sync: {} UTXOs, has_unconfirmed={}", 
          user_id, utxos.len(), has_unconfirmed);
    users_to_sync.push((user_id.clone(), chain.multisig_addr.clone()));
}
                                            
                                            if let Some(changeset) = wallet.wallet.take_staged() {
                                                if let Err(e) = state_manager.save_changeset(user_id, &changeset).await {
                                                    warn!("Failed to save changeset for user {}: {}", user_id, e);
                                                }
                                            }
                                        }
                                        drop(wallets_lock);
                                        
                                        let price_info_lock = price_info.lock().await;
                                        for chunk in users_to_sync.chunks(config.monitor_batch_size) {
                                            for (user_id, deposit_address) in chunk {
                                                let deposit_addr = Address::from_str(deposit_address)?.assume_checked();
                                                let mut wallets_lock = wallets.lock().await;
                                                if let Some((wallet, chain)) = wallets_lock.get_mut(user_id) {
                                                    let change_addr = wallet.wallet.reveal_next_address(KeychainKind::Internal).address;
                                                    let new_utxos = sync_and_stabilize_utxos(
                                                        user_id,
                                                        &mut wallet.wallet,
                                                        &wallet.blockchain,
                                                        chain,
                                                        price_info_lock.raw_btc_usd,
                                                        &*price_info_lock,
                                                        &deposit_addr,
                                                        &change_addr,
                                                        &state_manager,
                                                        config.min_confirmations,
                                                    ).await?;
                                                    if !new_utxos.is_empty() {
                                                        info!("Stabilized {} UTXOs for user {} on block {}", new_utxos.len(), user_id, height);
                                                    }
                                                    last_activity_check.lock().await.insert(user_id.to_string(), now_timestamp());
                                                }
                                            }
                                        }
                                        
                                        let mut activity = last_activity_check.lock().await;
                                        prune_inactive_users(&mut activity, config.deposit_window_hours * 3600, &state_manager).await?;
                                    }
                                    Err(e) => warn!("Failed to fetch block {}: {}", height, e),
                                }
                            }
                        }
                    }
                }
                Message::Ping(_) => { if let Some(ws_ref) = ws.as_mut() { ws_ref.send(Message::Pong(vec![])).await?; } }
                Message::Pong(_) => trace!("Received pong"),
                Message::Close(_) => {
                    ws = None;
                    use_fallback = true;
                    service_status.lock().await.websocket_active = false;
                }
                _ => trace!("Received other WebSocket message"),
            }
        }
        Err(e) => {
            warn!("WebSocket error: {}. Will reconnect.", e);
            if let Some(mut ws_ref) = ws.take() {
                ws_ref.close(None).await.ok();
            }
            ws = None;
            use_fallback = true;
            service_status.lock().await.websocket_active = false;
        }
    }
}

            _ = ping_interval.tick(), if ws.is_some() && !use_fallback => {
                let now = Instant::now();
                if now.duration_since(last_websocket_activity).as_secs() >= config.websocket_ping_interval_secs {
                    if let Some(ws_ref) = ws.as_mut() {
                        send_ping(ws_ref).await?;
                        last_ping_time = now;
                    }
                }
                if now.duration_since(last_ping_time).as_secs() >= 2 * config.websocket_ping_interval_secs &&
                   now.duration_since(last_websocket_activity).as_secs() >= 2 * config.websocket_ping_interval_secs {
                    warn!("WebSocket connection stale. Reconnecting.");
                    if let Some(mut ws_ref) = ws.take() {
                        ws_ref.close(None).await.ok();
                    }
                    ws = None;
                    use_fallback = true;
                    service_status.lock().await.websocket_active = false;
                }
            }

_ = fallback_interval.tick(), if use_fallback || ws.is_none() => {
    let now = now_timestamp();
    if ws.is_none() && reconnect_attempts < config.websocket_reconnect_max_attempts {
        let backoff_secs = config.websocket_reconnect_base_delay_secs * (1u64 << reconnect_attempts.min(10)) + (now % 3);
        info!("Reconnecting WebSocket (attempt {}/{}), backoff: {}s", reconnect_attempts + 1, config.websocket_reconnect_max_attempts, backoff_secs);
        sleep(Duration::from_secs(backoff_secs)).await;
        match connect_websocket(&config.websocket_url).await {
            Ok(mut websocket) => {
                subscribe_to_blocks(&mut websocket).await?;
                ws = Some(websocket);
                use_fallback = false;
                service_status.lock().await.websocket_active = true;
                last_websocket_activity = Instant::now();
                reconnect_attempts = 0;
            }
            Err(_) => {warn!("Reconnect attempt {}/{} failed: {}", reconnect_attempts + 1, config.websocket_reconnect_max_attempts, e);
                reconnect_attempts += 1;  }
        }
    }
    if now - last_fallback_sync >= config.fallback_sync_interval_secs {
        let mut wallets_lock = wallets.lock().await;
        let mut active_users_with_pending: Vec<(String, String)> = Vec::new();
        for (user_id, (wallet, chain)) in wallets_lock.iter_mut() {
        
// When collecting addresses to monitor
let mut spks = vec![Address::from_str(&chain.multisig_addr)?.assume_checked().script_pubkey()];
debug!("Monitoring current address for user {}: {}", user_id, chain.multisig_addr);

// Add old addresses
for old_addr in &chain.old_addresses {
    debug!("Also monitoring old address for user {}: {}", user_id, old_addr);
    if let Ok(addr) = Address::from_str(old_addr) {
        spks.push(addr.assume_checked().script_pubkey());
    } else {
        warn!("Invalid old address format for user {}: {}", user_id, old_addr);
    }
}

// Add derived addresses (look ahead)
let last_external_idx = wallet.wallet.derivation_index(KeychainKind::External).unwrap_or(0);
for i in 0..=last_external_idx {
    let addr = wallet.wallet.peek_address(KeychainKind::External, i).address;
    debug!("Monitoring derived address {} for user {}: {}", i, user_id, addr);
    spks.push(addr.script_pubkey());
}
// For each user
let request = SyncRequest::builder()
    .spks(spks.into_iter())
    .build();

match wallet.blockchain.sync(request, 5).await {
    Ok(_) => {
        debug!("Successfully synced blockchain for user {}", user_id);
    },
    Err(e) => {
        warn!("Failed to sync blockchain for user {}: {}", user_id, e);
        // Continue processing - don't return early
    }
}

let utxos = wallet.wallet.list_unspent().collect::<Vec<_>>();
debug!("User {} has {} UTXOs after sync", user_id, utxos.len());

            let mut unspent = wallet.wallet.list_unspent();
            if unspent.any(|u| matches!(u.chain_position, ChainPosition::Unconfirmed { .. }) && u.txout.value > Amount::from_sat(0)) {
                debug!("Found unconfirmed UTXO for user {}: {:?}", user_id, wallet.wallet.list_unspent().collect::<Vec<_>>());
                active_users_with_pending.push((user_id.clone(), chain.multisig_addr.clone()));
            }
        }
        drop(wallets_lock);
        let active_count = service_status.lock().await.active_syncs;
        if active_count < config.max_concurrent_users as u32 {
            info!("Running fallback sync for {} users", active_users_with_pending.len());
            last_fallback_sync = now;
            if let Ok(height) = get_blockchain_tip(&client, &config.esplora_url).await {
                if height > last_block_height {
                    info!("New block detected (fallback): {}", height);
                    last_block_height = height;
                }
            }
            let price_info_lock = price_info.lock().await;
            for chunk in active_users_with_pending.chunks(config.monitor_batch_size) {
                for (user_id, deposit_address) in chunk {
                    let deposit_addr = Address::from_str(&deposit_address)?.assume_checked();
                    let mut wallets_lock = wallets.lock().await;
if let Some((wallet, chain)) = wallets_lock.get_mut(user_id) {

                        let change_addr = wallet.wallet.reveal_next_address(KeychainKind::Internal).address;
                        let new_utxos = sync_and_stabilize_utxos(
                            &user_id,
                            &mut wallet.wallet,
                            &wallet.blockchain,
                            chain,
                            price_info_lock.raw_btc_usd,
                            &*price_info_lock,
                            &deposit_addr,
                            &change_addr,
                            &state_manager,
                            config.min_confirmations,
                        ).await?;
                        if !new_utxos.is_empty() {
                            info!("Stabilized {} UTXOs for user {} in fallback", new_utxos.len(), user_id);
                        }
                        last_activity_check.lock().await.insert(user_id.to_string(), now_timestamp());
                    }
                }
            }
        }
        let mut activity = last_activity_check.lock().await;
        prune_inactive_users(&mut activity, config.deposit_window_hours * 3600, &state_manager).await?;
    }
}
}
        let now = now_timestamp();
        let mut service_status_lock = service_status.lock().await;
        service_status_lock.total_utxos = wallets.lock().await.values().map(|(_, c)| c.utxos.len() as u32).sum();
        service_status_lock.total_value_btc = wallets.lock().await.values().map(|(w, _)| w.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0).sum();
        service_status_lock.total_value_usd = wallets.lock().await.values().map(|(_, c)| c.stabilized_usd.0).sum();
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
