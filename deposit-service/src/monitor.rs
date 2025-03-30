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
use common::wallet_sync::sync_and_stabilize_utxos;

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
        Ok(Err(e)) => Err(PulserError::NetworkError(e.to_string())),
        Err(_) => Err(PulserError::NetworkError(format!("Connection to {} timed out", url))),
    }
}

async fn subscribe_to_blocks(ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> Result<(), PulserError> {
    let subscribe_msg = Message::Text(r#"{"action":"want","data":["blocks"]}"#.to_string());
    timeout(Duration::from_secs(10), ws.send(subscribe_msg)).await??;
    debug!("Successfully subscribed to block notifications");
    Ok(())
}

async fn send_ping(ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> Result<(), PulserError> {
    let ping_msg = Message::Ping(vec![1, 2, 3, 4]);
    timeout(Duration::from_secs(5), ws.send(ping_msg)).await??;
    Ok(())
}

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
    
    let mut ws: Option<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> = None;
    let mut use_fallback = false;
    let mut reconnect_attempts: u32 = 0;
    let mut last_block_height: u64 = 0;
    let mut last_fallback_sync: u64 = 0;
    let mut last_websocket_activity: Instant = Instant::now();
    let mut last_ping_time: Instant = Instant::now();
    let mut last_sync: HashMap<String, Instant> = HashMap::new();
    let debounce_interval = Duration::from_secs(5);

    let mut check_interval = interval(Duration::from_secs(60));
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
                                match serde_json::from_str::<Value>(&text) {
                                    Ok(value) => {
                                        if let Some(height) = value.get("block").and_then(|b| b.get("height")).and_then(|h| h.as_u64()) {
                                            if height > last_block_height {
                                                info!("New block detected (WebSocket): {}", height);
                                                last_block_height = height;

                                                // Check each user's pending TXs for confirmation
                                                let mut wallets_lock = wallets.lock().await;
                                                for (user_id, deposit_address) in active_user_data.iter() {
                                                    if let Some((wallet, chain)) = wallets_lock.get_mut(user_id) {
                                                        let deposit_addr = Address::from_str(deposit_address)?.assume_checked();
                                                        let unconfirmed_utxos = wallet.wallet.list_unspent()
                                                            .into_iter()
                                                            .filter(|u| u.confirmation_time.is_none() && u.txout.value > 0)
                                                            .collect::<Vec<_>>();

                                                        for utxo in unconfirmed_utxos {
if let Ok(Some(_)) = blockchain.get_tx(&utxo.outpoint.txid.to_string()).await {
    let price_data = price_info.lock().await.clone();
    let deribit_price = price_feed.get_deribit_price().await.unwrap_or(price_data.0); // Fetch post-confirmation
    let change_addr = wallet.wallet.reveal_next_address(KeychainKind::Internal).address;
    let new_utxos = sync_and_stabilize_utxos(
        user_id,
        &mut wallet.wallet,
        &blockchain,
        chain,
        deribit_price, // Raw f64, fetched now
        &price_data,
        &deposit_addr,
        &change_addr,
        &state_manager,
        1,
    ).await?;

if !new_utxos.is_empty() {
    info!("Stabilized {} UTXOs for user {} on block {}", new_utxos.len(), user_id, height);
    let mut statuses = user_statuses.lock().await;
    if let Some(status) = statuses.get_mut(user_id) {
        status.utxo_count = chain.utxos.len() as u32;
        status.total_value_usd = chain.stabilized_usd.0;
        status.last_sync = now_timestamp();
        status.sync_status = "confirmed".to_string();
    }
}
                                                            }
                                                        }

                                                        // Debounced secondary sync
                                                        let now = Instant::now();
                                                        let last = last_sync.entry(user_id.clone()).or_insert(Instant::now());
                                                        if now.duration_since(*last) >= debounce_interval {
                                                            sync_tx.try_send(user_id.clone()).map_err(|e| warn!("Failed to send sync for {}: {}", user_id, e))?;
                                                            *last = now;
                                                            debug!("Queued secondary sync for user {}", user_id);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => warn!("Failed to parse WebSocket JSON: {}", e),
                                }
                            }
                            Message::Ping(_) => {
                                if let Some(ws_ref) = ws.as_mut() {
                                    ws_ref.send(Message::Pong(vec![])).await?;
                                }
                            }
                            Message::Pong(_) => trace!("Received pong"),
                            Message::Close(_) => {
                                info!("WebSocket closed by server");
                                ws = None;
                                use_fallback = true;
                                service_status.lock().await.websocket_active = false;
                            }
                            _ => trace!("Received other WebSocket message"),
                        }
                    }
                    Err(e) => {
                        warn!("WebSocket error: {}. Will reconnect.", e);
                        ws = None;
                        use_fallback = true;
                        service_status.lock().await.websocket_active = false;
                    }
                }
            }

            _ = ping_interval.tick(), if ws.is_some() && !use_fallback => {
                let now = Instant::now();
                let idle_time = now.duration_since(last_websocket_activity).as_secs();
                if idle_time >= config.websocket_ping_interval_secs {
                    debug!("WebSocket idle for {}s, sending ping", idle_time);
                    if let Some(ws_ref) = ws.as_mut() {
                        send_ping(ws_ref).await?;
                        last_ping_time = now;
                    }
                }
                if now.duration_since(last_ping_time).as_secs() >= 2 * config.websocket_ping_interval_secs &&
                   now.duration_since(last_websocket_activity).as_secs() >= 2 * config.websocket_ping_interval_secs {
                    warn!("WebSocket connection stale. Reconnecting.");
                    ws = None;
                    use_fallback = true;
                    service_status.lock().await.websocket_active = false;
                }
            }

            _ = fallback_interval.tick(), if use_fallback || ws.is_none() => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
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
                        Err(e) => {
                            warn!("Reconnect attempt {} failed: {}", reconnect_attempts + 1, e);
                            reconnect_attempts += 1;
                        }
                    }
                }

                if now - last_fallback_sync >= config.fallback_sync_interval_secs {
                    info!("Running fallback sync for {} users", active_user_data.len());
                    last_fallback_sync = now;
                    if let Ok(height) = get_blockchain_tip(&client, &config.esplora_url).await {
                        if height > last_block_height {
                            info!("New block detected (fallback): {}", height);
                            last_block_height = height;
                        }
                    }
                    for (user_id, _) in active_user_data.iter() {
                        sync_tx.try_send(user_id.clone()).map_err(|e| warn!("Failed to send fallback sync for {}: {}", user_id, e))?;
                    }
                }
            }

            _ = check_interval.tick() => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                debug!("Running deposit check at time: {}", now);

                for (user_id, deposit_address) in active_user_data {
                    let deposit_addr = match Address::from_str(&deposit_address) {
                        Ok(addr) => addr.assume_checked(),
                        Err(e) => {
                            warn!("Invalid address {} for user {}: {}", deposit_address, user_id, e);
                            continue;
                        }
                    };

                    let mut wallets_lock = wallets.lock().await;
                    if let Some((wallet, chain)) = wallets_lock.get_mut(&user_id) {
                        let previous_balance = wallet.wallet.balance().confirmed.to_sat();
                        let price_data = price_info.lock().await.clone();
                        let unconfirmed_utxos = wallet.wallet.list_unspent()
                            .into_iter()
                            .filter(|u| u.confirmation_time.is_none() && u.txout.value > 0)
                            .collect::<Vec<_>>();

                        for utxo in unconfirmed_utxos {
if let Ok(Some(_)) = blockchain.get_tx(&utxo.outpoint.txid.to_string()).await {
    let price_data = price_info.lock().await.clone();
    let deribit_price = price_feed.get_deribit_price().await.unwrap_or(price_data.0); // Fetch here
    let change_addr = wallet.wallet.reveal_next_address(KeychainKind::Internal).address;
    let new_utxos = sync_and_stabilize_utxos(
        user_id,
        &mut wallet.wallet,
        &blockchain,
        chain,
        deribit_price, // Pass f64
        &price_data,
        &deposit_addr,
        &change_addr,
        &state_manager,
        1,
    ).await?;

                                if !new_utxos.is_empty() {
                                    info!("Stabilized {} UTXOs for user {} during check", new_utxos.len(), user_id);
                                    let mut statuses = user_statuses.lock().await;
                                    if let Some(status) = statuses.get_mut(user_id) {
                                        status.utxo_count = chain.utxos.len() as u32;
                                        status.total_value_usd = chain.stabilized_usd.0;
                                        status.last_sync = now_timestamp();
                                        status.sync_status = "confirmed".to_string();
                                    }

                                    // Generate new address
                                    if let Ok(new_addr) = wallet.reveal_new_address().await {
                                        statuses.get_mut(user_id).unwrap().current_deposit_address = new_addr.to_string();
                                    }
                                }
                            }
                        }

                        // Debounced secondary sync
                        let now_instant = Instant::now();
                        let last = last_sync.entry(user_id.clone()).or_insert(Instant::now());
                        if now_instant.duration_since(*last) >= debounce_interval {
                            sync_tx.try_send(user_id.clone()).map_err(|e| warn!("Failed to send sync for {}: {}", user_id, e))?;
                            *last = now_instant;
                            debug!("Queued secondary sync for user {}", user_id);
                        }
                    }

                    last_activity_check.lock().await.insert(user_id.clone(), now);
                }

                let mut service_status_lock = service_status.lock().await;
                let wallets_lock = wallets.lock().await;
                service_status_lock.total_utxos = wallets_lock.values().map(|(_, chain)| chain.utxos.len() as u32).sum();
                service_status_lock.total_value_btc = wallets_lock.values().map(|(w, _)| w.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0).sum();
                service_status_lock.total_value_usd = wallets_lock.values().map(|(_, c)| c.stabilized_usd.0).sum();
                service_status_lock.last_update = now;
                service_status_lock.health = "healthy".to_string();
            }
        }
    }

    info!("Monitor task shutdown complete");
    Ok(())
}

fn now_timestamp() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}
