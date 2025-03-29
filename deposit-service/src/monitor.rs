// deposit-service/src/monitor.rs
use tokio::sync::{Mutex, mpsc, broadcast};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::str::FromStr;
use log::{info, warn, debug, error};
use bdk_wallet::bitcoin::{Address, Network};
use bdk_esplora::esplora_client::AsyncClient;
use bdk_esplora::EsploraAsyncExt;
use common::error::PulserError;
use common::types::PriceInfo;
use crate::wallet::DepositWallet;
use common::{StableChain, UserStatus, ServiceStatus};
use tokio::time::{sleep, interval};
use std::collections::HashMap;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use futures_util::{StreamExt, SinkExt};
use common::price_feed::PriceFeed;
use reqwest::Client;
use serde_json::Value;
use common::StateManager;
use bdk_wallet::KeychainKind;
use common::wallet_utils;
use std::fs; // Added for fs::read_to_string

#[derive(Clone)]
pub struct MonitorConfig {
    pub deposit_window_hours: u64,
    pub batch_size: usize,
    pub esplora_url: String,
    pub websocket_url: String,
    pub fallback_esplora_url: String,
}

impl MonitorConfig {
    pub fn from_toml(config: &toml::Value) -> Self {
        Self {
            deposit_window_hours: config.get("monitor_deposit_window_hours").and_then(|v| v.as_integer()).unwrap_or(24) as u64,
            batch_size: config.get("monitor_batch_size").and_then(|v| v.as_integer()).unwrap_or(15) as usize,
            esplora_url: config.get("esplora_url").and_then(|v| v.as_str()).unwrap_or("https://blockstream.info/testnet/api").to_string(),
            websocket_url: config.get("websocket_url").and_then(|v| v.as_str()).unwrap_or("wss://mempool.space/testnet/api/v1/ws").to_string(),
            fallback_esplora_url: config.get("fallback_esplora_url").and_then(|v| v.as_str()).unwrap_or("https://mempool.space/testnet/api").to_string(),
        }
    }
}

async fn connect_websocket(url: &str) -> Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, PulserError> {
    info!("Attempting to connect to WebSocket: {}", url);
    match connect_async(url).await {
        Ok((ws, _)) => {
            info!("Successfully connected to WebSocket: {}", url);
            Ok(ws)
        }
        Err(e) => {
            error!("WebSocket connection failed: {}", e);
            Err(PulserError::NetworkError(e.to_string()))
        }
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
    let mut interval = interval(Duration::from_secs(60));
    info!("Starting deposit monitor task with batch_size: {}", config.batch_size);
    let mut last_block_height: u64 = 0;
    let mut last_force_sync = 0u64;
    const FORCE_SYNC_INTERVAL: u64 = 120;
    let mut use_fallback = false;
    let mut ws: Option<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> = None;

    if !use_fallback {
        match connect_websocket(&config.websocket_url).await {
            Ok(mut websocket) => {
                if let Err(e) = websocket.send(tokio_tungstenite::tungstenite::Message::Text(
                    r#"{"action":"want","data":["blocks"]}"#.to_string()
                )).await {
                    warn!("Failed to subscribe to WebSocket blocks: {}", e);
                    use_fallback = true;
                } else {
                    ws = Some(websocket);
                }
            }
            Err(e) => {
                warn!("WebSocket failed: {}. Switching to fallback", e);
                use_fallback = true;
            }
        }
    }

    loop {
        let active_user_data = {
            let wallets_lock = wallets.lock().await;
            let statuses_lock = user_statuses.lock().await;
            let mut active_users = Vec::new();
            for (id, _) in wallets_lock.iter() {
                let address = statuses_lock.get(id).map(|s| s.current_deposit_address.clone())
                    .unwrap_or_else(|| "unknown".to_string());
                active_users.push((id.clone(), address));
            }
            info!("Active users to scan: {:?}", active_users.iter().map(|(id, addr)| format!("{}:{}", id, addr)).collect::<Vec<_>>());
            active_users
        };

        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Monitor task shutting down");
                if let Some(mut ws) = ws.take() {
                    ws.close(None).await.ok();
                }
                break;
            }

            msg = async {
                if let Some(ws) = ws.as_mut() {
                    ws.next().await
                } else {
                    futures_util::future::ready(None).await
                }
            }, if !use_fallback => {
                match msg {
                    Some(Ok(msg)) => {
                        let text = msg.into_text()?;
                        let value: Value = serde_json::from_str(&text)?;
                        let new_block_detected = if let Some(block) = value.get("block") {
                            if let Some(height) = block.get("height").and_then(|h| h.as_u64()) {
                                if height > last_block_height {
                                    info!("New block detected (WebSocket): {}", height);
                                    last_block_height = height;
                                    true
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        };

                        if new_block_detected {
                            for (user_id, _) in wallets.lock().await.iter() {
                                debug!("Queuing sync for user {} after block height {}", user_id, last_block_height);
                                if let Err(e) = sync_tx.clone().send(user_id.clone()).await {
                                    warn!("Failed to queue sync for user {}: {}", user_id, e);
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        warn!("WebSocket error: {}. Switching to fallback", e);
                        use_fallback = true;
                        ws = None;
                        {
                            let mut status = service_status.lock().await;
                            status.websocket_active = false;
                            status.silent_failures += 1;
                        }
                    }
                    None => {
                        warn!("WebSocket closed. Switching to fallback");
                        use_fallback = true;
                        ws = None;
                        {
                            let mut status = service_status.lock().await;
                            status.websocket_active = false;
                            status.silent_failures += 1;
                        }
                    }
                }
            }

            _ = sleep(Duration::from_secs(10)), if use_fallback => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                if !ws.is_some() {
                    match connect_websocket(&config.websocket_url).await {
                        Ok(mut websocket) => {
                            if websocket.send(tokio_tungstenite::tungstenite::Message::Text(
                                r#"{"action":"want","data":["blocks"]}"#.to_string()
                            )).await.is_ok() {
                                info!("Reconnected to WebSocket: {}", config.websocket_url);
                                use_fallback = false;
                                ws = Some(websocket);
                                service_status.lock().await.websocket_active = true;
                            }
                        }
                        Err(e) => warn!("WebSocket reconnect failed: {}", e),
                    }
                }
                if now - last_force_sync > FORCE_SYNC_INTERVAL {
                    info!("Performing periodic forced sync for all users (every {} seconds)", FORCE_SYNC_INTERVAL);
                    last_force_sync = now;
                    for (user_id, _) in active_user_data.iter() {
                        debug!("Queuing forced sync for user {}", user_id);
                        if let Err(e) = sync_tx.clone().send(user_id.clone()).await {
                            warn!("Failed to queue forced sync for user {}: {}", user_id, e);
                        }
                    }
                }
            }

            _ = interval.tick() => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                debug!("Processing block at time: {} (height: {})", now, last_block_height);

                for chunk in active_user_data.chunks(config.batch_size) {
                    debug!("Processing batch of {} users", chunk.len());
                    for (user_id, deposit_address) in chunk {
                        let deposit_addr = match Address::from_str(deposit_address).map(|a| a.require_network(Network::Testnet)) {
                            Ok(Ok(addr)) => addr,
                            Ok(Err(e)) => {
                                warn!("Invalid network for address {} of user {}: {}", deposit_address, user_id, e);
                                continue;
                            }
                            Err(e) => {
                                warn!("Failed to parse address {} for user {}: {}", deposit_address, user_id, e);
                                continue;
                            }
                        };

                        let mut wallets_lock = wallets.lock().await;
                        if let Some((wallet, chain)) = wallets_lock.get_mut(user_id) {
                            let change_addr = wallet.wallet.reveal_next_address(KeychainKind::Internal).address;
                            let price_data = price_info.lock().await.clone();
                            let config = crate::wallet_init::Config::from_toml(&toml::from_str(&fs::read_to_string("config/service_config.toml")?)?)?;

                            let new_utxos = wallet_utils::sync_and_stabilize_utxos(
                                user_id, &mut wallet.wallet, &blockchain, chain, price_feed.clone(), &price_data,
                                &deposit_addr, &change_addr, &state_manager, None, config.min_confirmations,
                            ).await?;

                            if !new_utxos.is_empty() {
                                info!("New deposits detected for user {}: {} UTXOs", user_id, new_utxos.len());
                                if let Err(e) = sync_tx.send(user_id.clone()).await {
                                    warn!("Failed to queue sync for user {}: {}", user_id, e);
                                }
                                let mut statuses = user_statuses.lock().await;
                                if let Some(status) = statuses.get_mut(user_id) {
                                    let balance = wallet.wallet.balance();
                                    status.last_deposit_time = Some(now);
                                    status.total_value_btc = balance.confirmed.to_sat() as f64 / 100_000_000.0;
                                    status.confirmations_pending = balance.untrusted_pending.to_sat() > 0;
                                    debug!("Updated status for user {}: {} BTC, pending: {}", 
                                        user_id, status.total_value_btc, status.confirmations_pending);
                                }
                            }

                            let mut last_check_lock = last_activity_check.lock().await;
                            last_check_lock.insert(user_id.clone(), now);
                        } else {
                            warn!("Wallet not found for user {}", user_id);
                        }
                    }
                }
            }
        }
    }
    info!("Monitor task shutdown complete");
    Ok(())
}
