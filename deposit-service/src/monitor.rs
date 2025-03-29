// deposit-service/src/monitor.rs
use tokio::sync::{Mutex, mpsc, broadcast};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
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
                match timeout(Duration::from_secs(10), websocket.send(tokio_tungstenite::tungstenite::Message::Text(
                    r#"{"action":"want","data":["blocks"]}"#.to_string()
                ))).await {
                    Ok(Ok(_)) => {
                        ws = Some(websocket);
                        debug!("Successfully subscribed to block notifications");
                    }
                    _ => {
                        warn!("Failed to subscribe to WebSocket, switching to fallback");
                        use_fallback = true;
                    }
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
            for (id, (_, chain)) in wallets_lock.iter() {
                let address = statuses_lock.get(id).map(|s| s.current_deposit_address.clone())
                    .unwrap_or_else(|| chain.multisig_addr.clone());
                active_users.push((id.clone(), address));
            }
            info!("Active users to scan: {:?}", active_users.iter().map(|(id, addr)| format!("{}:{}", id, addr)).collect::<Vec<_>>());
            active_users
        };

        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Monitor task shutting down");
                if let Some(mut ws) = ws.take() {
                    timeout(Duration::from_secs(5), ws.close(None)).await.ok();
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
                        if let Some(height) = value.get("block").and_then(|b| b.get("height").and_then(|h| h.as_u64())) {
                            if height > last_block_height {
                                info!("New block detected (WebSocket): {}", height);
                                last_block_height = height;
                                for (user_id, _) in active_user_data.iter() {
                                    sync_tx.send(user_id.clone()).await?;
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        warn!("WebSocket error: {}. Switching to fallback", e);
                        use_fallback = true;
                        ws = None;
                        service_status.lock().await.websocket_active = false;
                    }
                    None => {
                        warn!("WebSocket closed. Switching to fallback");
                        use_fallback = true;
                        ws = None;
                        service_status.lock().await.websocket_active = false;
                    }
                }
            }

            _ = sleep(Duration::from_secs(10)), if use_fallback => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                if now - last_force_sync > FORCE_SYNC_INTERVAL {
                    info!("Forced sync for all users");
                    last_force_sync = now;
                    for (user_id, _) in active_user_data.iter() {
                        sync_tx.send(user_id.clone()).await?;
                    }
                }
            }

            _ = interval.tick() => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                debug!("Processing block at time: {}", now);
                for (user_id, deposit_address) in active_user_data {
                    let deposit_addr = match Address::from_str(&deposit_address) {
                        Ok(addr) => addr.assume_checked(),
                        Err(e) => {
                            warn!("Invalid address {} for user {}: {}", deposit_address, user_id, e);
                            continue;
                        }
                    };
                    let mut wallets_lock = wallets.lock().await;
                    if let Some((wallet, _)) = wallets_lock.get_mut(&user_id) {
                        let price_data = price_info.lock().await.clone();
                        let new_utxos = wallet.update_stable_chain(&price_data).await?;
                        if !new_utxos.is_empty() {
                            info!("New deposits for user {}: {} UTXOs", user_id, new_utxos.len());
                            let new_deposit_addr = wallet.reveal_new_address().await?;
                            let mut statuses = user_statuses.lock().await;
                            if let Some(status) = statuses.get_mut(&user_id) {
                                status.current_deposit_address = new_deposit_addr.to_string();
                                status.last_deposit_time = Some(now);
                                status.total_value_btc = wallet.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0;
                                status.confirmations_pending = wallet.wallet.balance().untrusted_pending.to_sat() > 0;
                                // No save here - keep UserStatus in memory
                            }
                            sync_tx.send(user_id.clone()).await?;
                        }
                        last_activity_check.lock().await.insert(user_id.clone(), now);
                    }
                }
            }
        }
    }
    info!("Monitor task shutdown complete");
    Ok(())
}
