use tokio::sync::{Mutex, mpsc, broadcast};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::str::FromStr;
use log::{info, warn, debug, error};
use bdk_wallet::bitcoin::{Address, Network, Txid};
use bdk_chain::ChainPosition;
use bdk_esplora::esplora_client; // Add this for Builder
use bdk_esplora::esplora_client::AsyncClient;
use bdk_esplora::EsploraAsyncExt;
use common::error::PulserError;
use common::types::PriceInfo;
use crate::wallet::DepositWallet;
use crate::types::{StableChain, UserStatus};
use tokio::time::{sleep, interval};
use std::collections::HashMap;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use futures_util::{StreamExt, SinkExt};
use common::price_feed::PriceFeed;
use reqwest::Client;
use serde_json::Value;

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
    blockchain: AsyncClient,
) -> Result<(), PulserError> {
    let mut interval = interval(Duration::from_secs(60));
    info!("Starting deposit monitor task with batch_size: {}", config.batch_size);
    let mut last_block_height = 0;
    let mut use_fallback = false;

    loop {
        // Periodic wallet sync with retry and fallback
        {
            let mut wallets_lock = wallets.lock().await;
            for (user_id, (wallet, _)) in wallets_lock.iter_mut() {
                let mut attempts = 0;
                const MAX_ATTEMPTS: u32 = 3;
                let mut current_blockchain = blockchain.clone(); // Clone the primary client

                while attempts < MAX_ATTEMPTS {
                    let request = wallet.wallet.start_full_scan();
                    match current_blockchain.full_scan(request, 10, 5).await {
                        Ok(update) => {
                            wallet.wallet.apply_update(update)?;
                            let sync_height = wallet.wallet.latest_checkpoint().height();
                            info!("Pre-synced wallet for user {} to height {}", user_id, sync_height);
                            break;
                        }
                        Err(e) => {
                            attempts += 1;
                            warn!(
                                "Failed to pre-sync wallet for user {} with {} (attempt {}/{}): {}",
                                user_id, if attempts == 1 { &config.esplora_url } else { &config.fallback_esplora_url },
                                attempts, MAX_ATTEMPTS, e
                            );
                            if attempts == MAX_ATTEMPTS {
                                error!("Exhausted retries for user {}. Skipping sync.", user_id);
                            } else if attempts == 1 {
                                // Switch to fallback after first failure
                                current_blockchain = esplora_client::Builder::new(&config.fallback_esplora_url)
                                    .build_async()
                                    .map_err(|e| PulserError::NetworkError(e.to_string()))?;
                                info!("Switching to fallback Esplora: {}", config.fallback_esplora_url);
                            }
                            sleep(Duration::from_secs(2)).await;
                        }
                    }
                }
            }
        }

        let mut ws = if !use_fallback {
            match connect_websocket(&config.websocket_url).await {
                Ok(mut ws) => {
                    if let Err(e) = ws.send(tokio_tungstenite::tungstenite::Message::Text(
                        r#"{"action":"want","data":["blocks"]}"#.to_string()
                    )).await {
                        warn!("Failed to subscribe to WebSocket blocks: {}", e);
                        use_fallback = true;
                        None
                    } else {
                        Some(ws)
                    }
                }
                Err(e) => {
                    warn!("WebSocket failed: {}. Switching to fallback", e);
                    use_fallback = true;
                    None
                }
            }
        } else {
            None
        };

        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Monitor task shutting down");
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
                        let text = match msg.into_text() {
                            Ok(text) => {
                                debug!("WebSocket message received: {}", text);
                                text
                            }
                            Err(e) => {
                                warn!("Failed to parse WebSocket message: {}", e);
                                continue;
                            }
                        };
                        let value: Value = match serde_json::from_str(&text) {
                            Ok(value) => value,
                            Err(e) => {
                                warn!("Failed to deserialize WebSocket message: {}", e);
                                continue;
                            }
                        };
                        if let Some(height) = value.get("block-height").and_then(|v| v.as_u64()) {
                            debug!("Block height from WebSocket: {}", height);
                            if height <= last_block_height {
                                debug!("Block height {} not newer than last: {}", height, last_block_height);
                                continue;
                            }
                            info!("New block detected (WebSocket): {}", height);
                            last_block_height = height;
                        } else {
                            debug!("No block-height in WebSocket message: {}", text);
                            if let Some(blocks) = value.get("blocks").and_then(|v| v.as_array()) {
                                if let Some(first_block) = blocks.first() {
                                    if let Some(height) = first_block.get("height").and_then(|h| h.as_u64()) {
                                        if height > last_block_height {
                                            info!("New block detected from array (WebSocket): {}", height);
                                            last_block_height = height;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        warn!("WebSocket error: {}. Switching to fallback", e);
                        use_fallback = true;
                        continue;
                    }
                    None => {
                        warn!("WebSocket closed. Switching to fallback");
                        use_fallback = true;
                        continue;
                    }
                }
            }
            _ = sleep(Duration::from_secs(10)), if use_fallback => {
                match common::price_feed::fetch_btc_usd_price(&client).await {
                    Ok(price) => *price_info.lock().await = price,
                    Err(e) => warn!("Failed to fetch BTC/USD price: {}", e),
                };
                let height = match client.get(format!("{}/blocks/tip/height", config.fallback_esplora_url)).send().await {
                    Ok(response) => match response.text().await {
                        Ok(text) => match text.parse::<u64>() {
                            Ok(height) => {
                                debug!("Fetched block height (fallback): {}, last: {}", height, last_block_height);
                                height
                            }
                            Err(e) => {
                                warn!("Failed to parse block height from fallback: {}", e);
                                continue;
                            }
                        },
                        Err(e) => {
                            warn!("Failed to get text from fallback response: {}", e);
                            continue;
                        }
                    },
                    Err(e) => {
                        warn!("Failed to fetch block height from fallback: {}", e);
                        continue;
                    }
                };
                if height <= last_block_height {
                    debug!("Block height {} not newer than last: {}", height, last_block_height);
                    continue;
                }
                info!("New block detected (fallback): {}", height);
                last_block_height = height;
            }
            _ = interval.tick() => {
                debug!("Periodic sync tick at height {}", last_block_height);
            }
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        debug!("Processing block at time: {} (height: {})", now, last_block_height);

        let active_user_data = {
            let wallets_lock = wallets.lock().await;
            let statuses_lock = user_statuses.lock().await;
            let mut active_users = Vec::new();
            for (id, _) in wallets_lock.iter() {
                let address = statuses_lock.get(id)
                    .map(|s| s.current_deposit_address.clone())
                    .unwrap_or_else(|| {
                        warn!("No status found for user {}, using default address", id);
                        "unknown".to_string()
                    });
                active_users.push((id.clone(), address));
            }
            info!("Active users to scan: {:?}", active_users.iter().map(|(id, addr)| format!("{}:{}", id, addr)).collect::<Vec<_>>());
            active_users
        };

        for chunk in active_user_data.chunks(config.batch_size) {
            debug!("Processing batch of {} users", chunk.len());
            let mut futures = Vec::new();

            for (user_id, deposit_address) in chunk {
                let user_id = user_id.clone();
                let deposit_address = deposit_address.clone();
                let price_data = price_info.lock().await.clone();
                let price_feed_clone = price_feed.clone();
                let wallets_clone = wallets.clone();
                let statuses_clone = user_statuses.clone();

                let future = async move {
                    debug!("Checking address {} for user {}", deposit_address, user_id);
                    let addr = match Address::from_str(&deposit_address) {
                        Ok(addr) => match addr.require_network(Network::Testnet) {
                            Ok(addr) => addr,
                            Err(e) => {
                                error!("Invalid address network for user {}: {}", user_id, e);
                                return Err(PulserError::BitcoinError(e.to_string()));
                            }
                        },
                        Err(e) => {
                            error!("Failed to parse address {} for user {}: {}", deposit_address, user_id, e);
                            return Err(PulserError::BitcoinError(e.to_string()));
                        }
                    };

                    let utxos = {
                        let mut wallets_lock = wallets_clone.lock().await;
                        match wallets_lock.get_mut(&user_id) {
                            Some((wallet, _)) => {
                                let sync_height = wallet.wallet.latest_checkpoint().height();
                                debug!("Wallet sync height for user {}: {}", user_id, sync_height);
                                match wallet.check_address(&addr, &price_data, &price_feed_clone).await {
                                    Ok(utxos) => {
                                        debug!("Found {} UTXOs for user {} at address {}", utxos.len(), user_id, deposit_address);
                                        if !utxos.is_empty() {
                                            let mut statuses = statuses_clone.lock().await;
                                            if let Some(status) = statuses.get_mut(&user_id) {
                                                status.utxo_count = utxos.len() as u32;
                                                status.last_sync = now;
                                                debug!("Updated status for user {}: {} UTXOs, last sync {}", user_id, status.utxo_count, status.last_sync);
                                            }
                                        }
                                        utxos
                                    }
                                    Err(e) => {
                                        warn!("Failed to check address for user {}: {}", user_id, e);
                                        return Err(e);
                                    }
                                }
                            }
                            None => {
                                error!("Wallet not found for user {}", user_id);
                                return Err(PulserError::UserNotFound(user_id));
                            }
                        }
                    };
                    Ok((user_id, utxos))
                };
                futures.push(future);
            }

            let results = futures::future::join_all(futures).await;
            debug!("Batch completed with {} results", results.len());

            for result in results {
                match result {
                    Ok((user_id, utxos)) => {
                        if utxos.is_empty() {
                            debug!("No UTXOs found for user {}", user_id);
                            continue;
                        }
                        info!("Processing {} UTXOs for user {}", utxos.len(), user_id);

                        for utxo in utxos {
                            let is_new_confirmation = {
                                let wallets_lock = wallets.lock().await;
                                if let Some((_, chain)) = wallets_lock.get(&user_id) {
                                    let is_new = !chain.utxos.iter().any(|u| u.txid == utxo.txid);
                                    debug!("UTXO {} for user {} is new: {}", utxo.txid, user_id, is_new);
                                    is_new
                                } else {
                                    error!("StableChain not found for user {}", user_id);
                                    false
                                }
                            };

                            if is_new_confirmation {
                                let is_confirmed = {
                                    let wallets_lock = wallets.lock().await;
                                    if let Some((wallet, _)) = wallets_lock.get(&user_id) {
                                        match Txid::from_str(&utxo.txid) {
                                            Ok(txid) => {
                                                if let Some(tx) = wallet.wallet.get_tx(txid) {
                                                    let confirmed = matches!(tx.chain_position, ChainPosition::Confirmed { .. });
                                                    debug!("UTXO {} for user {} confirmed: {}", utxo.txid, user_id, confirmed);
                                                    confirmed
                                                } else {
                                                    debug!("Transaction {} not found in wallet for user {}", utxo.txid, user_id);
                                                    false
                                                }
                                            }
                                            Err(e) => {
                                                warn!("Failed to parse txid {} for user {}: {}", utxo.txid, user_id, e);
                                                false
                                            }
                                        }
                                    } else {
                                        error!("Wallet not found for user {} during confirmation check", user_id);
                                        false
                                    }
                                };

                                if is_confirmed {
                                    info!("Confirmed deposit for user {}: txid {}", user_id, utxo.txid);

                                    let mut retries = 3;
                                    while retries > 0 {
                                        match sync_tx.send(user_id.clone()).await {
                                            Ok(()) => {
                                                debug!("Successfully queued sync for user {}", user_id);
                                                break;
                                            }
                                            Err(e) => {
                                                warn!("Sync queue failed for user {}: {}, retrying ({}/3)", user_id, e, 4 - retries);
                                                sleep(Duration::from_secs(1)).await;
                                                retries -= 1;
                                            }
                                        }
                                    }
                                    if retries == 0 {
                                        error!("Failed to queue sync for user {} after retries", user_id);
                                    }

                                    {
                                        let mut statuses_lock = user_statuses.lock().await;
                                        if let Some(status) = statuses_lock.get_mut(&user_id) {
                                            status.last_deposit_time = Some(now);
                                            debug!("Updated last_deposit_time for user {} to {}", user_id, now);
                                        } else {
                                            warn!("Status not found for user {}", user_id);
                                        }

                                        let mut wallets_lock = wallets.lock().await;
                                        if let Some((wallet, _)) = wallets_lock.get_mut(&user_id) {
                                            match wallet.reveal_new_address().await {
                                                Ok(new_addr) => {
                                                    if let Some(status) = statuses_lock.get_mut(&user_id) {
                                                        status.current_deposit_address = new_addr.to_string();
                                                        info!("User {} switched to new address: {}", user_id, new_addr);
                                                    }
                                                }
                                                Err(e) => {
                                                    warn!("Failed to generate new address for user {}: {}", user_id, e);
                                                }
                                            }
                                        } else {
                                            error!("Wallet not found for user {} during address update", user_id);
                                        }
                                    }
                                }
                            }
                        }

                        let mut last_check_lock = last_activity_check.lock().await;
                        last_check_lock.insert(user_id.clone(), now);
                        debug!("Updated last activity check for user {} to {}", user_id, now);
                    }
                    Err(e) => error!("Error processing user deposit check: {}", e),
                }
            }
        }
    }

    info!("Monitor task shutdown complete");
    Ok(())
}
