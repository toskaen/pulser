use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, VecDeque};
use log::{info, warn, debug};
use serde_json::json;
use reqwest::Client;
use futures_util::FutureExt;
use std::pin::Pin;
use bitcoin::Address;
use std::str::FromStr;

use bdk_wallet::KeychainKind;
use bdk_chain::spk_client::SyncRequest;
use bdk_chain::ChainPosition;
use bdk_esplora::EsploraAsyncExt;

use common::wallet_sync::{resync_full_history, sync_and_stabilize_utxos};
use common::error::PulserError;
use common::types::{PriceInfo, StableChain, UserStatus, UtxoInfo};
use common::price_feed::PriceFeed;
use common::StateManager;
use common::wallet_sync;

use crate::wallet::DepositWallet;
use common::webhook::{WebhookManager, WebhookPayload};
use crate::config::Config;
use crate::monitor::check_mempool_for_address;
use crate::api::status;

// Sync endpoints
pub fn sync_route(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_info: Arc<Mutex<PriceInfo>>,
    esplora_urls: Arc<Mutex<Vec<(String, u32)>>>,
    esplora: Arc<bdk_esplora::esplora_client::AsyncClient>,
    state_manager: Arc<StateManager>,
    webhook_url: String,
    webhook_manager: WebhookManager,
    client: Client,
    active_tasks_manager: Arc<common::task_manager::UserTaskLock>,
    price_feed: Arc<PriceFeed>,
    service_status: Arc<Mutex<common::types::ServiceStatus>>,
    config: Arc<Config>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::post()
        .and(warp::path!("sync" / String))
        .and(super::with_wallets(wallets))
        .and(super::with_statuses(user_statuses))
        .and(super::with_price(price_info))
        .and(super::with_esplora_urls(esplora_urls))
        .and(warp::any().map(move || Arc::clone(&esplora)))
        .and(super::with_state_manager(state_manager))
        .and(warp::any().map(move || webhook_manager.clone()))
        .and(warp::any().map(move || webhook_url.clone()))
        .and(warp::any().map(move || client.clone()))
        .and(super::with_active_tasks_manager(active_tasks_manager))
        .and(warp::any().map(move || price_feed.clone()))
        .and(super::with_service_status(service_status))
        .and(super::with_config(config))
        .and_then(sync_user_handler)
}

pub fn force_sync(
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    active_tasks_manager: Arc<common::task_manager::UserTaskLock>,
    price_info: Arc<Mutex<PriceInfo>>,
    esplora: Arc<bdk_esplora::esplora_client::AsyncClient>,
    state_manager: Arc<StateManager>,
    config: Arc<Config>
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path!("force_sync" / String)
        .and(warp::post())
        .and(super::with_wallets(wallets))
        .and(super::with_active_tasks_manager(active_tasks_manager))
        .and(super::with_price(price_info))
        .and(warp::any().map(move || Arc::clone(&esplora)))
        .and(super::with_state_manager(state_manager))
        .and(super::with_config(config))
        .and_then(force_sync_handler)
}

async fn force_sync_handler(
    user_id: String,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    active_tasks_manager: Arc<common::task_manager::UserTaskLock>,
    price_info: Arc<Mutex<PriceInfo>>,
    esplora: Arc<bdk_esplora::esplora_client::AsyncClient>,
    state_manager: Arc<StateManager>,
    config: Arc<Config>
) -> Result<impl Reply, Rejection> {
    debug!("Received force_sync request for user {}", user_id);

    let price_feed = Arc::new(PriceFeed::new());

    let mut wallets_lock = match tokio::time::timeout(Duration::from_secs(3), wallets.lock()).await {
        Ok(lock) => lock,
        Err(_) => return Err(PulserError::InternalError("Timeout checking if user exists".to_string()))?,
    };

    let (wallet, chain) = if !wallets_lock.contains_key(&user_id) {
        debug!("User {} not found, initializing wallet", &user_id);
        match DepositWallet::from_config(&config, &user_id, &state_manager, price_feed.clone()).await {
            Ok((wallet, _, chain, _recovery_doc)) => {
                wallets_lock.insert(user_id.clone(), (wallet, chain));
                wallets_lock.get_mut(&user_id).expect("Just inserted")
            },
            Err(e) => return Err(e)?,
        }
    } else {
        match wallets_lock.get_mut(&user_id) {
            Some(entry) => entry,
            None => return Ok(warp::reply::json(&json!({"status": "error", "message": "User not found"})))
        }
    };

    if active_tasks_manager.is_user_active(&user_id).await {
        debug!("User {} already being synced", &user_id);
        return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
    }

    if !active_tasks_manager.mark_user_active(&user_id, "force_sync").await {
        return Ok(warp::reply::json(&json!({"status": "error", "message": "Failed to lock user for sync"})));
    }
    let _guard = super::ActivityGuard { user_id: &user_id, manager: &active_tasks_manager };

    let price_info = match tokio::time::timeout(Duration::from_secs(3), price_info.lock()).await {
        Ok(lock) => lock.clone(),
        Err(_) => return Err(PulserError::InternalError("Timeout acquiring price info".to_string()))?,
    };

    match wallet_sync::resync_full_history(
        &user_id,
        &mut wallet.wallet,
        &esplora,
        chain,
        price_feed.clone(),  // Add price_feed here
        &price_info,
        &state_manager,
        config.min_confirmations,
    ).await {
        Ok(new_utxos) => {
            info!("Full resync completed for user {}: {} new UTXOs", &user_id, new_utxos.len());
            Ok(warp::reply::json(&json!({"status": "ok", "message": format!("Full resync completed, {} new UTXOs", new_utxos.len())})))
        },
        Err(e) => Err(e)?,
    }
}

async fn sync_user_handler(
    user_id: String,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price: Arc<Mutex<PriceInfo>>,
    esplora_urls: Arc<Mutex<Vec<(String, u32)>>>,
    esplora: Arc<bdk_esplora::esplora_client::AsyncClient>,
    state_manager: Arc<StateManager>,
    webhook_manager: WebhookManager,
    webhook_url: String,
    client: Client,
    active_tasks_manager: Arc<common::task_manager::UserTaskLock>,
    price_feed: Arc<PriceFeed>,
    service_status: Arc<Mutex<common::types::ServiceStatus>>,
    config: Arc<Config>,
) -> Result<impl Reply, Rejection> {
    debug!("Received sync request for user {}", user_id);

    let is_active = match tokio::time::timeout(Duration::from_secs(5), active_tasks_manager.is_user_active(&user_id)).await {
        Ok(active) => active,
        Err(_) => return Err(PulserError::InternalError("Timeout checking user status".to_string()))?,
    };

    if is_active {
        debug!("User {} is already being synced", user_id);
        return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
    }

    match tokio::time::timeout(Duration::from_secs(5), active_tasks_manager.mark_user_active(&user_id, "sync")).await {
        Ok(result) => if !result {
            debug!("Failed to mark user {} as active", user_id);
            return Ok(warp::reply::json(&json!({"status": "error", "message": "User already syncing"})));
        },
        Err(_) => return Err(PulserError::InternalError("Timeout acquiring lock".to_string()))?,
    };

    let _guard = super::ActivityGuard { user_id: &user_id, manager: &active_tasks_manager };

    {
        match tokio::time::timeout(Duration::from_secs(5), service_status.lock()).await {
            Ok(mut status) => status.active_syncs += 1,
            Err(_) => warn!("Timeout updating service status for user {}", user_id),
        }
    }

    let result = tokio::time::timeout(Duration::from_secs(120), sync_user(
        &user_id,
        wallets.clone(),
        user_statuses.clone(),
        price.clone(),
        price_feed.clone(),
        esplora_urls.clone(),
        &esplora,
        state_manager.clone(),
        webhook_manager.clone(), 
        &webhook_url,
        client.clone(),
        config.clone(),
    )).await;

    {
        match tokio::time::timeout(Duration::from_secs(5), service_status.lock()).await {
            Ok(mut status) => {
                status.active_syncs = status.active_syncs.saturating_sub(1);
                if let Ok(wallets_lock) = tokio::time::timeout(Duration::from_secs(5), wallets.lock()).await {
                    status.total_utxos = wallets_lock.values().map(|(_, chain)| chain.utxos.len() as u32).sum();
                    status.total_value_btc = wallets_lock.values().map(|(wallet, _)| wallet.wallet.balance().confirmed.to_sat() as f64 / 100_000_000.0).sum();
                    status.total_value_usd = wallets_lock.values().map(|(_, chain)| chain.stabilized_usd.0).sum();
                }
            },
            Err(_) => warn!("Timeout updating service status after sync for user {}", user_id),
        }
    }

    match result {
        Ok(Ok(new_funds)) => {
            let message = if new_funds { "Sync completed with new funds" } else { "Sync completed" };
            Ok(warp::reply::json(&json!({"status": "ok", "message": message})))
        },
        Ok(Err(e)) => Err(e)?,
        Err(_) => Err(PulserError::InternalError("Sync timed out".to_string()))?,
    }
}

pub async fn sync_user(
    user_id: &str,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_info: Arc<Mutex<PriceInfo>>,
    price_feed: Arc<PriceFeed>,
    esplora_urls: Arc<Mutex<Vec<(String, u32)>>>,
    esplora: &bdk_esplora::esplora_client::AsyncClient,
    state_manager: Arc<StateManager>,
    webhook_manager: WebhookManager,
    webhook_url: &str,
    client: Client,
    config: Arc<Config>,
) -> Result<bool, PulserError> {
    let start_time = Instant::now();
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    status::update_user_status_simple(user_id, "syncing", "Starting sync", &user_statuses).await;

    let price_info_data = match tokio::time::timeout(Duration::from_secs(5), price_info.lock()).await {
        Ok(guard) => guard.clone(),
        Err(_) => {
            status::update_user_status_simple(user_id, "error", "Price info timeout", &user_statuses).await;
            return Err(PulserError::InternalError("Price info timeout".to_string()));
        }
    };

    if price_info_data.raw_btc_usd <= 0.0 {
        status::update_user_status_simple(user_id, "error", "Invalid price data", &user_statuses).await;
        return Err(PulserError::PriceFeedError("Invalid price data".to_string()));
    }

    let update_result = {
        let mut wallets_guard = match tokio::time::timeout(Duration::from_secs(10), wallets.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                status::update_user_status_simple(user_id, "error", "Wallet lock timeout", &user_statuses).await;
                return Err(PulserError::InternalError("Wallet lock timeout".to_string()));
            }
        };

        if !wallets_guard.contains_key(user_id) {
            debug!("Initializing wallet for user {}", user_id);
            let (wallet, deposit_info, chain, _recovery_doc) = DepositWallet::from_config(&config, user_id, &state_manager, price_feed.clone()).await?;
            match tokio::time::timeout(Duration::from_secs(5), user_statuses.lock()).await {
                Ok(mut statuses) => {
                    let status_obj = statuses.entry(user_id.to_string()).or_insert_with(|| UserStatus::new(user_id));
                    status_obj.sync_status = "initializing".to_string();
                    status_obj.current_deposit_address = deposit_info.address.clone();
                },
                Err(_) => warn!("Timeout updating status with address for user {}", user_id),
            };
            wallets_guard.insert(user_id.to_string(), (wallet, chain));
            debug!("Wallet initialized for user {}", user_id);
        }

        if let Some((wallet, chain)) = wallets_guard.get_mut(user_id) {
            let mut spks = vec![Address::from_str(&chain.multisig_addr)?.assume_checked().script_pubkey()];
            for i in 0..=wallet.wallet.derivation_index(KeychainKind::External).unwrap_or(0) {
                spks.push(wallet.wallet.peek_address(KeychainKind::External, i).address.script_pubkey());
            }
            let request = SyncRequest::builder().spks(spks.into_iter()).build();
            wallet.blockchain.sync(request, 5).await?;

            match check_mempool_for_address(&client, &config.esplora_url, &chain.multisig_addr).await {
                Ok(mempool_txs) if !mempool_txs.is_empty() => {
                    wallet.wallet.apply_unconfirmed_txs(mempool_txs.iter().map(|tx| (Arc::new(tx.clone()), now as u64)));
                    debug!("Applied {} mempool transactions for user {}", mempool_txs.len(), user_id);
                },
                Ok(_) => {},
                Err(e) => warn!("Mempool check failed for user {}: {}", user_id, e),
            }

            let utxos = wallet.wallet.list_unspent().collect::<Vec<_>>();
            let has_unconfirmed = utxos.iter().any(|u| matches!(u.chain_position, ChainPosition::Unconfirmed { .. }));

            let deposit_addr = Address::from_str(&chain.multisig_addr)?.assume_checked();
            let change_addr = wallet.wallet.reveal_next_address(KeychainKind::Internal).address;
let new_utxos = sync_and_stabilize_utxos(
    user_id,
    &mut wallet.wallet,
    &wallet.blockchain,
    chain,
    price_feed.clone(),  // Use this instead of price_info_data.raw_btc_usd
    &price_info_data,    // Pass a reference to PriceInfo
    &deposit_addr,
    &change_addr,
    &state_manager,
    config.min_confirmations,
).await?;

            if let Some(changeset) = wallet.wallet.take_staged() {
                state_manager.save_changeset(user_id, &changeset).await?;
            }

            let new_funds_detected = !new_utxos.is_empty();
            let new_utxos_clone = new_utxos.clone();
            let chain_clone = chain.clone();
            let balance = wallet.wallet.balance();
            let confirmed_btc = balance.confirmed.to_sat() as f64 / 100_000_000.0;
            let unconfirmed_btc = balance.untrusted_pending.to_sat() as f64 / 100_000_000.0;
            let confirmations_pending = balance.untrusted_pending.to_sat() > 0;
            let utxo_count = chain.utxos.len() as u32;
            let stabilized_usd = chain.stabilized_usd.0;

            Ok((new_funds_detected, new_utxos_clone, chain_clone, confirmed_btc, unconfirmed_btc, confirmations_pending, utxo_count, stabilized_usd))
        } else {
            Err(PulserError::UserNotFound(user_id.to_string()))
        }
    };

    match update_result {
        Ok((new_funds_detected, new_utxos, chain, confirmed_btc, unconfirmed_btc, confirmations_pending, utxo_count, stabilized_usd)) => {
            if new_funds_detected && !webhook_url.is_empty() {
                spawn_webhook_notification(user_id, &new_utxos, &chain, webhook_manager, webhook_url).await;
            }
            status::update_user_status_full(
                user_id,
                "completed",
                utxo_count,
                confirmed_btc,
                stabilized_usd,
                confirmations_pending,
                if new_funds_detected { "Sync completed with new funds" } else { "Sync completed" },
                start_time.elapsed().as_millis() as u64,
                None,
                now,
                &user_statuses,
            ).await;
            if utxo_count > 0 {
                info!("User {} sync complete: {} UTXOs, {} BTC (${:.2})", user_id, utxo_count, confirmed_btc, stabilized_usd);
            }
            Ok(new_funds_detected)
        },
        Err(e) => {
            status::update_user_status_with_error(user_id, "error", &format!("Wallet update failed: {}", e), &user_statuses).await;
            Err(e)
        }
    }
}

/// Sends a webhook notification for new UTXOs with enhanced summary data
///
/// This function creates a rich payload with summary information about the new UTXOs
/// and sends it asynchronously via the WebhookManager, properly handling retries
/// and logging.
async fn spawn_webhook_notification(
    user_id: &str,
    new_utxos: &[UtxoInfo],
    chain: &StableChain,
    webhook_manager: WebhookManager,
    webhook_url: &str,
) {
    // Clone what we need
    let user_id_str = user_id.to_string();
    let webhook_manager = webhook_manager.clone();
    let webhook_url = webhook_url.to_string();
    let new_utxos_cloned = new_utxos.to_vec(); // Clone the utxos
    let chain_clone = chain.clone();
    
    // Calculate summary information
    let total_btc: f64 = new_utxos.iter()
        .map(|u| u.amount_sat as f64 / 100_000_000.0)
        .sum();
    let total_stable_usd: f64 = new_utxos.iter()
        .map(|u| u.stable_value_usd)
        .sum();
    
    // Create payload with richer information
    let payload = WebhookPayload {
        event: "new_funds".to_string(),
        user_id: user_id_str.clone(),
        data: serde_json::json!({
            "utxos": new_utxos_cloned, // Use the cloned data
            "chain": chain_clone,      // Use the cloned chain
            "summary": {
                "total_btc": total_btc,
                "total_stable_usd": total_stable_usd,
                "utxo_count": new_utxos.len(),
                "current_price": chain.raw_btc_usd,
                "timestamp": common::utils::now_timestamp(), // Use utils version
            }
        }),
        timestamp: common::utils::now_timestamp() as u64, // Use utils version
    };
    
    // Launch notification as non-blocking task
    tokio::spawn(async move {
        match webhook_manager.send(&webhook_url, payload).await {
            Ok(_) => {
                debug!("Webhook sent for user {}: {} new UTXOs (${:.2})", 
                      user_id_str, new_utxos_cloned.len(), total_stable_usd);
            },
            Err(e) => {
                warn!("Webhook failed for user {}: {}. Queued for retry.", 
                     user_id_str, e);
            }
        }
    });
}
