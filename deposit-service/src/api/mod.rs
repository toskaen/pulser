use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use std::collections::{HashMap, VecDeque};
use log::{info, warn, debug};
use common::error::PulserError;
use common::StateManager;
use common::task_manager::UserTaskLock;
use common::types::{ServiceStatus, UserStatus, WebhookRetry, StableChain, PriceInfo};
use common::price_feed::PriceFeed;
use crate::wallet::DepositWallet;
use crate::webhook::WebhookConfig;
use crate::config::Config;
use reqwest::Client;
use std::time::Duration;
pub use sync::sync_user; // Add this line near the top or with other re-exports



mod health;
mod user;
mod sync;
mod register;

// ActivityGuard for managing user task lifecycle
struct ActivityGuard<'a> {
    user_id: &'a str,
    manager: &'a Arc<UserTaskLock>,
}

impl<'a> Drop for ActivityGuard<'a> {
    fn drop(&mut self) {
        let user_id = self.user_id.to_string();
        let manager = self.manager.clone();
        tokio::spawn(async move {
            manager.mark_user_inactive(&user_id).await;
        });
    }
}

// Combine all routes
pub fn routes(
    service_status: Arc<Mutex<ServiceStatus>>,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_info: Arc<Mutex<PriceInfo>>,
    esplora_urls: Arc<Mutex<Vec<(String, u32)>>>,
    state_manager: Arc<StateManager>,
    sync_tx: mpsc::Sender<String>,
    webhook_manager: WebhookManager,
    webhook_url: String,
    client: reqwest::Client,
    active_tasks_manager: Arc<UserTaskLock>,
    price_feed: Arc<PriceFeed>,
    esplora: Arc<bdk_esplora::esplora_client::AsyncClient>,
    config: Arc<Config>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    health::health(service_status.clone(), esplora_urls.clone(), price_info.clone(), active_tasks_manager.clone())
        .or(user::user_status(wallets.clone(), user_statuses.clone()))
        .or(user::user_txs(wallets.clone()))
        .or(user::user_address(wallets.clone(), user_statuses.clone()))
        .or(sync::sync_route(
            wallets.clone(),
            user_statuses.clone(),
            price_info.clone(),
            esplora_urls.clone(),
            esplora.clone(),
            state_manager.clone(),
            webhook_url,
            client.clone(),
            active_tasks_manager.clone(),
            price_feed.clone(),
            service_status.clone(),
            config.clone(),
        ))
        .or(sync::force_sync(
            wallets.clone(),
            active_tasks_manager.clone(),
            price_info.clone(),
            esplora.clone(),
            state_manager.clone(),
            config.clone(),
        ))
        .or(register::register(
            config.clone(),
            state_manager.clone(),
            wallets.clone(),
            service_status.clone(),
            user_statuses.clone(),
            price_feed.clone(),
        ))
}

// Filter helpers
pub fn with_wallets(wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>) -> impl Filter<Extract = (Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || wallets.clone())
}

pub fn with_statuses(statuses: Arc<Mutex<HashMap<String, UserStatus>>>) -> impl Filter<Extract = (Arc<Mutex<HashMap<String, UserStatus>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || statuses.clone())
}

pub fn with_esplora_urls(urls: Arc<Mutex<Vec<(String, u32)>>>) -> impl Filter<Extract = (Arc<Mutex<Vec<(String, u32)>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || urls.clone())
}

pub fn with_state_manager(manager: Arc<StateManager>) -> impl Filter<Extract = (Arc<StateManager>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || manager.clone())
}

pub fn with_retry_queue(queue: Arc<Mutex<VecDeque<WebhookRetry>>>) -> impl Filter<Extract = (Arc<Mutex<VecDeque<WebhookRetry>>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || queue.clone())
}

pub fn with_active_tasks_manager(manager: Arc<UserTaskLock>) -> impl Filter<Extract = (Arc<UserTaskLock>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || manager.clone())
}

pub fn with_service_status(status: Arc<Mutex<ServiceStatus>>) -> impl Filter<Extract = (Arc<Mutex<ServiceStatus>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || status.clone())
}

pub fn with_price(price: Arc<Mutex<PriceInfo>>) -> impl Filter<Extract = (Arc<Mutex<PriceInfo>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || price.clone())
}

pub fn with_config(config: Arc<Config>) -> impl Filter<Extract = (Arc<Config>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || config.clone())
}

fn with_client(client: Client) -> impl Filter<Extract = (Client,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || client.clone())
}

fn with_price_feed(price_feed: Arc<PriceFeed>) -> impl Filter<Extract = (Arc<PriceFeed>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || price_feed.clone())
}

// Status update helper functions
pub async fn update_user_status(user_id: &str, status: &str, message: &str, user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>) {
    match tokio::time::timeout(std::time::Duration::from_secs(5), user_statuses.lock()).await {
        Ok(mut statuses) => {
            if let Some(status_obj) = statuses.get_mut(user_id) {
                status_obj.sync_status = status.to_string();
                status_obj.last_update_message = message.to_string();
                if status == "error" {
                    status_obj.last_error = Some(message.to_string());
                }
            }
        },
        Err(_) => warn!("Timeout updating status for user {}", user_id),
    }
}

pub async fn update_user_status_with_error(user_id: &str, status: &str, error: &str, user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>) {
    match tokio::time::timeout(std::time::Duration::from_secs(5), user_statuses.lock()).await {
        Ok(mut statuses) => {
            if let Some(status_obj) = statuses.get_mut(user_id) {
                status_obj.sync_status = status.to_string();
                status_obj.last_update_message = format!("Error: {}", error);
                status_obj.last_error = Some(error.to_string());
            }
        },
        Err(_) => warn!("Timeout updating error status for user {}", user_id),
    }
}

async fn update_user_status_with_address(user_id: &str, status: &str, address: &str, user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>) {
    match tokio::time::timeout(Duration::from_secs(5), user_statuses.lock()).await {
        Ok(mut statuses) => {
            let status_obj = statuses.entry(user_id.to_string()).or_insert_with(|| UserStatus::new(user_id));
            status_obj.sync_status = status.to_string();
            status_obj.current_deposit_address = address.to_string();
        },
        Err(_) => warn!("Timeout updating status with address for user {}", user_id),
    }
}

pub async fn update_user_status_full(
    user_id: &str,
    sync_status: &str,
    utxo_count: u32,
    total_value_btc: f64,
    total_value_usd: f64,
    confirmations_pending: bool,
    update_message: &str,
    sync_duration_ms: u64,
    error: Option<String>,
    last_success: u64,
    user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>,
) {
    match tokio::time::timeout(std::time::Duration::from_secs(5), user_statuses.lock()).await {
        Ok(mut statuses) => {
            if let Some(status) = statuses.get_mut(user_id) {
                status.sync_status = sync_status.to_string();
                status.utxo_count = utxo_count;
                status.total_value_btc = total_value_btc;
                status.total_value_usd = total_value_usd;
                status.confirmations_pending = confirmations_pending;
                status.last_update_message = update_message.to_string();
                status.sync_duration_ms = sync_duration_ms;
                status.last_error = error;
                status.last_success = last_success;
            }
        },
        Err(_) => warn!("Timeout updating full status for user {}", user_id),
    }
}
