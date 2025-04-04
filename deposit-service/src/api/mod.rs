// deposit-service/src/api/mod.rs
pub mod health;
pub mod user;
pub mod sync;
pub mod register;
pub mod status;  // New consolidated status module

use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use std::collections::HashMap;
use log::{info, warn, debug};
use common::error::PulserError;
use common::StateManager;
use common::task_manager::UserTaskLock;
use common::types::{ServiceStatus, UserStatus, StableChain, PriceInfo};
use common::price_feed::PriceFeed;
use crate::wallet::DepositWallet;
use common::webhook::WebhookManager;
use crate::config::Config;
use reqwest::Client;
use std::time::Duration;
use common::health::HealthChecker;

// Re-export the sync_user function for use by other modules
pub use sync::sync_user;

// Re-export the status update functions for convenience
pub use status::{
    update_user_status_simple as update_user_status,
    update_user_status_with_error,
    update_user_status_with_address,
    update_user_status_full,
};

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
    health_checker: Arc<HealthChecker>, // Add health_checker parameter
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    health::health(health_checker)
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
            webhook_manager.clone(),
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
