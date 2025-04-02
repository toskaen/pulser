use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use serde_json::json;
use common::error::PulserError;
use common::types::{ServiceStatus, PriceInfo};
use common::task_manager::UserTaskLock;
use std::collections::HashMap;

pub fn health(
    service_status: Arc<Mutex<ServiceStatus>>,
    esplora_urls: Arc<Mutex<Vec<(String, u32)>>>,
    price_info: Arc<Mutex<PriceInfo>>,
    active_tasks_manager: Arc<UserTaskLock>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path("health")
        .and(warp::get())
        .and(super::with_service_status(service_status))
        .and(super::with_esplora_urls(esplora_urls))
        .and(super::with_price(price_info))
        .and(super::with_active_tasks_manager(active_tasks_manager))
        .and_then(health_handler)
}

async fn health_handler(
    service_status: Arc<Mutex<ServiceStatus>>,
    esplora_urls: Arc<Mutex<Vec<(String, u32)>>>,
    price_info: Arc<Mutex<PriceInfo>>,
    active_tasks_manager: Arc<UserTaskLock>,
) -> Result<impl Reply, Rejection> {
    let status = match tokio::time::timeout(Duration::from_secs(3), service_status.lock()).await {
        Ok(status) => status,
        Err(_) => return Err(PulserError::InternalError("Timeout acquiring service status lock".to_string()))?,
    };
    
    let esplora_urls_lock = match tokio::time::timeout(Duration::from_secs(3), esplora_urls.lock()).await {
        Ok(lock) => lock,
        Err(_) => return Err(PulserError::InternalError("Timeout acquiring esplora URLs lock".to_string()))?,
    };
    
    let price_info_lock = match tokio::time::timeout(Duration::from_secs(3), price_info.lock()).await {
        Ok(lock) => lock,
        Err(_) => return Err(PulserError::InternalError("Timeout acquiring price info lock".to_string()))?,
    };
    
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0)).as_secs();
    let websocket_active = status.websocket_active;
    
    let mut esplora_status = HashMap::new();
    let mut esplora_details = HashMap::new();
    let mut any_healthy_esplora = false;
    
    for (url, errors) in esplora_urls_lock.iter() {
        let is_healthy = *errors < 10;
        esplora_status.insert(url.clone(), is_healthy);
        esplora_details.insert(url.clone(), json!({
            "healthy": is_healthy,
            "error_count": errors,
            "last_used": now - status.last_update,
            "primary": url == &esplora_urls_lock[0].0
        }));
        if is_healthy {
            any_healthy_esplora = true;
        }
    }
    
    let active_tasks = active_tasks_manager.get_active_tasks().await;
    let task_counts = active_tasks_manager.get_task_counts().await;
    
    let price_staleness_secs = now - price_info_lock.timestamp as u64;
    let price_healthy = price_staleness_secs < 120 && price_info_lock.raw_btc_usd > 1000.0;
    
    let overall_status = if websocket_active && price_healthy && any_healthy_esplora {
        "healthy"
    } else if !any_healthy_esplora {
        "critical"
    } else if !websocket_active || !price_healthy {
        "degraded"
    } else {
        "warning"
    };
    
    let websocket_action = if websocket_active { "WebSocket operational" } else { "Action: Check WebSocket connection to Deribit" };
    let price_action = if price_healthy { "Price feed current" } else if price_info_lock.raw_btc_usd <= 1000.0 { "Action: Invalid price detected, check price sources" } else { "Action: Price feed stale, check Deribit connectivity" };
    let esplora_action = if any_healthy_esplora { "At least one Esplora endpoint operational" } else { "CRITICAL: All Esplora endpoints failing, service severely degraded" };
    
    Ok(warp::reply::json(&json!({
        "status": overall_status,
        "timestamp": now,
        "health": status.health.clone(),
        "active_syncs": status.active_syncs,
        "websocket": { 
            "active": websocket_active, 
            "action": websocket_action,
            "last_connected": status.last_update
        },
        "price": { 
            "staleness_secs": price_staleness_secs, 
            "last_price": price_info_lock.raw_btc_usd, 
            "action": price_action,
            "sources": price_info_lock.price_feeds.keys().collect::<Vec<_>>()
        },
        "esplora": { 
            "status": if any_healthy_esplora { "operational" } else { "failing" },
            "endpoints": esplora_status, 
            "details": esplora_details,
            "action": esplora_action
        },
        "tasks": {
            "active_count": active_tasks.len(),
            "by_type": task_counts,
            "details": active_tasks
        },
        "system": {
            "users_monitored": status.users_monitored,
            "total_utxos": status.total_utxos,
            "total_value_btc": status.total_value_btc,
            "total_value_usd": status.total_value_usd,
            "uptime_seconds": now - status.up_since
        }
    })))
}
