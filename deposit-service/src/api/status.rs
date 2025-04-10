// In deposit-service/src/api/status.rs - Replace with this improved implementation

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use log::warn;
use common::types::UserStatus;

/// Core function to update user status with timeout handling
pub async fn update_user_status(
    user_id: &str, 
    status: &str, 
    message: &str, 
    user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>,
    update_error: bool,
) {
    const LOCK_TIMEOUT: Duration = Duration::from_secs(5);
    
    match tokio::time::timeout(LOCK_TIMEOUT, user_statuses.lock()).await {
        Ok(mut statuses) => {
            if let Some(status_obj) = statuses.get_mut(user_id) {
                status_obj.sync_status = status.to_string();
                status_obj.last_update_message = message.to_string();
                
                // Update error field conditionally
                if update_error || status == "error" {
                    status_obj.last_error = Some(message.to_string());
                }
            } else {
                warn!("Attempted to update non-existent user status: {}", user_id);
            }
        },
        Err(_) => warn!("Timeout updating status for user {}", user_id),
    }
}

/// Update status without marking as error
pub async fn update_user_status_simple(
    user_id: &str,
    status: &str,
    message: &str,
    user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>,
) {
    update_user_status(user_id, status, message, user_statuses, false).await;
}

/// Update status and mark as error
pub async fn update_user_status_with_error(
    user_id: &str,
    status: &str,
    error: &str,
    user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>,
) {
    update_user_status(user_id, status, error, user_statuses, true).await;
}

/// Update status with address information
pub async fn update_user_status_with_address(
    user_id: &str,
    status: &str,
    address: &str,
    user_statuses: &Arc<Mutex<HashMap<String, UserStatus>>>,
) {
    match tokio::time::timeout(Duration::from_secs(5), user_statuses.lock()).await {
        Ok(mut statuses) => {
            let status_obj = statuses.entry(user_id.to_string())
                .or_insert_with(|| UserStatus::new(user_id));
            status_obj.sync_status = status.to_string();
            status_obj.current_deposit_address = address.to_string();
        },
        Err(_) => warn!("Timeout updating status with address for user {}", user_id),
    }
}

/// Comprehensive status update with all fields
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
    match tokio::time::timeout(Duration::from_secs(5), user_statuses.lock()).await {
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
            } else {
                warn!("Attempted to fully update non-existent user status: {}", user_id);
            }
        },
        Err(_) => warn!("Timeout updating full status for user {}", user_id),
    }
}
