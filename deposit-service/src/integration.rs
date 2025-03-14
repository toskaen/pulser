use common::error::PulserError;
use common::types::ChannelInfo;
use crate::types::{HedgeNotification, ChannelOpenRequest};
use std::time::Duration;
use log::{info};
use serde_json::json;

// Configuration for service integration
#[derive(Clone, Debug)]
pub struct ServiceConfig {
    pub hedge_service_url: String,
    pub channel_service_url: String,
    pub api_key: String,
    pub timeout_secs: u64,
}

/// Notify the hedge service about deposit/withdrawal events
pub async fn notify_hedge_service(
    client: &reqwest::Client,
    config: &ServiceConfig,
    notification: &HedgeNotification,
) -> Result<(), PulserError> {
    let url = format!("{}/position", config.hedge_service_url);
    
    info!("Notifying hedge service: User {} - {} ${:.2}", 
          notification.user_id, notification.action, notification.usd_amount);
    
    let response = client.post(&url)
        .header("Authorization", format!("Bearer {}", config.api_key))
        .timeout(Duration::from_secs(config.timeout_secs))
        .json(notification)
        .send()
        .await
        .map_err(|e| PulserError::NetworkError(format!("Failed to notify hedge service: {}", e)))?;
    
    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await
            .unwrap_or_else(|_| "Unknown error".to_string());
        
        return Err(PulserError::ApiError(format!(
            "Hedge service returned error: {} - {}", 
            status, error_text
        )));
    }
    
    info!("Hedge service notification successful");
    Ok(())
}

/// Request channel opening from the channel service
pub async fn initiate_channel_opening(
    client: &reqwest::Client,
    config: &ServiceConfig,
    request: &ChannelOpenRequest,
) -> Result<ChannelInfo, PulserError> {
    let url = format!("{}/open", config.channel_service_url);
    
    info!("Requesting channel opening: User {} - {} sats (${:.2})", 
          request.user_id, request.amount_sats, request.expected_usd);
    
    let response = client.post(&url)
        .header("Authorization", format!("Bearer {}", config.api_key))
        .timeout(Duration::from_secs(config.timeout_secs))
        .json(request)
        .send()
        .await
        .map_err(|e| PulserError::NetworkError(format!("Failed to request channel opening: {}", e)))?;
    
    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await
            .unwrap_or_else(|_| "Unknown error".to_string());
        
        return Err(PulserError::ApiError(format!(
            "Channel service returned error: {} - {}", 
            status, error_text
        )));
    }
    
    let channel_info = response.json::<ChannelInfo>().await
        .map_err(|e| PulserError::ApiError(format!("Failed to parse channel response: {}", e)))?;
    
    info!("Channel opening request successful: {}", channel_info.channel_id);
    Ok(channel_info)
}

/// Notify cosigners for PSBT approval
pub async fn notify_cosigner(
    client: &reqwest::Client,
    endpoint: &str,
    user_id: u32,
    psbt_base64: &str,
    purpose: &str,
) -> Result<String, PulserError> {
    let payload = json!({
        "user_id": user_id,
        "psbt": psbt_base64,
        "purpose": purpose
    });
    
    info!("Notifying cosigner at {}: User {} - Purpose {}", 
          endpoint, user_id, purpose);
    
    let response = client.post(endpoint)
        .timeout(Duration::from_secs(30))
        .json(&payload)
        .send()
        .await
        .map_err(|e| PulserError::NetworkError(format!("Failed to notify cosigner: {}", e)))?;
    
    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await
            .unwrap_or_else(|_| "Unknown error".to_string());
        
        return Err(PulserError::ApiError(format!(
            "Cosigner returned error: {} - {}", 
            status, error_text
        )));
    }
    
    let response_text = response.text().await
        .map_err(|e| PulserError::ApiError(format!("Failed to read cosigner response: {}", e)))?;
    
    info!("Cosigner notification successful");
    Ok(response_text)
}

/// Check the status of a transaction on the blockchain
pub async fn check_tx_status(
    client: &reqwest::Client,
    esplora_url: &str, 
    txid: &str
) -> Result<(bool, u32), PulserError> {
    let url = format!("{}/tx/{}", esplora_url.trim_end_matches('/'), txid);
    
    let response = client.get(&url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| PulserError::NetworkError(format!("Failed to check transaction status: {}", e)))?;
    
    // 404 means transaction not found (not broadcast or not indexed yet)
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok((false, 0));
    }
    
    if !response.status().is_success() {
        return Err(PulserError::ApiError(format!("Esplora returned error: {}", response.status())));
    }
    
    let tx_info = response.json::<serde_json::Value>().await
        .map_err(|e| PulserError::ApiError(format!("Failed to parse transaction info: {}", e)))?;
    
    // Check if confirmed
    let confirmed = tx_info["status"]["confirmed"].as_bool().unwrap_or(false);
    let confirmations = tx_info["status"]["block_height"]
        .as_u64()
        .and_then(|height| {
            tx_info["status"]["current_height"].as_u64().map(|current| {
                if current >= height {
                    (current - height + 1) as u32
                } else {
                    0
                }
            })
        })
        .unwrap_or(0);
    
    Ok((confirmed, confirmations))
}
