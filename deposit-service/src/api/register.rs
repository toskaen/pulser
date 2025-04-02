use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use log::{info, debug, warn, error};
use serde_json::json;
use bitcoin::Address;
use std::str::FromStr;

use common::error::PulserError;
use common::types::{UserStatus, ServiceStatus, StableChain};
use common::price_feed::PriceFeed;
use common::StateManager;

use crate::wallet::DepositWallet;
use crate::config::Config;

pub fn register(
    config: Arc<Config>,
    state_manager: Arc<StateManager>,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    service_status: Arc<Mutex<ServiceStatus>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_feed: Arc<PriceFeed>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path("register")
        .and(warp::post())
        .and(warp::body::json())
        .and(super::with_config(config))
        .and_then(move |public_data: serde_json::Value, config: Arc<Config>| {
            let state_manager = state_manager.clone();
            let wallets = wallets.clone();
            let service_status = service_status.clone();
            let user_statuses = user_statuses.clone();
            let price_feed = price_feed.clone();
            
            async move {
                let user_id = match public_data["user_id"].as_str() {
                    Some(id) if !id.is_empty() && id.chars().all(|c| c.is_ascii_digit()) => id.to_string(),
                    _ => return Err(PulserError::InvalidRequest("Invalid or missing user_id".to_string()))?,
                };
                
                let user_dir = PathBuf::from(format!("{}/user_{}", config.data_dir, user_id));
                let public_path = user_dir.join(format!("user_{}_public.json", user_id));
                
                let mut wallets_lock = match tokio::time::timeout(std::time::Duration::from_secs(5), wallets.lock()).await {
                    Ok(lock) => lock,
                    Err(_) => {
                        warn!("Timeout acquiring wallets lock for user {} registration", user_id);
                        return Ok(warp::reply::json(&json!({
                            "status": "registered",
                            "message": "User registered but wallet initialization delayed"
                        })));
                    }
                };
                
                if !wallets_lock.contains_key(&user_id) {
                    match DepositWallet::from_config(&config, &user_id, &state_manager, price_feed.clone()).await {
                        Ok((wallet, deposit_info, chain, recovery_doc)) => {
                            if !public_path.exists() {
                                // Use data from from_config instead of re-running init_pulser_wallet
                                let public_data = json!({
                                    "wallet_descriptor": deposit_info.descriptor,
                                    "internal_descriptor": wallet.wallet.public_descriptor(bdk_wallet::KeychainKind::Internal).to_string(),
                                    "lsp_pubkey": config.lsp_pubkey,
                                    "trustee_pubkey": config.trustee_pubkey,
                                    "user_pubkey": deposit_info.user_pubkey,
                                    "user_id": user_id
                                });
                                
                                match tokio::time::timeout(std::time::Duration::from_secs(5), state_manager.save(&public_path, &public_data)).await {
                                    Ok(Ok(_)) => info!("Public data saved to {}", public_path.display()),
                                    Ok(Err(e)) => {
                                        error!("Failed to save user {} public data: {}", user_id, e);
                                        return Err(e)?;
                                    }
                                    Err(_) => return Err(PulserError::InternalError("Timeout saving user data".to_string()))?,
                                }
                            }
                            
                            let mut statuses = match tokio::time::timeout(std::time::Duration::from_secs(5), user_statuses.lock()).await {
                                Ok(lock) => lock,
                                Err(_) => {
                                    wallets_lock.insert(user_id.clone(), (wallet, chain));
                                    return Ok(warp::reply::json(&json!({
                                        "status": "registered",
                                        "message": "User registered but status initialization delayed"
                                    })));
                                }
                            };
                            
                            statuses.insert(user_id.clone(), UserStatus {
                                user_id: user_id.clone(),
                                last_sync: 0,
                                sync_status: "initialized".to_string(),
                                utxo_count: 0,
                                total_value_btc: 0.0,
                                total_value_usd: 0.0,
                                confirmations_pending: false,
                                last_update_message: "User initialized".to_string(),
                                sync_duration_ms: 0,
                                last_error: None,
                                last_success: 0,
                                pruned_utxo_count: 0,
                                current_deposit_address: deposit_info.address.clone(),
                                last_deposit_time: None,
                            });
                            
                            wallets_lock.insert(user_id.clone(), (wallet, chain));
                            
                            match tokio::time::timeout(std::time::Duration::from_secs(5), service_status.lock()).await {
                                Ok(mut status) => status.users_monitored = wallets_lock.len() as u32,
                                Err(_) => warn!("Timeout updating service status for new user {}", user_id)
                            }
                            
                            info!("Registered user {} with descriptors", user_id);
                            
                            Ok::<_, Rejection>(warp::reply::json(&json!({
                                "status": "registered",
                                "deposit_address": deposit_info.address,
                                "user_xpub": deposit_info.user_pubkey,
                                "recovery_doc": recovery_doc
                            })))
                        },
                        Err(e) => Err(e)?,
                    }
                } else {
                    debug!("User {} already registered, skipping wallet init", user_id);
                    let (existing_wallet, existing_chain) = wallets_lock.get(&user_id).unwrap();
                    let deposit_address = existing_chain.multisig_addr.clone();
                    
                    if !public_path.exists() {
                        let public_data = json!({
                            "wallet_descriptor": existing_wallet.wallet.public_descriptor(bdk_wallet::KeychainKind::External).to_string(),
                            "internal_descriptor": existing_wallet.wallet.public_descriptor(bdk_wallet::KeychainKind::Internal).to_string(),
                            "lsp_pubkey": config.lsp_pubkey,
                            "trustee_pubkey": config.trustee_pubkey,
                            "user_pubkey": existing_chain.multisig_addr.clone(), // or use deposit_info.user_pubkey if available
                            "user_id": user_id
                        });
                        
                        match tokio::time::timeout(std::time::Duration::from_secs(5), state_manager.save(&public_path, &public_data)).await {
                            Ok(Ok(_)) => info!("Updated public data for existing user {}", user_id),
                            Ok(Err(e)) => error!("Failed to update public data for {}: {}", user_id, e),
                            Err(_) => error!("Timeout updating public data for {}", user_id),
                        }
                    }
                    
                    Ok::<_, Rejection>(warp::reply::json(&json!({
                        "status": "registered",
                        "deposit_address": deposit_address
                    })))
                }
            }
        })
}
