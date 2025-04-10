// deposit-service/src/api/register.rs - Enhanced implementation
use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::timeout;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use log::{info, debug, warn, error};
use serde::{Deserialize, Serialize};
use serde_json::json;
use bitcoin::Address;
use std::str::FromStr;
use tokio::time::Duration;

use common::error::PulserError;
use common::types::{UserStatus, ServiceStatus, StableChain};
use common::price_feed::PriceFeed;
use common::StateManager;

use crate::wallet::DepositWallet;
use crate::config::Config;

// Constants
const REGISTER_TIMEOUT_SECS: u64 = 30;
const LOCK_TIMEOUT_SECS: u64 = 10;

// Registration request schema for validation
#[derive(Debug, Deserialize)]
struct RegistrationRequest {
    user_id: String,
    #[serde(default)]
    xpub: Option<String>,
    #[serde(default)]
    test_mode: bool,
}

// Registration response for consistent API
#[derive(Debug, Serialize)]
struct RegistrationResponse {
    status: String,
    deposit_address: String,
    user_xpub: Option<String>,
    recovery_doc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

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
        .and(super::with_state_manager(state_manager))
        .and(super::with_wallets(wallets))
        .and(super::with_service_status(service_status))
        .and(super::with_statuses(user_statuses))
        .and(warp::any().map(move || price_feed.clone()))
        .and_then(register_handler)
}

async fn register_handler(
    request: RegistrationRequest,
    config: Arc<Config>,
    state_manager: Arc<StateManager>,
    wallets: Arc<Mutex<HashMap<String, (DepositWallet, StableChain)>>>,
    service_status: Arc<Mutex<ServiceStatus>>,
    user_statuses: Arc<Mutex<HashMap<String, UserStatus>>>,
    price_feed: Arc<PriceFeed>,
) -> Result<impl Reply, Rejection> {
    // Enhanced input validation
    if request.user_id.is_empty() || !request.user_id.chars().all(|c| c.is_ascii_digit()) {
        return Err(PulserError::InvalidRequest("Invalid user_id format. Must be numeric.".to_string()))?;
    }
    
    if let Some(ref xpub) = request.xpub {
        if !xpub.starts_with("tpub") && !xpub.starts_with("xpub") {
            return Err(PulserError::InvalidRequest("Invalid xpub format".to_string()))?;
        }
    }
    
    let user_id = request.user_id.clone();
    debug!("Processing registration request for user {}", user_id);
    
    // Wrap the entire registration process with a timeout
    match timeout(Duration::from_secs(REGISTER_TIMEOUT_SECS), async {
        // First check if user is already registered
        let is_registered = {
            let wallets_lock = timeout(
                Duration::from_secs(LOCK_TIMEOUT_SECS), 
                wallets.lock()
            ).await?;
            wallets_lock.contains_key(&user_id)
        };
        
        if is_registered {
            debug!("User {} already registered, retrieving existing wallet", user_id);
            
            // Get the existing wallet data with minimal lock time
            let (deposit_address, user_pubkey) = {
                let wallets_lock = timeout(
                    Duration::from_secs(LOCK_TIMEOUT_SECS), 
                    wallets.lock()
                ).await?;
                
                let (wallet, chain) = wallets_lock.get(&user_id)
                    .ok_or_else(|| PulserError::InternalError("Wallet disappeared".to_string()))?;
                
                (chain.multisig_addr.clone(), "".to_string()) // We may not have the original user_xpub
            };
            
            Ok::<RegistrationResponse, PulserError>(RegistrationResponse {
                status: "registered".to_string(),
                deposit_address,
                user_xpub: None, // Don't send back xpub for existing users
                recovery_doc: None,
                error: None,
            })
        } else {
            info!("Registering new user {}", user_id);
            
            // Prepare directory structure for the new user
            let user_dir = PathBuf::from(format!("{}/user_{}", config.data_dir, user_id));
            if !user_dir.exists() {
                fs::create_dir_all(&user_dir)
                    .map_err(|e| PulserError::StorageError(format!("Failed to create user directory: {}", e)))?;
            }
            
            // Initialize wallet from config
            let wallet_result = DepositWallet::from_config(
                &config, 
                &user_id, 
                &state_manager,
                price_feed.clone()
            ).await;
            
            match wallet_result {
                Ok((wallet, deposit_info, chain, recovery_doc)) => {
                    debug!("Wallet initialized for user {}: {}", user_id, deposit_info.address);
                    
                    // Update service status
                    {
                        let mut status_lock = timeout(
                            Duration::from_secs(LOCK_TIMEOUT_SECS), 
                            service_status.lock()
                        ).await?;
                        status_lock.users_monitored += 1;
                    }
                    
                    // Save user status
                    {
                        let mut statuses_lock = timeout(
                            Duration::from_secs(LOCK_TIMEOUT_SECS), 
                            user_statuses.lock()
                        ).await?;
                        
                        statuses_lock.insert(user_id.clone(), UserStatus {
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
                    }
                    
                    // Save wallet in map
                    {
                        let mut wallets_lock = timeout(
                            Duration::from_secs(LOCK_TIMEOUT_SECS), 
                            wallets.lock()
                        ).await?;
                        wallets_lock.insert(user_id.clone(), (wallet, chain));
                    }
                    
                    info!("Successfully registered user {} with address {}", 
                          user_id, deposit_info.address);
                    
                    // Return success with wallet info
                    Ok(RegistrationResponse {
                        status: "registered".to_string(),
                        deposit_address: deposit_info.address,
                        user_xpub: Some(deposit_info.user_pubkey),
                        recovery_doc,
                        error: None,
                    })
                },
                Err(e) => {
                    error!("Failed to initialize wallet for user {}: {}", user_id, e);
                    
                    // Cleanup any partial files on error
                    if user_dir.exists() {
                        if let Err(cleanup_err) = fs::remove_dir_all(&user_dir) {
                            warn!("Failed to clean up user directory after error: {}", cleanup_err);
                        }
                    }
                    
                    Err(e)
                }
            }
        }
    }).await {
        Ok(Ok(response)) => Ok(warp::reply::json(&response)),
        Ok(Err(e)) => {
            error!("Error during user registration: {}", e);
            Err(e)?
        },
        Err(_) => {
            error!("Timeout during user registration");
            Err(PulserError::Timeout("Registration process timed out".to_string()))?
        }
    }
}
