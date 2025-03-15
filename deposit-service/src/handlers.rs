// deposit-service/src/handlers.rs
use actix_web::{web, HttpResponse, Responder, http::StatusCode};
use bitcoin::{Network, Address, secp256k1::{Secp256k1, SecretKey, PublicKey, XOnlyPublicKey}};
use bdk_wallet::SignOptions;
use common::{PulserError, utils};
use rand::{thread_rng, Rng};
use std::str::FromStr;
use std::time::Duration;
use std::path::Path;
use log::{info, warn, error, debug};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};

use crate::AppState;
use crate::wallet::DepositWallet;
use crate::secure_storage::WalletSecrets;
use crate::integration::{notify_hedge_service, ServiceConfig};
use crate::types::{
    Bitcoin, Utxo, DepositAddressInfo, CreateDepositRequest,
    WithdrawalRequest, WithdrawalResponse, HedgeNotification, 
    PsbtSignRequest, PsbtStatusRequest
};

// Handle errors with proper HTTP responses
pub async fn not_found() -> HttpResponse {
    HttpResponse::NotFound().json(serde_json::json!({
        "error": "Endpoint not found",
        "status": 404,
        "success": false
    }))
}


#[derive(Deserialize)]
pub struct CloudBackupRequest {
    pub user_id: u32,
    pub cloud_provider: String,
    pub credentials: Option<String>,
}

pub async fn backup_to_cloud(
    req: web::Json<CloudBackupRequest>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let user_id = req.user_id;
    let config = app_state.config.read().unwrap();
    
    // Load key material
    let mut key_material = keys::load_key_material(
        "user", 
        Some(user_id), 
        Path::new(&config.data_dir),
    )?;
    
    // Check if we have a wallet descriptor
    if key_material.wallet_descriptor.is_none() {
        // Get the descriptor from the wallet
        let wallets = app_state.wallets.read().unwrap();
        if let Some((_, chain)) = wallets.get(&user_id) {
            key_material.wallet_descriptor = Some(chain.descriptor.clone());
            key_material.lsp_pubkey = Some(chain.lsp_pubkey.clone());
            key_material.trustee_pubkey = Some(chain.trustee_pubkey.clone());
            
            // Save updated key material
            keys::store_key_material(&key_material, Path::new(&config.data_dir))?;
        }
    }
    
    // Upload to cloud
    keys::upload_to_cloud(
        &mut key_material,
        &req.cloud_provider,
        req.credentials.as_deref(),
        Path::new(&config.data_dir),
    ).await?;
    
    // Generate a response
    let response = serde_json::json!({
        "status": "success",
        "user_id": user_id,
        "cloud_provider": req.cloud_provider,
        "backup_status": format!("{:?}", key_material.cloud_backup_status),
        "message": "Wallet recovery information backed up to cloud storage"
    });
    
    Ok(HttpResponse::Ok().json(response))
}

// Service health/status endpoint
pub async fn get_service_status(app_state: web::Data<AppState>) -> impl Responder {
    let config = app_state.config.read().unwrap();
    let wallets_count = app_state.wallets.read().unwrap().len();
    
    let response = serde_json::json!({
        "status": "running",
        "service": "Pulser Deposit Service",
        "version": config.version,
        "network": config.network,
        "role": app_state.role,
        "active_wallets": wallets_count,
        "current_btc_price": *app_state.current_price.read().unwrap(),
        "synthetic_price": *app_state.synthetic_price.read().unwrap(),
        "timestamp": utils::now_timestamp(),
        "uptime": "unknown", // Could track this if needed
    });
    
    HttpResponse::Ok().json(response)
}

// In deposit-service/src/handlers.rs
// Update the create_deposit function

pub async fn create_deposit(
    req: web::Json<CreateDepositRequest>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let user_id = req.user_id;
    
    // Check if user already has a wallet
    {
        let wallets = app_state.wallets.read().unwrap();
        if wallets.contains_key(&user_id) {
            return Err(PulserError::InvalidRequest(format!(
                "User {} already has a deposit address", user_id
            )));
        }
    }
    
    // Get config 
    let config = app_state.config.read().unwrap();
    let network = config.bitcoin_network();
    
    // Get or load keypairs for all participants
    let data_dir = Path::new(&config.data_dir);
    
    // For LSP and Trustee, we use static keys (loaded from config or generated if not exists)
    let lsp_keypair = match &req.lsp_pubkey {
        Some(pubkey) => {
            // Use provided pubkey without secret key
            // This is just a placeholder for the test
            TaprootKeypair {
                secret_key: SecretKey::new(&mut rand::thread_rng()),
                public_key: XOnlyPublicKey::from_str(pubkey)
                    .map_err(|_| PulserError::InvalidRequest("Invalid LSP public key".to_string()))?,
                public_key_hex: pubkey.clone(),
            }
        },
        None => {
            // Load or generate LSP key
            load_or_generate_keypair(data_dir, "lsp", None)?
        }
    };
    
    let trustee_keypair = match &req.trustee_pubkey {
        Some(pubkey) => {
            // Use provided pubkey without secret key
            TaprootKeypair {
                secret_key: SecretKey::new(&mut rand::thread_rng()),
                public_key: XOnlyPublicKey::from_str(pubkey)
                    .map_err(|_| PulserError::InvalidRequest("Invalid Trustee public key".to_string()))?,
                public_key_hex: pubkey.clone(),
            }
        },
        None => {
            // Load or generate Trustee key
            load_or_generate_keypair(data_dir, "trustee", None)?
        }
    };
    
    // For user, we generate a new key if in user mode, or use provided key in LSP/Trustee mode
    let user_keypair = if config.role == "user" {
        // Load or generate user key
        load_or_generate_keypair(data_dir, "user", Some(user_id))?
    } else {
        // In LSP/Trustee mode, we need the user's pubkey provided
        match &req.user_pubkey {
            Some(pubkey) => {
                TaprootKeypair {
                    secret_key: SecretKey::new(&mut rand::thread_rng()),
                    public_key: XOnlyPublicKey::from_str(pubkey)
                        .map_err(|_| PulserError::InvalidRequest("Invalid User public key".to_string()))?,
                    public_key_hex: pubkey.clone(),
                }
            },
            None => return Err(PulserError::InvalidRequest("User public key required".to_string())),
        }
    };
    
    // Create the Taproot multisig wallet
    let (wallet, deposit_info) = wallet::create_taproot_multisig(
        &user_keypair.public_key_hex,
        &lsp_keypair.public_key_hex,
        &trustee_keypair.public_key_hex,
        network,
    )?;
    
    // Initialize stable chain state
    let current_price = *app_state.current_price.read().unwrap();
    let synthetic_price = *app_state.synthetic_price.read().unwrap();
    let now = common::utils::now_timestamp();
    
    let expected_usd = req.expected_amount_usd.unwrap_or(0.0);
    
    let stable_chain = StableChain {
        user_id,
        is_stable_receiver: true,
        counterparty: lsp_keypair.public_key_hex.clone(),
        accumulated_btc: Bitcoin::from_sats(0),
        stabilized_usd: USD(0.0),
        timestamp: now,
        formatted_datetime: common::utils::format_timestamp(now),
        sc_dir: format!("./data/stable_chain_{}", user_id),
        raw_btc_usd: current_price,
        synthetic_price: Some(synthetic_price),
        prices: HashMap::new(),  // Will be updated during syncs
        multisig_addr: deposit_info.address.clone(),
        utxos: Vec::new(),
        pending_sweep_txid: None,
        events: vec![Event {
            timestamp: now,
            source: "DepositService".to_string(),
            kind: "AddressCreated".to_string(),
            details: format!("Created taproot multisig address: {}", deposit_info.address),
        }],
        total_withdrawn_usd: 0.0,
        expected_usd: USD(expected_usd),
        hedge_position_id: None,
        pending_channel_id: None,
    };
    
    // Store wallet and chain
    {
        let mut wallets = app_state.wallets.write().unwrap();
        wallets.insert(user_id, (wallet, stable_chain.clone()));
    }
    
    // Save to disk
    let wallet_dir = Path::new(&config.data_dir).join(&config.wallet_dir);
    if !wallet_dir.exists() {
        std::fs::create_dir_all(&wallet_dir)
            .map_err(|e| PulserError::StorageError(format!("Failed to create wallet directory: {}", e)))?;
    }
    
    let wallet_path = wallet_dir.join(format!("user_{}.json", user_id));
    
    let wallet_data = serde_json::to_string_pretty(&stable_chain)
        .map_err(|e| PulserError::InternalError(format!("Failed to serialize wallet data: {}", e)))?;
    
    std::fs::write(&wallet_path, wallet_data)
        .map_err(|e| PulserError::StorageError(format!("Failed to save wallet data: {}", e)))?;
    
    info!("Created new deposit address for user {}: {}", user_id, deposit_info.address);
    
    // Return success response
    let response = serde_json::json!({
        "status": "success",
        "user_id": user_id,
        "deposit_address": deposit_info.address,
        "expected_amount_usd": expected_usd,
        "current_btc_price": current_price,
        "synthetic_price": synthetic_price,
        "created_at": common::utils::format_timestamp(now),
        "network": config.network,
        "deposit_info": {
            "lsp_pubkey": deposit_info.lsp_pubkey,
            "trustee_pubkey": deposit_info.trustee_pubkey,
            "user_pubkey": deposit_info.user_pubkey,
            "descriptor": deposit_info.descriptor,
        }
    });
    
    Ok(HttpResponse::Ok().json(response))
}

// Process a withdrawal request
pub async fn process_withdrawal(
    req: web::Json<WithdrawalRequest>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let user_id = req.user_id;
    
    // Check withdrawal type
    if req.destination_type != "usdt" && req.destination_type != "btc" {
        return Err(PulserError::InvalidRequest(
            "Only USDT or BTC withdrawals are supported".to_string()
        ));
    }
    
    // Get wallet and chain info
    let mut wallets = app_state.wallets.write().unwrap();
    
    let (wallet, chain) = wallets.get_mut(&user_id)
        .ok_or_else(|| PulserError::UserNotFound(format!("User {} not found", user_id)))?;
    
    // Check if withdrawal amount is valid
    if req.amount_usd <= 0.0 {
        return Err(PulserError::InvalidRequest("Withdrawal amount must be positive".to_string()));
    }
    
    // Check if there are sufficient funds
    if chain.stabilized_usd.0 < req.amount_usd {
        return Err(PulserError::InsufficientFunds(
            format!("Insufficient funds: ${:.2} available, ${:.2} requested", 
                  chain.stabilized_usd.0, req.amount_usd)
        ));
    }
    
    // Check if there's a pending withdrawal already
    if chain.pending_sweep_txid.is_some() {
        return Err(PulserError::InvalidRequest(
            "There is already a pending withdrawal for this user".to_string()
        ));
    }
    
    // Calculate amount in BTC based on raw BTC price
    let btc_amount = req.amount_usd / chain.raw_btc_usd;
    let sats_amount = (btc_amount * 100_000_000.0) as u64;
    
    // Get current Bitcoin network fee
    let config = app_state.config.read().unwrap();
    
    let fee_rate = match crate::blockchain::fetch_fee_rate(&app_state.blockchain).await {
        Ok(fee) => fee,
        Err(e) => {
            warn!("Failed to fetch fee rate, using default: {}", e);
            6.9 // Default fallback
        }
    };
    
    // Override fee rate if user specified a max fee
    let fee_rate = if let Some(max_fee) = req.max_fee_sats {
        let calculated_fee = fee_rate as u64;
        if calculated_fee > max_fee {
            max_fee as f32
        } else {
            fee_rate
        }
    } else {
        fee_rate
    };
    
    // Get destination address
    let destination = match &req.destination_address {
        Some(addr) => addr.clone(),
        None => {
            // For USDT withdrawal, use a service-defined Kraken address
            if req.destination_type == "usdt" {
                config.kraken_btc_address.clone().ok_or_else(|| 
                    PulserError::ConfigError("Kraken BTC address not configured".to_string())
                )?
            } else {
                return Err(PulserError::InvalidRequest("Destination address required for BTC withdrawal".to_string()));
            }
        }
    };
    
    // Create transaction
    let is_urgent = req.urgent.unwrap_or(false);
    let (mut psbt, txid) = match wallet.create_transaction(&destination, sats_amount, fee_rate, is_urgent) {
        Ok((psbt, txid)) => (psbt, txid),
        Err(e) => {
            error!("Failed to create withdrawal transaction: {}", e);
            return Err(e);
        }
    };
    
    // For multisig, we need other signatures - save the PSBT for cosigning
    let psbt_base64 = BASE64.encode(bincode::serialize(&psbt).map_err(|e| 
        PulserError::StorageError(format!("Failed to serialize PSBT: {}", e)))?);
        
    let psbt_path = Path::new(&config.data_dir)
        .join(&config.wallet_dir)
        .join(format!("withdrawal_{}_{}.psbt", user_id, txid));
    
    std::fs::write(&psbt_path, &psbt_base64)
        .map_err(|e| PulserError::StorageError(format!("Failed to save PSBT: {}", e)))?;
    
    // Update chain state
    chain.pending_sweep_txid = Some(txid.to_string());
    
    // Generate request ID
    let request_id = format!("wd_{}_{}_{}", 
                          user_id, 
                          chrono::Utc::now().timestamp(), 
                          thread_rng().gen::<u32>());
    
    // Calculate fees - in a real implementation we'd extract this from the PSBT
    // For now, estimate based on the number of inputs and outputs
    let input_count = chain.utxos.len();
    let taproot_input_vbytes = 57.5; // Taproot input size (P2TR)
    let output_vbytes = 43.0; // P2WPKH or P2TR output
    let overhead_vbytes = 10.5; // Transaction overhead
    
    let estimated_tx_vbytes = (input_count as f32 * taproot_input_vbytes) + output_vbytes + overhead_vbytes;
    let fee_sats = (estimated_tx_vbytes * fee_rate) as u64;
    let fee_usd = (fee_sats as f64 / 100_000_000.0) * chain.raw_btc_usd;
    
    // Log the event
    chain.events.push(common::types::Event {
        timestamp: utils::now_timestamp(),
        source: "DepositService".to_string(), 
        kind: "WithdrawalRequested".to_string(), 
        details: format!("Requested ${:.2} {} withdrawal, txid: {}", 
                       req.amount_usd, req.destination_type, txid)
    });
    
    // Notify hedge service
    let notification = HedgeNotification {
        user_id,
        action: "withdrawal".to_string(),
        btc_amount,
        usd_amount: req.amount_usd,
        current_price: chain.raw_btc_usd,
        timestamp: utils::now_timestamp(),
        transaction_id: Some(txid.to_string()),
    };
    
    // Notify hedge service (don't block on this)
    let http_client = app_state.http_client.clone();
    let hedge_service_url = config.hedge_service_url.clone();
    let channel_service_url = config.channel_service_url.clone();
    let api_key = config.api_key.clone();
    let notif = notification.clone();
    
    // Spawn a task to notify the hedge service
    tokio::spawn(async move {
        if let Err(e) = notify_hedge_service(
            &http_client, 
            &ServiceConfig {
                hedge_service_url,
                channel_service_url,
                api_key,
                timeout_secs: 30,
            }, 
            &notif
        ).await {
            warn!("Failed to notify hedge service: {}", e);
        }
    });
    
    // Create response
    let response = WithdrawalResponse {
        request_id,
        txid: Some(txid.to_string()),
        status: "pending_signatures".to_string(),
        amount_usd: req.amount_usd,
        amount_btc: btc_amount,
        fee_sats,
        fee_usd,
        estimated_completion_time: chrono::Utc::now() + chrono::Duration::hours(2),
        confirmation_url: Some(format!(
            "https://mempool.space/{}/tx/{}", 
            if app_state.network == Network::Testnet { "testnet" } else { "" },
            txid
        )),
    };
    
    info!("Withdrawal created for user {}: {} sats (${:.2}) to {}", 
          user_id, sats_amount, req.amount_usd, destination);
    
    Ok(HttpResponse::Ok().json(response))
}

// API endpoint for PSBT signing (used by LSP or trustee)
pub async fn sign_psbt(
    req: web::Json<PsbtSignRequest>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let user_id = req.user_id;
    
    // Verify the user exists
    let wallets = app_state.wallets.read().unwrap();
    
    if !wallets.contains_key(&user_id) {
        return Err(PulserError::UserNotFound(format!("User {} not found", user_id)));
    }
    
    // Decode the PSBT
    let psbt_data = BASE64.decode(&req.psbt)
        .map_err(|e| PulserError::InvalidRequest(format!("Invalid PSBT base64: {}", e)))?;
    
    let mut psbt: bitcoin::psbt::Psbt = bincode::deserialize(&psbt_data)
        .map_err(|e| PulserError::InvalidRequest(format!("Invalid PSBT format: {}", e)))?;
    
    // Get the wallet
    let (wallet, _) = wallets.get(&user_id).unwrap();
    
    // Sign the PSBT
    match wallet.sign(&mut psbt, SignOptions::default()) {
        Ok(is_finalized) => {
            // Serialize and encode the signed PSBT
            let signed_psbt_data = bincode::serialize(&psbt)
                .map_err(|e| PulserError::StorageError(format!("Failed to serialize signed PSBT: {}", e)))?;
                
            let signed_psbt_base64 = BASE64.encode(signed_psbt_data);
            
            info!("PSBT signed for user {} (purpose: {})", user_id, req.purpose);
            
            // Return the signed PSBT
            let response = serde_json::json!({
                "status": "success",
                "user_id": user_id,
                "signed_psbt": signed_psbt_base64,
                "purpose": req.purpose,
                "is_finalized": is_finalized
            });
            
            Ok(HttpResponse::Ok().json(response))
        },
        Err(e) => {
            error!("Failed to sign PSBT: {}", e);
            Err(PulserError::SigningError(format!("Failed to sign PSBT: {}", e)))
        }
    }
}

// Check status of a PSBT and broadcast if ready
pub async fn check_psbt_status(
    req: web::Json<PsbtStatusRequest>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let user_id = req.user_id;
    let txid = &req.txid;
    
    // Get wallet and chain
    let mut wallets = app_state.wallets.write().unwrap();
    
    let (wallet, chain) = wallets.get_mut(&user_id)
        .ok_or_else(|| PulserError::UserNotFound(format!("User {} not found", user_id)))?;
    
    // Check if txid matches pending_sweep_txid
    if chain.pending_sweep_txid.as_ref() != Some(txid) {
        return Err(PulserError::InvalidRequest(
            format!("No pending transaction with txid {} for user {}", txid, user_id)
        ));
    }
    
    // Get config for paths
    let config = app_state.config.read().unwrap();
    
    // Find the appropriate PSBT file
    let wallet_dir = Path::new(&config.data_dir).join(&config.wallet_dir);
    let psbt_path = wallet_dir.join(format!("{}_{}_{}.psbt", 
                                         req.purpose, user_id, txid));
    
    let psbt_path = if psbt_path.exists() {
        psbt_path
    } else {
        // Try alternate naming patterns
        let alt_paths = [
            wallet_dir.join(format!("psbt_{}_{}.psbt", user_id, txid)),
            wallet_dir.join(format!("sweep_{}_{}.psbt", user_id, txid)),
            wallet_dir.join(format!("withdrawal_{}_{}.psbt", user_id, txid)),
        ];
        
        alt_paths.iter()
            .find(|p| p.exists())
            .cloned()
            .ok_or_else(|| PulserError::StorageError(
                format!("PSBT file not found for user {} and txid {}", user_id, txid)
            ))?
    };
    
    // Load and deserialize the PSBT
    let psbt_base64 = std::fs::read_to_string(&psbt_path)
        .map_err(|e| PulserError::StorageError(format!("Failed to read PSBT file: {}", e)))?;
    
    let psbt_data = BASE64.decode(&psbt_base64)
        .map_err(|e| PulserError::InvalidRequest(format!("Invalid PSBT base64: {}", e)))?;
    
    let mut psbt: bitcoin::psbt::Psbt = bincode::deserialize(&psbt_data)
        .map_err(|e| PulserError::InvalidRequest(format!("Invalid PSBT format: {}", e)))?;
    
    // Check if PSBT is fully signed
    match wallet.is_psbt_fully_signed(&psbt) {
        Ok(is_fully_signed) => {
            if is_fully_signed {
                // Finalize and broadcast
                match wallet.finalize_and_broadcast(&mut psbt).await {
                    Ok(txid) => {
                        // Update chain state based on purpose
                        match req.purpose.as_str() {
                            "withdrawal" => {
                                // Move from pending to completed
                                chain.pending_sweep_txid = None;
                                // Record the withdrawal
                                chain.total_withdrawn_usd += req.amount_usd.unwrap_or(0.0);
                                chain.events.push(common::types::Event {
                                    timestamp: utils::now_timestamp(),
                                    source: "DepositService".to_string(), 
                                    kind: "WithdrawalCompleted".to_string(), 
                                    details: format!("Withdrawal tx {} broadcast", txid)
                                });
                            },
                            "channel_opening" => {
                                // Wait for confirmation before initiating channel
                                chain.events.push(common::types::Event {
                                    timestamp: utils::now_timestamp(),
                                    source: "DepositService".to_string(), 
                                    kind: "SweepBroadcast".to_string(), 
                                    details: format!("Sweep tx {} broadcast for channel opening", txid)
                                });
                            },
                            _ => {
                                chain.events.push(common::types::Event {
                                    timestamp: utils::now_timestamp(),
                                    source: "DepositService".to_string(), 
                                    kind: "TransactionBroadcast".to_string(), 
                                    details: format!("Transaction {} broadcast", txid)
                                });
                            }
                        }
                        
                        info!("Successfully broadcast transaction {} for user {}", txid, user_id);
                        
                        let response = serde_json::json!({
                            "status": "success",
                            "user_id": user_id,
                            "txid": txid.to_string(),
                            "message": "Transaction successfully broadcast",
                            "confirmation_url": format!(
                                "https://mempool.space/{}/tx/{}", 
                                if app_state.network == Network::Testnet { "testnet" } else { "" },
                                txid
                            ),
                        });
                        
                        Ok(HttpResponse::Ok().json(response))
                    },
                    Err(e) => {
                        error!("Failed to finalize and broadcast transaction: {}", e);
                        Err(e)
                    }
                }
            } else {
                // Return current status
                let response = serde_json::json!({
                    "status": "pending",
                    "user_id": user_id,
                    "txid": txid,
                    "message": "Transaction still requires signatures"
                });
                
                Ok(HttpResponse::Ok().json(response))
            }
        },
        Err(e) => {
            error!("Failed to check PSBT signatures: {}", e);
            Err(e)
        }
    }
}

// API endpoint to initiate channel opening
pub async fn initiate_channel_opening(
    user_id: web::Path<u32>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let config = app_state.config.read().unwrap();
    
    let mut wallets = app_state.wallets.write().unwrap();
    
    let user_id = user_id.into_inner();
    let (wallet, chain) = wallets.get_mut(&user_id)
        .ok_or_else(|| PulserError::UserNotFound(format!("User {} not found", user_id)))?;
    
    // Check if chain is ready for channel opening
    let min_confirmations = config.min_confirmations;
    let channel_threshold_usd = config.channel_threshold_usd;
    
    // Calculate confirmed funds
    let confirmed_utxos: Vec<&Utxo> = chain.utxos.iter()
        .filter(|u| u.confirmations >= min_confirmations)
        .collect();
    
    let total_confirmed_sats = confirmed_utxos.iter().map(|u| u.amount).sum::<u64>();
    let total_confirmed_usd = (total_confirmed_sats as f64 / 100_000_000.0) * chain.raw_btc_usd;
    
    if confirmed_utxos.is_empty() || total_confirmed_usd < channel_threshold_usd {
        return Err(PulserError::InvalidRequest(
            format!("Chain not ready for channel opening. Need at least ${:.2} with {} confirmations.", 
                  channel_threshold_usd, min_confirmations)
        ));
    }
    
    // Check if channel is already pending
    if chain.pending_channel_id.is_some() {
        return Err(PulserError::InvalidRequest(
            "Channel opening already in progress".to_string()
        ));
    }
    
    // Check if there's already a sweep transaction pending
    if chain.pending_sweep_txid.is_some() {
        return Err(PulserError::InvalidRequest(
            "Sweep transaction already in progress".to_string()
        ));
    }
    
    // Create sweep transaction to move funds to LDK node
    match initiate_channel_sweep(
        app_state.clone(),
        user_id,
        wallet,
        chain,
        &config,
    ).await {
        Ok(txid) => {
            // Update chain state
            chain.pending_sweep_txid = Some(txid.clone());
            chain.events.push(common::types::Event {
                timestamp: utils::now_timestamp(),
                source: "DepositService".to_string(),
                kind: "SweepInitiated".to_string(),
                details: format!("Initiated sweep for channel opening, txid: {}", txid)
            });
            
            // Return success
            let response = serde_json::json!({
                "status": "success",
                "user_id": user_id,
                "sweep_txid": txid,
                "amount_sats": chain.accumulated_btc.sats,
                "amount_usd": chain.stabilized_usd.0,
                "message": "Sweep transaction initiated. Channel will be opened once the transaction is confirmed."
            });
            
            info!("Sweep transaction initiated for channel opening. User: {}, txid: {}", user_id, txid);
            
            Ok(HttpResponse::Ok().json(response))
        },
        Err(e) => {
            error!("Failed to initiate channel sweep: {}", e);
            Err(e)
        }
    }
}

// Helper function to initiate a sweep transaction for channel opening
async fn initiate_channel_sweep(
    app_state: web::Data<AppState>,
    user_id: u32,
    wallet: &DepositWallet,
    chain: &mut crate::types::StableChain,
    config: &crate::config::Config,
) -> Result<String, PulserError> {
    // Get fee rate
    let fee_rate = match crate::blockchain::fetch_fee_rate(&app_state.blockchain).await {
        Ok(rate) => rate,
        Err(e) => {
            warn!("Failed to fetch fee rate, using default: {}", e);
            2.1 // Default fallback
        }
    };
    
    // Get the LDK node address from config
    let ldk_address = Address::from_str(&config.ldk_address)
        .map_err(|e| PulserError::ConfigError(format!("Invalid LDK address: {}", e)))?;
    
    // Calculate transaction size and fee
    let input_count = chain.utxos.len();
    let taproot_input_vbytes = 57.5; // Taproot input size (P2TR)
    let output_vbytes = 43.0; // P2WPKH or P2TR output
    let overhead_vbytes = 10.5; // Transaction overhead
    
    let estimated_tx_vbytes = (input_count as f32 * taproot_input_vbytes) + output_vbytes + overhead_vbytes;
    let fee_sats = (estimated_tx_vbytes * fee_rate) as u64;
    
    // Ensure fee is not too high
    let fee_percent = (fee_sats as f64 / chain.accumulated_btc.sats as f64) * 100.0;
    if fee_percent > config.max_fee_percent {
        return Err(PulserError::InvalidRequest(
            format!("Fee too high: {:.2}% > {:.2}%", fee_percent, config.max_fee_percent)
        ));
    }
    
    // Calculate amount to send (total - fee)
    let send_amount = chain.accumulated_btc.sats.saturating_sub(fee_sats);
    
    // Create and sign transaction
    let (psbt, txid) = wallet.create_transaction(
        &ldk_address.to_string(), 
        send_amount,
        fee_rate,
        false // Don't enable RBF for channel opening
    )?;
    
    // Save PSBT for cosigning
    let psbt_base64 = BASE64.encode(bincode::serialize(&psbt).map_err(|e| 
        PulserError::StorageError(format!("Failed to serialize PSBT: {}", e)))?);
        
    let psbt_path = Path::new(&config.data_dir)
        .join(&config.wallet_dir)
        .join(format!("sweep_{}_{}.psbt", user_id, txid));
    
    std::fs::write(&psbt_path, &psbt_base64)
        .map_err(|e| PulserError::StorageError(format!("Failed to save PSBT: {}", e)))?;
    
    // Notify cosigners for PSBT signing
    // This is a placeholder for the actual implementation
    // In a real implementation, we would send a request to the cosigners
    
    info!("Created sweep transaction for channel opening: {} sats, txid: {}", send_amount, txid);
    
    Ok(txid.to_string())
}

// Not found handler
pub async fn not_found() -> HttpResponse {
    HttpResponse::NotFound().json(serde_json::json!({
        "error": "Endpoint not found",
        "status": 404,
        "success": false
    }))
}
}

// API endpoint to get deposit UTXOs
pub async fn get_deposit_utxos(
    user_id: web::Path<u32>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let wallets = app_state.wallets.read().unwrap();
    
    let user_id = user_id.into_inner();
    let (wallet, chain) = wallets.get(&user_id)
        .ok_or_else(|| PulserError::UserNotFound(format!("User {} not found", user_id)))?;
    
    // Create response with UTXOs
    let response = serde_json::json!({
        "user_id": chain.user_id,
        "deposit_address": chain.multisig_addr,
        "utxos": chain.utxos,
    });
    
    Ok(HttpResponse::Ok().json(response))
}

// API endpoint to get deposit events
pub async fn get_deposit_events(
    user_id: web::Path<u32>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let wallets = app_state.wallets.read().unwrap();
    
    let user_id = user_id.into_inner();
    let (wallet, chain) = wallets.get(&user_id)
        .ok_or_else(|| PulserError::UserNotFound(format!("User {} not found", user_id)))?;
    
    // Create response with events
    let response = serde_json::json!({
        "user_id": chain.user_id,
        "deposit_address": chain.multisig_addr,
        "events": chain.events,
    });
    
    Ok(HttpResponse::Ok().json(response))
}

// API endpoint to get wallet export for the user
pub async fn export_wallet(
    user_id: web::Path<u32>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let user_id = user_id.into_inner();
    
    // Generate export
    let export = app_state.secure_storage.generate_export(user_id)?;
    
    // Encode in base64 for secure transfer
    let export_b64 = BASE64.encode(export.as_bytes());
    
    // Create response
    let response = serde_json::json!({
        "user_id": user_id,
        "export": export_b64,
        "export_time": utils::now_timestamp(),
        "format": "base64",
    });
    
    Ok(HttpResponse::Ok().json(response))
}

// Create a new deposit address
pub async fn create_deposit(
    req: web::Json<CreateDepositRequest>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let user_id = req.user_id;
    
    // Check if user already has a wallet
    {
        let wallets = app_state.wallets.read().unwrap();
        if wallets.contains_key(&user_id) {
            return Err(PulserError::InvalidRequest(format!(
                "User {} already has a deposit address", user_id
            )));
        }
    }
    
    // Get config information
    let config = app_state.config.read().unwrap();
    
    // Get keys based on role
    let (user_pubkey, user_secret, lsp_pubkey, lsp_secret, trustee_pubkey, trustee_secret) = 
        match app_state.role.as_str() {
            "user" => {
                // Generate user keys, use public keys for LSP and trustee
                let secp = Secp256k1::new();
                let (user_secret_key, user_public_key) = secp.generate_keypair(&mut thread_rng());
                
                // Get LSP and trustee pubkeys from config
                let lsp_pub = match &req.lsp_pubkey {
                    Some(key) => key.clone(),
                    None => config.lsp_pubkey.clone(),
                };
                
                let trustee_pub = match &req.trustee_pubkey {
                    Some(key) => key.clone(),
                    None => config.trustee_pubkey.clone(),
                };
                
                (
                    user_public_key.to_string(), 
                    Some(user_secret_key.display_secret().to_string()),
                    lsp_pub,
                    None,
                    trustee_pub,
                    None
                )
            },
            "lsp" => {
                // Use provided user pubkey, generate LSP keys, use trustee pubkey
                let user_pub = match &req.user_pubkey {
                    Some(key) => key.clone(),
                    None => return Err(PulserError::InvalidRequest("User public key required".to_string())),
                };
                
                let secp = Secp256k1::new();
                let (lsp_secret_key, lsp_public_key) = secp.generate_keypair(&mut thread_rng());
                
                let trustee_pub = match &req.trustee_pubkey {
                    Some(key) => key.clone(),
                    None => config.trustee_pubkey.clone(),
                };
                
                (
                    user_pub,
                    None,
                    lsp_public_key.to_string(),
                    Some(lsp_secret_key.display_secret().to_string()),
                    trustee_pub,
                    None
                )
            },
            "trustee" => {
                // Use provided user and LSP pubkeys, generate trustee keys
                let user_pub = match &req.user_pubkey {
                    Some(key) => key.clone(),
                    None => return Err(PulserError::InvalidRequest("User public key required".to_string())),
                };
                
                let lsp_pub = match &req.lsp_pubkey {
                    Some(key) => key.clone(),
                    None => config.lsp_pubkey.clone(),
                };
                
                let secp = Secp256k1::new();
                let (trustee_secret_key, trustee_public_key) = secp.generate_keypair(&mut thread_rng());
                
                (
                    user_pub,
                    None,
                    lsp_pub,
                    None,
                    trustee_public_key.to_string(),
                    Some(trustee_secret_key.display_secret().to_string())
                )
            },
            _ => return Err(PulserError::ConfigError(format!("Unknown service role: {}", app_state.role))),
        };
    
    // Create taproot multisig wallet
    let (wallet, deposit_info) = DepositWallet::create_taproot_multisig(
        &user_pubkey,
        &lsp_pubkey,
        &trustee_pubkey,
        app_state.network,
        app_state.blockchain.clone(),
        &config.data_dir,
    )?;
    
    // Get current prices
    let current_price = *app_state.current_price.read().unwrap();
    let synthetic_price = *app_state.synthetic_price.read().unwrap();
    
    // Initialize stable chain
    let now = utils::now_timestamp();
    let expected_usd = req.expected_amount_usd.unwrap_or(0.0);
    
    let chain = crate::types::StableChain {
        user_id,
        is_stable_receiver: true,
        counterparty: lsp_pubkey.clone(),
        accumulated_btc: Bitcoin::from_sats(0),
        stabilized_usd: common::types::USD(0.0),
        timestamp: now,
        formatted_datetime: utils::format_timestamp(now),
        sc_dir: format!("{}/wallets/user_{}", config.data_dir, user_id),
        raw_btc_usd: current_price,
        synthetic_price: Some(synthetic_price),
        prices: std::collections::HashMap::new(),
        multisig_addr: deposit_info.address.clone(),
        utxos: Vec::new(),
        pending_sweep_txid: None,
        events: vec![common::types::Event {
            timestamp: now,
            source: "DepositService".to_string(),
            kind: "AddressCreated".to_string(),
            details: format!("Created multisig address: {}", deposit_info.address),
        }],
        total_withdrawn_usd: 0.0,
        expected_usd: common::types::USD(expected_usd),
        hedge_position_id: None,
        pending_channel_id: None,
    };
    
    // Store wallet secrets
    let wallet_secret = WalletSecrets {
        user_id,
        network: app_state.network.to_string(),
        created_at: now,
        last_accessed: now,
        user_seed_phrase: None, // We're using xprv instead of seed phrase
        user_pubkey: user_pubkey.clone(),
        user_xprv: user_secret,
        lsp_pubkey: lsp_pubkey.clone(),
        lsp_xprv: lsp_secret,
        trustee_pubkey: trustee_pubkey.clone(),
        trustee_xprv: trustee_secret,
        descriptor: deposit_info.descriptor.clone(),
        address: deposit_info.address.clone(),
        service_role: app_state.role.clone(),
        is_active: true,
        exported_to_user: false,
    };
    
    app_state.secure_storage.store_wallet(wallet_secret)?;
    
    // Store wallet and chain in memory
    {
        let mut wallets = app_state.wallets.write().unwrap();
        wallets.insert(user_id, (wallet, chain.clone()));
    }
    
    info!("Created new deposit address for user {}: {}", user_id, deposit_info.address);
    
    // Return success response
    let response = serde_json::json!({
        "status": "success",
        "user_id": user_id,
        "deposit_address": deposit_info.address,
        "expected_amount_usd": expected_usd,
        "current_btc_price": current_price,
        "synthetic_price": synthetic_price,
        "created_at": utils::format_timestamp(now),
        "network": config.network,
        "deposit_info": {
            "user_pubkey": user_pubkey,
            "lsp_pubkey": lsp_pubkey,
            "trustee_pubkey": trustee_pubkey,
            "descriptor": deposit_info.descriptor,
        }
    });
    
    Ok(HttpResponse::Ok().json(response))
