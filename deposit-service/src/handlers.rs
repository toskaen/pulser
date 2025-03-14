// deposit-service/src/handlers.rs
use actix_web::{web, HttpResponse, Responder};
use bitcoin::{Address, Network, Txid};
use bdk_wallet::{Blockchain, KeychainKind, SignOptions, Wallet};
use bdk_wallet::bitcoin::psbt::Psbt;
use common::{Event, PulserError, USD};
use rand::{thread_rng, Rng};
use chrono::Utc;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use log::{info, warn, error, debug};
use base64::Engine;
use crate::integration::ServiceConfig;
use crate::integration::notify_hedge_service;

use crate::types::{
    StableChain, Bitcoin, Utxo, DepositAddressInfo, CreateDepositRequest,
    WithdrawalRequest, WithdrawalResponse, HedgeNotification, PsbtSignRequest,
};
use crate::wallet::DepositWallet;
use crate::blockchain::{create_esplora_client, fetch_address_utxos, fetch_fee_rate};
use crate::config::Config;
use crate::integration::{notify_hedge_service, initiate_channel_opening};

pub struct AppState {
    pub config: Arc<RwLock<Config>>,
    pub wallets: Arc<RwLock<HashMap<u32, (DepositWallet, StableChain)>>>,
    pub http_client: reqwest::Client,
    pub current_price: Arc<RwLock<f64>>,
    pub synthetic_price: Arc<RwLock<f64>>,
    pub network: Network,   // Bitcoin network (bitcoin, testnet, regtest)
    pub role: String,       // Service role (user, lsp, trustee)
}

pub async fn get_service_status(
    app_state: web::Data<AppState>,
) -> impl Responder {
    let config = app_state.config.read().unwrap();
    let wallets_count = app_state.wallets.read().unwrap().len();
    
    let response = serde_json::json!({
        "status": "running",
        "service_name": config.service_name,
        "version": config.version,
        "network": config.network,
        "active_wallets": wallets_count,
        "current_price": *app_state.current_price.read().unwrap(),
        "synthetic_price": *app_state.synthetic_price.read().unwrap(),
    });
    
    HttpResponse::Ok().json(response)
}

// Update the wallet sync task function
pub async fn wallet_sync_task(app_state: web::Data<AppState>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
    
    loop {
        interval.tick().await;
        
        // Lock wallets for writing since we need to sync and update state
        let mut wallets = match app_state.wallets.write() {
            Ok(wallets) => wallets,
            Err(e) => {
                error!("Failed to lock wallets for writing: {}", e);
                continue;
            }
        };
        
        // Iterate through each wallet and sync
        for (user_id, (wallet, chain)) in wallets.iter_mut() {
            match wallet.sync().await {
                Ok(_) => {
                    debug!("Wallet sync completed for user {}", user_id);
                    
                    // Update UTXOs after sync
                    match wallet.list_utxos() {
                        Ok(utxos) => {
                            let old_utxo_count = chain.utxos.len();
                            
                            // Update accumulated_btc from UTXOs
                            let total_sats = utxos.iter().map(|u| u.amount).sum();
                            chain.accumulated_btc = Bitcoin::from_sats(total_sats);
                            
                            // Update utxos in chain
                            if utxos.len() != old_utxo_count {
                                info!("UTXOs changed for user {}: {} -> {}", 
                                      user_id, old_utxo_count, utxos.len());
                                
                                chain.utxos = utxos;
                                chain.stabilized_usd = USD(chain.accumulated_btc.sats as f64 / 100_000_000.0 * chain.raw_btc_usd);
                                
                                // Log event for new deposits
                                if utxos.len() > old_utxo_count {
                                    chain.events.push(Event {
                                        timestamp: common::utils::now_timestamp(),
                                        source: "DepositService".to_string(),
                                        kind: "DepositReceived".to_string(),
                                        details: format!("Received new deposit. Total: {} sats (${:.2})", 
                                                        chain.accumulated_btc.sats, chain.stabilized_usd.0),
                                    });
                                }
                                
                                // Save updated chain state to disk
                                let wallet_dir = Path::new(&app_state.config.read().unwrap().data_dir)
                                    .join(&app_state.config.read().unwrap().wallet_dir);
                                let wallet_path = wallet_dir.join(format!("user_{}.json", user_id));
                                
                                if let Ok(wallet_data) = serde_json::to_string_pretty(&chain) {
                                    if let Err(e) = std::fs::write(&wallet_path, wallet_data) {
                                        error!("Failed to save wallet data for user {}: {}", user_id, e);
                                    }
                                }
                            }
                        },
                       Err(e) => error!("Failed to list UTXOs for user {}: {}", user_id, e),
                    }
                },
                Err(e) => error!("Wallet sync failed for user {}: {}", user_id, e),
            }
        }
    }
}

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
    
    // Get config and pubkeys
    let config = app_state.config.read().unwrap();
    
    // Determine network
    let network = match config.network.as_str() {
        "testnet" => bitcoin::Network::Testnet,
        "regtest" => bitcoin::Network::Regtest,
        "signet" => bitcoin::Network::Signet,
        _ => bitcoin::Network::Bitcoin,
    };
    
    // Get LSP and trustee pubkeys
    let lsp_pubkey = match &req.lsp_pubkey {
        Some(key_str) => key_str.clone(),
        None => config.lsp_pubkey.clone(),
    };
    
    let trustee_pubkey = match &req.trustee_pubkey {
        Some(key_str) => key_str.clone(),
        None => config.trustee_pubkey.clone(),
    };
    
    // Generate or get user's public key
    let user_pubkey = if cfg!(feature = "user") {
        // For user mode, generate a random key
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let mut rng = rand::thread_rng();
        let (secret_key, _) = secp.generate_keypair(&mut rng);
        let user_pk = bitcoin::PublicKey::from_private_key(&secp, &secret_key);
        user_pk.to_string()
    } else {
        // For LSP/trustee mode, this would be provided by the user
        req.user_pubkey.clone().ok_or_else(|| 
            PulserError::InvalidRequest("User public key required".to_string())
        )?
    };
    
    // Get blockchain client
    let blockchain = Arc::new(create_esplora_client(network)?);
    
    // Create the taproot multisig wallet
    let (wallet, deposit_info) = match DepositWallet::create_taproot_multisig(
        &user_pubkey,
        &lsp_pubkey,
        &trustee_pubkey,
        network,
        blockchain,
        &config.data_dir,
    ) {
        Ok(result) => result,
        Err(e) => {
            error!("Failed to create taproot multisig wallet: {}", e);
            return Err(e);
        }
    };
    
    // Initialize stable chain
    let current_price = *app_state.current_price.read().unwrap();
    let now = chrono::Utc::now().timestamp();
    
    let expected_usd = req.expected_amount_usd.unwrap_or(0.0);
    
    let stable_chain = StableChain {
        user_id,
        is_stable_receiver: true,
        counterparty: lsp_pubkey.clone(),
        accumulated_btc: Bitcoin::from_sats(0),
        stabilized_usd: USD(0.0),
        timestamp: now,
        formatted_datetime: chrono::Utc::now().to_rfc3339(),
        sc_dir: format!("./data/stable_chain_{}", user_id),
        raw_btc_usd: current_price,
        synthetic_price: Some(*app_state.synthetic_price.read().unwrap()),
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
        "created_at": chrono::Utc::now().to_rfc3339(),
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
// API endpoint to get deposit status
pub async fn get_deposit_status(
    user_id: web::Path<u32>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let wallets = app_state.wallets.read()
        .map_err(|_| PulserError::InternalError("Failed to lock wallets for reading".to_string()))?;
    
    let user_id = user_id.into_inner();
    let (wallet, chain) = wallets.get(&user_id)
        .ok_or_else(|| PulserError::UserNotFound(format!("User {} not found", user_id)))?;
    
    // Calculate some derived values for the response
    let total_confirmed_sats = chain.utxos.iter()
        .filter(|u| u.confirmations > 0)
        .map(|u| u.amount)
        .sum::<u64>();
    
    let total_unconfirmed_sats = chain.utxos.iter()
        .filter(|u| u.confirmations == 0)
        .map(|u| u.amount)
        .sum::<u64>();
    
    // Create response
    let response = serde_json::json!({
        "user_id": chain.user_id,
        "deposit_address": chain.multisig_addr,
        "accumulated_btc": {
            "sats": chain.accumulated_btc.sats,
            "btc": chain.accumulated_btc.to_btc(),
        },
        "stabilized_usd": chain.stabilized_usd.0,
        "expected_usd": chain.expected_usd.0,
        "current_btc_price": chain.raw_btc_usd,
        "synthetic_price": chain.synthetic_price,
        "created_at": chain.formatted_datetime,
        "utxos": chain.utxos,
        "pending_sweep_txid": chain.pending_sweep_txid,
        "total_confirmed_sats": total_confirmed_sats,
        "total_unconfirmed_sats": total_unconfirmed_sats,
        "total_confirmed_usd": (total_confirmed_sats as f64 / 100_000_000.0) * chain.raw_btc_usd,
        "hedge_position_id": chain.hedge_position_id,
        "pending_channel_id": chain.pending_channel_id,
    });
    
    Ok(HttpResponse::Ok().json(response))
}

// API endpoint to get deposit UTXOs
pub async fn get_deposit_utxos(
    user_id: web::Path<u32>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let wallets = app_state.wallets.read()
        .map_err(|_| PulserError::InternalError("Failed to lock wallets for reading".to_string()))?;
    
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
    let wallets = app_state.wallets.read()
        .map_err(|_| PulserError::InternalError("Failed to lock wallets for reading".to_string()))?;
    
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

// Update the process_withdrawal handler
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
    let mut wallets = app_state.wallets.write()
        .map_err(|_| PulserError::InternalError("Failed to lock wallets for writing".to_string()))?;
    
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
    let config = app_state.config.read()
        .map_err(|_| PulserError::InternalError("Failed to read config".to_string()))?;
    
    let fee_rate = match fetch_fee_rate(&wallet.blockchain).await {
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
let psbt_base64 = base64::engine::general_purpose::STANDARD.encode(&psbt.serialize());
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
    chain.events.push(Event {
        timestamp: common::utils::now_timestamp(),
        source: "DepositService".to_string(), 
        kind: "WithdrawalRequested".to_string(), 
        details: format!("Requested ${:.2} {} withdrawal, txid: {}", 
                       req.amount_usd, req.destination_type, txid)
    });
    
    // Save wallet state to disk
    let wallet_dir = Path::new(&config.data_dir).join(&config.wallet_dir);
    let wallet_path = wallet_dir.join(format!("user_{}.json", user_id));
    
    let wallet_data = serde_json::to_string_pretty(chain)
        .map_err(|e| PulserError::InternalError(format!("Failed to serialize wallet data: {}", e)))?;
    
    std::fs::write(&wallet_path, wallet_data)
        .map_err(|e| PulserError::StorageError(format!("Failed to save wallet data: {}", e)))?;
    
    // Notify hedge service
    let notification = HedgeNotification {
        user_id,
        action: "withdrawal".to_string(),
        btc_amount,
        usd_amount: req.amount_usd,
        current_price: chain.raw_btc_usd,
        timestamp: chrono::Utc::now().timestamp(),
        transaction_id: Some(txid.to_string()),
    };
    
    // Notify cosigners (LSP and/or trustee)
    notify_signers_for_cosigning(&app_state, user_id, txid.to_string(), "withdrawal").await;
    
    // Notify hedge service (don't block on this)
    let http_client = app_state.http_client.clone();
    let hedge_service_url = config.hedge_service_url.clone();
    let channel_service_url = config.channel_service_url.clone();
    let api_key = config.api_key.clone();
    let notif = notification.clone();
    
    tokio::spawn(async move {
        if let Err(e) = notify_hedge_service(
            &http_client, 
            &ServiceConfig {
                hedge_service_url,
                channel_service_url,
                api_key,
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
// Update the sign_psbt handler function
pub async fn sign_psbt(
    req: web::Json<PsbtSignRequest>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let user_id = req.user_id;
    
    // Verify the user exists
    let mut wallets = app_state.wallets.write()
        .map_err(|_| PulserError::InternalError("Failed to lock wallets for writing".to_string()))?;
    
    if !wallets.contains_key(&user_id) {
        return Err(PulserError::UserNotFound(format!("User {} not found", user_id)));
    }
    
    // Parse the PSBT
let psbt_bytes = base64::engine::general_purpose::STANDARD.decode(psbt_base64.as_bytes())
    .map_err(|e| PulserError::InvalidRequest(format!("Invalid PSBT hex: {}", e)))?;
    
    let mut psbt = Psbt::deserialize(&psbt_bytes)
        .map_err(|e| PulserError::InvalidRequest(format!("Invalid PSBT format: {}", e)))?;
    
    // Get the wallet for this user
    let (wallet, _) = wallets.get_mut(&user_id).unwrap();
    
    // Sign the PSBT with our key
    match wallet.sign(&mut psbt, SignOptions::default()) {
        Ok(finalized) => {
            // Serialize the signed PSBT
            let signed_psbt_bytes = psbt.serialize();
            let signed_psbt_hex = hex::encode(signed_psbt_bytes);
            
            info!("PSBT signed for user {} (purpose: {})", user_id, req.purpose);
            
            // Return the signed PSBT
            let response = serde_json::json!({
                "status": "success",
                "user_id": user_id,
                "signed_psbt": signed_psbt_hex,
                "purpose": req.purpose,
                "finalized": finalized
            });
            
            Ok(HttpResponse::Ok().json(response))
        },
        Err(e) => {
            error!("Failed to sign PSBT: {}", e);
            Err(PulserError::SigningError(format!("Failed to sign PSBT: {}", e)))
        }
    }
}

// Helper function to sign a PSBT with the appropriate key based on role
async fn sign_psbt_with_role(
    psbt: &Psbt,
    wallet: &DepositWallet,
    role: &str,
) -> Result<Psbt, PulserError> {
    // Clone the PSBT to avoid modifying the original
    let mut psbt_to_sign = psbt.clone();
    
    // Sign with appropriate wallet based on role
    let signed_psbt = wallet.sign(&psbt_to_sign, SignOptions::default())
        .map_err(|e| PulserError::TransactionError(
            format!("Failed to sign transaction as {}: {}", role, e)
        ))?;
    
    info!("PSBT signed by {}", role);
    
    Ok(signed_psbt)
}

// Helper function to notify cosigners for PSBT signing
async fn notify_signers_for_cosigning(
    app_state: &web::Data<AppState>,
    user_id: u32,
    txid: String,
    purpose: &str,
) {
    // Get required endpoints from config
    let config = match app_state.config.read() {
        Ok(cfg) => cfg.clone(),
        Err(e) => {
            error!("Failed to read config for cosigning notification: {}", e);
            return;
        }
    };
    
    // Based on our role, determine which endpoints to notify
    let service_role = &app_state.role;
    
    let lsp_endpoint = format!("{}/sign_psbt", config.lsp_endpoint);
    let trustee_endpoint = format!("{}/sign_psbt", config.trustee_endpoint);
    
    // Fetch the PSBT from disk
    let wallet_dir = Path::new(&config.data_dir).join(&config.wallet_dir);
    let psbt_path = match purpose {
        "withdrawal" => wallet_dir.join(format!("withdrawal_{}_{}.psbt", user_id, txid)),
        "channel_opening" => wallet_dir.join(format!("sweep_{}_{}.psbt", user_id, txid)),
        _ => wallet_dir.join(format!("psbt_{}_{}.psbt", user_id, txid)),
    };
            }
    
    let psbt_hex = hex::encode(&psbt_bytes);
    
    // Create notification payload
    let payload = serde_json::json!({
        "user_id": user_id,
        "psbt": psbt_hex,
        "purpose": purpose,
        "txid": txid
    });
   
    // Send to appropriate endpoint(s)
    let client = &app_state.http_client;
    
    match service_role.as_str() {
        "user" => {
            // User needs both LSP and trustee to sign
            info!("Notifying LSP and trustee for cosigning user {}'s transaction", user_id);
            
            // Notify LSP first
            tokio::spawn(async move {
                match client.post(&lsp_endpoint)
                    .json(&payload)
                    .timeout(Duration::from_secs(30))
                    .send()
                    .await 
                {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            info!("LSP successfully notified for cosigning user {}'s transaction", user_id);
                            
                            // Now notify trustee
                            match client.post(&trustee_endpoint)
                                .json(&payload)
                                .timeout(Duration::from_secs(30))
                                .send()
                                .await 
                            {
                                Ok(resp) => {
                                    if resp.status().is_success() {
                                        info!("Trustee successfully notified for cosigning user {}'s transaction", user_id);
                                    } else {
                                        warn!("Trustee notification failed with status {}: {}", 
                                             resp.status(), resp.text().await.unwrap_or_default());
                                    }
                                },
                                Err(e) => warn!("Failed to notify trustee: {}", e),
                            }
                        } else {
                            warn!("LSP notification failed with status {}: {}", 
                                 resp.status(), resp.text().await.unwrap_or_default());
                        }
                    },
                    Err(e) => warn!("Failed to notify LSP: {}", e),
                }
            });
        },
        "lsp" => {
            // LSP needs trustee to sign
            info!("Notifying trustee for cosigning user {}'s transaction", user_id);
            
            tokio::spawn(async move {
                match client.post(&trustee_endpoint)
                    .json(&payload)
                    .timeout(Duration::from_secs(30))
                    .send()
                    .await 
                {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            info!("Trustee successfully notified for cosigning user {}'s transaction", user_id);
                        } else {
                            warn!("Trustee notification failed with status {}: {}", 
                                 resp.status(), resp.text().await.unwrap_or_default());
                        }
                    },
                    Err(e) => warn!("Failed to notify trustee: {}", e),
                }
            });
        },
        "trustee" => {
            // Trustee needs LSP to sign
            info!("Notifying LSP for cosigning user {}'s transaction", user_id);
            
            tokio::spawn(async move {
                match client.post(&lsp_endpoint)
                    .json(&payload)
                    .timeout(Duration::from_secs(30))
                    .send()
                    .await 
                {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            info!("LSP successfully notified for cosigning user {}'s transaction", user_id);
                        } else {
                            warn!("LSP notification failed with status {}: {}", 
                                 resp.status(), resp.text().await.unwrap_or_default());
                        }
                    },
                    Err(e) => warn!("Failed to notify LSP: {}", e),
                }
            });
        },
        _ => {
            warn!("Unknown service role: {}", service_role);
        }
    }

// Update the check_psbt_status handler function
pub async fn check_psbt_status(
    req: web::Json<PsbtStatusRequest>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let user_id = req.user_id;
    let txid = &req.txid;
    
    // Get wallet and chain
    let mut wallets = app_state.wallets.write()
        .map_err(|_| PulserError::InternalError("Failed to lock wallets for writing".to_string()))?;
    
    let (wallet, chain) = wallets.get_mut(&user_id)
        .ok_or_else(|| PulserError::UserNotFound(format!("User {} not found", user_id)))?;
    
    // Check if txid matches pending_sweep_txid
    if chain.pending_sweep_txid.as_ref() != Some(txid) {
        return Err(PulserError::InvalidRequest(
            format!("No pending sweep with txid {} for user {}", txid, user_id)
        ));
    }
    
    // Get config for paths
    let config = app_state.config.read()
        .map_err(|_| PulserError::InternalError("Failed to read config".to_string()))?;
    
    // Find the appropriate PSBT file
    let wallet_dir = Path::new(&config.data_dir).join(&config.wallet_dir);
    let psbt_path = wallet_dir.join(format!("{}_{}_{}.psbt", 
                                         req.purpose, user_id, txid));
    
    if !psbt_path.exists() {
        // Try alternate naming patterns
        let alt_paths = [
            wallet_dir.join(format!("psbt_{}_{}.psbt", user_id, txid)),
            wallet_dir.join(format!("sweep_{}_{}.psbt", user_id, txid)),
            wallet_dir.join(format!("withdrawal_{}_{}.psbt", user_id, txid)),
        ];
        
        let found_path = alt_paths.iter().find(|p| p.exists());
        
        if found_path.is_none() {
            return Err(PulserError::StorageError(
                format!("PSBT file not found for user {} and txid {}", user_id, txid)
            ));
        }
    }
    
    // Load and deserialize the PSBT
    let psbt_base64 = std::fs::read_to_string(&psbt_path)
        .map_err(|e| PulserError::StorageError(format!("Failed to read PSBT file: {}", e)))?;
    
let psbt_bytes = base64::engine::general_purpose::STANDARD.decode(psbt_base64.as_bytes())
    .map_err(|e| PulserError::InvalidRequest(format!("Invalid PSBT base64: {}", e)))?;
    
    let mut psbt = Psbt::deserialize(&psbt_bytes)
        .map_err(|e| PulserError::InvalidRequest(format!("Invalid PSBT format: {}", e)))?;
    
    // Check if PSBT is fully signed
    let is_fully_signed = wallet.is_psbt_fully_signed(&psbt)
        .map_err(|e| PulserError::TransactionError(format!("Failed to check PSBT signatures: {}", e)))?;
    
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
                        chain.events.push(Event {
                            timestamp: common::utils::now_timestamp(),
                            source: "DepositService".to_string(), 
                            kind: "WithdrawalCompleted".to_string(), 
                            details: format!("Withdrawal tx {} broadcast", txid)
                        });
                    },
                    "channel_opening" => {
                        // Wait for confirmation before initiating channel
                        chain.events.push(Event {
                            timestamp: common::utils::now_timestamp(),
                            source: "DepositService".to_string(), 
                            kind: "SweepBroadcast".to_string(), 
                            details: format!("Sweep tx {} broadcast for channel opening", txid)
                        });
                    },
                    _ => {
                        chain.events.push(Event {
                            timestamp: common::utils::now_timestamp(),
                            source: "DepositService".to_string(), 
                            kind: "TransactionBroadcast".to_string(), 
                            details: format!("Transaction {} broadcast", txid)
                        });
                    }
                }
                
                // Save updated state
                let wallet_path = wallet_dir.join(format!("user_{}.json", user_id));
                let wallet_data = serde_json::to_string_pretty(chain)
                    .map_err(|e| PulserError::InternalError(
                        format!("Failed to serialize wallet data: {}", e)
                    ))?;
                
                std::fs::write(&wallet_path, wallet_data)
                    .map_err(|e| PulserError::StorageError(
                        format!("Failed to save wallet data: {}", e)
                    ))?;
                
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
}

// Request struct for PSBT status check
#[derive(serde::Deserialize)]
pub struct PsbtStatusRequest {
    pub user_id: u32,
    pub txid: String,
    pub purpose: String,
    pub amount_usd: Option<f64>,
}

// API endpoint to initiate channel opening
pub async fn initiate_channel_opening(
    user_id: web::Path<u32>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let config = app_state.config.read()
        .map_err(|_| PulserError::InternalError("Failed to read config".to_string()))?;
    
    let mut wallets = app_state.wallets.write()
        .map_err(|_| PulserError::InternalError("Failed to lock wallets for writing".to_string()))?;
    
    let user_id = user_id.into_inner();
    let (wallet, chain) = wallets.get_mut(&user_id)
        .ok_or_else(|| PulserError::UserNotFound(format!("User {} not found", user_id)))?;
    
    // Check if chain is ready for channel opening
    if !chain.is_ready_for_channel(config.min_confirmations, config.channel_threshold_usd) {
        return Err(PulserError::InvalidRequest(
            format!("Chain not ready for channel opening. Need at least ${:.2} with {} confirmations.", 
                  config.channel_threshold_usd, config.min_confirmations)
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
            chain.log_event("DepositService", "SweepInitiated", 
                           &format!("Initiated sweep for channel opening, txid: {}", txid));
            
            // Save wallet state to disk
            let wallet_dir = Path::new(&config.data_dir).join(&config.wallet_dir);
            let wallet_path = wallet_dir.join(format!("user_{}.json", user_id));
            
            let wallet_data = serde_json::to_string_pretty(&chain)
                .map_err(|e| PulserError::InternalError(format!("Failed to serialize wallet data: {}", e)))?;
            
            std::fs::write(&wallet_path, wallet_data)
                .map_err(|e| PulserError::StorageError(format!("Failed to save wallet data: {}", e)))?;
            
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
    chain: &StableChain,
    config: &Config,
) -> Result<String, PulserError> {
    // Get fee rate
    let fee_rate = match fetch_fee_rate(&app_state.blockchain, Target::Normal).await {
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
let psbt_base64 = base64::engine::general_purpose::STANDARD.encode(&psbt.serialize());
    let psbt_path = Path::new(&config.data_dir)
        .join(&config.wallet_dir)
        .join(format!("sweep_{}.psbt", txid));
    
    std::fs::write(&psbt_path, &psbt_base64)
        .map_err(|e| PulserError::StorageError(format!("Failed to save PSBT: {}", e)))?;
    
    // Notify other signers
    notify_signers_for_cosigning(&app_state, user_id, txid.to_string(), "channel_opening").await;
    
    info!("Created sweep transaction for channel opening: {} sats, txid: {}", send_amount, txid);
    
    Ok(txid.to_string())
}
