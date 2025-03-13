// deposit-service/src/handlers.rs
use actix_web::{web, HttpResponse, Responder};
use bitcoin::{Address, Network, PublicKey};
use bdk_wallet::keys::{KeychainKind, DerivationPath, PublicKey as BdkPublicKey};
use bdk_wallet::psbt::Psbt;
use common::PulserError;
use rand::{thread_rng, Rng};
use structopt::StructOpt;
use chrono::Utc;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::path::Path;

use crate::types::{
    StableChain, Bitcoin, Utxo, DepositAddressInfo, CreateDepositRequest,
    WithdrawalRequest, WithdrawalResponse, HedgeNotification, PsbtSignRequest,
};
use crate::wallet::DepositWallet;
use crate::blockchain::{create_esplora_client, fetch_address_utxos, fetch_fee_rate};
use crate::config::Config;
use crate::integration::{notify_hedge_service, initiate_channel_opening};

// Application state
pub struct AppState {
    pub config: Arc<RwLock<Config>>,
    pub wallets: Arc<RwLock<HashMap<u32, (DepositWallet, StableChain)>>>,
    pub blockchain: Arc<dyn bdk_wallet::chain::BlockChain + Send + Sync>,
    pub http_client: reqwest::Client,
    pub current_price: Arc<RwLock<f64>>,
    pub synthetic_price: Arc<RwLock<f64>>,
    pub network: Network,   // Bitcoin network (bitcoin, testnet, regtest)
    pub role: String,       // Service role (user, lsp, trustee)
}

// API endpoint to create a new deposit address
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
        "testnet" => Network::Testnet,
        "regtest" => Network::Regtest,
        _ => Network::Bitcoin,
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
    
    // Generate a random key for the user (in a real app, this would come from the user)
    let user_pubkey = if cfg!(feature = "user") {
        // For user mode, generate a random key
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let mut rng = thread_rng();
        let user_sk = bitcoin::secp256k1::SecretKey::new(&mut rng);
        let user_pk = bitcoin::secp256k1::PublicKey::from_secret_key(&secp, &user_sk);
        user_pk.to_string()
    } else {
        // For LSP/trustee mode, this would be provided by the user
        req.user_pubkey.clone().ok_or_else(|| 
            PulserError::InvalidRequest("User public key required".to_string())
        )?
    };
    
    // Create the multisig wallet
    let (wallet, deposit_info) = DepositWallet::create_multisig(
        &user_pubkey,
        &lsp_pubkey,
        &trustee_pubkey,
        network,
        app_state.blockchain.clone(),
    )?;
    
    // Initialize stable chain
    let current_price = *app_state.current_price.read().unwrap();
    let now = chrono::Utc::now().timestamp();
    
    let expected_usd = req.expected_amount_usd.unwrap_or(0.0);
    
    let stable_chain = StableChain {
        user_id,
        is_stable_receiver: true,
        counterparty: lsp_pubkey.clone(),
        accumulated_btc: Bitcoin::from_sats(0),
        stabilized_usd: common::USD(0.0),
        timestamp: now,
        formatted_datetime: Utc::now().to_rfc3339(),
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
            details: format!("Created multisig address: {}", deposit_info.address),
        }],
        total_withdrawn_usd: 0.0,
        expected_usd: common::USD(expected_usd),
        hedge_position_id: None,
        pending_channel_id: None,
    };
    
    // Store wallet and chain
    {
        let mut wallets = app_state.wallets.write().unwrap();
        wallets.insert(user_id, (wallet, stable_chain.clone()));
    }
    
    // Save to disk
    let wallet_dir = Path::new(&config.data_dir).join(&config.storage.wallet_dir);
    let wallet_path = wallet_dir.join(format!("user_{}.json", user_id));
    
    let wallet_data = serde_json::to_string_pretty(&stable_chain)
        .map_err(|e| PulserError::InternalError(format!("Failed to serialize wallet data: {}", e)))?;
    
    std::fs::write(&wallet_path, wallet_data)
        .map_err(|e| PulserError::StorageError(format!("Failed to save wallet data: {}", e)))?;
    
    // Return success response
    let response = serde_json::json!({
        "status": "success",
        "user_id": user_id,
        "deposit_address": deposit_info.address,
        "expected_amount_usd": expected_usd,
        "created_at": Utc::now().to_rfc3339(),
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
    let wallets = app_state.wallets.read().unwrap();
    
    let (wallet, chain) = wallets.get(&user_id.into_inner())
        .ok_or_else(|| PulserError::UserNotFound(format!("User {} not found", user_id)))?;
    
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
        "total_confirmed_sats": chain.utxos.iter().filter(|u| u.confirmations > 0).map(|u| u.amount).sum::<u64>(),
        "total_unconfirmed_sats": chain.utxos.iter().filter(|u| u.confirmations == 0).map(|u| u.amount).sum::<u64>(),
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
    let wallets = app_state.wallets.read().unwrap();
    
    let (wallet, chain) = wallets.get(&user_id.into_inner())
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
    
    let (wallet, chain) = wallets.get(&user_id.into_inner())
        .ok_or_else(|| PulserError::UserNotFound(format!("User {} not found", user_id)))?;
    
    // Create response with events
    let response = serde_json::json!({
        "user_id": chain.user_id,
        "deposit_address": chain.multisig_addr,
        "events": chain.events,
    });
    
    Ok(HttpResponse::Ok().json(response))
}

// API endpoint to process withdrawals
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
    
    let fee_rate = match fetch_fee_rate(&app_state.blockchain, bdk_wallet::chain::Target::Normal).await {
        Ok(fee) => fee,
        Err(_) => 5.0, // Default fallback
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
                "bc1qxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_string() // Replace with actual address
            } else {
                return Err(PulserError::InvalidRequest("Destination address required for BTC withdrawal".to_string()));
            }
        }
    };
    
    // Create and sign transaction
    let (psbt, txid) = wallet.create_transaction(&destination, sats_amount, fee_rate, req.urgent.unwrap_or(false))?;
    
    // For multisig, we need other signatures - just store the PSBT in memory or disk in a real implementation
    // For now we'll simulate a successful transaction
    
    // Update chain state
    chain.pending_sweep_txid = Some(txid.to_string());
    
    // Generate request ID
    let request_id = format!("wd_{}_{}_{}", 
                           user_id, 
                           chrono::Utc::now().timestamp(), 
                           thread_rng().gen::<u32>());
    
    // Calculate fees
    let fee_sats = 1000; // This would be calculated from the tx in a real implementation
    let fee_usd = (fee_sats as f64 / 100_000_000.0) * chain.raw_btc_usd;
    
    // Log the event
    chain.log_event("DepositService", "WithdrawalRequested", 
                   &format!("Requested ${:.2} {} withdrawal, txid: {}", 
                           req.amount_usd, req.destination_type, txid));
    
    // Save wallet state to disk
    let wallet_dir = Path::new(&config.data_dir).join(&config.storage.wallet_dir);
    let wallet_path = wallet_dir.join(format!("user_{}.json", user_id));
    
    let wallet_data = serde_json::to_string_pretty(&chain)
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
    
    // In a real implementation, handle the error gracefully
    let _ = notify_hedge_service(&app_state.http_client, 
                              &crate::integration::ServiceConfig {
                                  hedge_service_url: config.hedge_service_url.clone(),
                                  channel_service_url: config.channel_service_url.clone(),
                                  api_key: config.api_key.clone(),
                              }, 
                              &notification).await;
    
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
            "https://mempool.space/tx/{}", 
            txid
        )),
    };
    
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
    
    // Parse the PSBT
    let psbt_bytes = hex::decode(&req.psbt)
        .map_err(|e| PulserError::InvalidRequest(format!("Invalid PSBT hex: {}", e)))?;
    
    let psbt: Psbt = Psbt::from_bytes(&psbt_bytes)
        .map_err(|e| PulserError::InvalidRequest(format!("Invalid PSBT format: {}", e)))?;
    
    // In a real implementation, this is where you would:
    // 1. Verify the PSBT is valid for this user
    // 2. Check that inputs belong to the user's multisig
    // 3. Check output addresses against policy
    // 4. Sign with your key (LSP or trustee)
    // 5. Update the PSBT
    
    // For now, just return the same PSBT (simulating no-op signing)
    let signed_psbt = psbt;
    
    // Serialize the signed PSBT
    let signed_psbt_bytes = signed_psbt.to_bytes();
    let signed_psbt_hex = hex::encode(signed_psbt_bytes);
    
    // Return the signed PSBT
    let response = serde_json::json!({
        "status": "success",
        "user_id": user_id,
        "signed_psbt": signed_psbt_hex,
        "purpose": req.purpose,
    });
    
    Ok(HttpResponse::Ok().json(response))
}

// API endpoint to initiate channel opening
pub async fn initiate_channel_opening(
    user_id: web::Path<u32>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let config = app_state.config.read().unwrap();
    let mut wallets = app_state.wallets.write().unwrap();
    
    let (wallet, chain) = wallets.get_mut(&user_id.into_inner())
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
    
    // Initiate channel opening with channel service
    let channel_id = crate::integration::initiate_channel_opening(
        &app_state.http_client,
        &crate::integration::ServiceConfig {
            hedge_service_url: config.hedge_service_url.clone(),
            channel_service_url: config.channel_service_url.clone(),
            api_key: config.api_key.clone(),
        },
        chain,
        &config.lsp_pubkey,
        "127.0.0.1:9737", // In a real implementation, get this from config
    ).await?;
    
    // Update chain state
    chain.pending_channel_id = Some(channel_id.clone());
    chain.log_event("DepositService", "ChannelInitiated", 
                   &format!("Initiated channel opening with ID: {}", channel_id));
    
    // Save wallet state to disk
    let wallet_dir = Path::new(&config.data_dir).join(&config.storage.wallet_dir);
    let wallet_path = wallet_dir.join(format!("user_{}.json", user_id.into_inner()));
    
    let wallet_data = serde_json::to_string_pretty(&chain)
        .map_err(|e| PulserError::InternalError(format!("Failed to serialize wallet data: {}", e)))?;
    
    std::fs::write(&wallet_path, wallet_data)
        .map_err(|e| PulserError::StorageError(format!("Failed to save wallet data: {}", e)))?;
    
    // Return success
    let response = serde_json::json!({
        "status": "success",
        "user_id": user_id.into_inner(),
        "channel_id": channel_id,
        "amount_sats": chain.accumulated_btc.sats,
        "amount_usd": chain.stabilized_usd.0,
    });
    
    Ok(HttpResponse::Ok().json(response))
}

// API endpoint to get service status
pub async fn get_service_status(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, PulserError> {
    let config = app_state.config.read().unwrap();
    let current_price = *app_state.current_price.read().unwrap();
    
    // Get count of active wallets
    let wallet_count = app_state.wallets.read().unwrap().len();
    
    // Create response
    let response = serde_json::json!({
        "service": "Pulser Deposit Service",
        "version": config.version,
        "status": "operational",
        "network": config.network,
        "current_btc_price": current_price,
        "synthetic_price": *app_state.synthetic_price.read().unwrap(),
        "active_wallets": wallet_count,
        "server_time": chrono::Utc::now().to_rfc3339(),
    });
    
    Ok(HttpResponse::Ok().json(response))
}

// Background wallet sync task
pub async fn wallet_sync_task(app_state: web::Data<AppState>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(120)); // Every 2 minutes
    
    loop {
        interval.tick().await;
        
        // Get config
        let config = app_state.config.read().unwrap().clone();
        
        // Lock wallets
        let mut wallets = match app_state.wallets.write() {
            Ok(wallets) => wallets,
            Err(e) => {
                log::error!("Failed to lock wallets: {}", e);
                continue;
            }
        };
        
        let current_price = *app_state.current_price.read().unwrap();
        let synthetic_price = *app_state.synthetic_price.read().unwrap();
        
        for (user_id, (wallet, chain)) in wallets.iter_mut() {
            // Sync wallet with blockchain
            if let Err(e) = wallet.sync().await {
                log::warn!("Failed to sync wallet for user {}: {}", user_id, e);
                continue;
            }
            
            log::debug!("Wallet synced for user {}", user_id);
            
            // Get balance and update chain info
            let balance = match wallet.get_balance() {
                Ok(balance) => balance,
                Err(e) => {
                    log::warn!("Failed to get balance for user {}: {}", user_id, e);
                    continue;
                }
            };
            
            let old_balance = chain.accumulated_btc.sats;
            chain.accumulated_btc = Bitcoin::from_sats(balance.confirmed);
            
            // Update USD value based on current BTC price
            chain.stabilized_usd = common::USD(chain.accumulated_btc.to_btc() * current_price);
            chain.raw_btc_usd = current_price;
            chain.synthetic_price = Some(synthetic_price);
            
            // Check for new deposits
            if balance.confirmed > old_balance && balance.confirmed > 0 {
                log::info!("New deposit detected for user {}: {} sats", 
                         user_id, balance.confirmed - old_balance);
                
                // Log the deposit event
                chain.log_event("WalletSync", "Deposit", 
                               &format!("New deposit: {} sats (${:.2})", 
                                       balance.confirmed - old_balance, 
                                       (balance.confirmed - old_balance) as f64 / 100_000_000.0 * current_price));
                
                // Notify hedge service of new deposit (in background)
                let notification = HedgeNotification {
                    user_id: *user_id,
                    action: "deposit".to_string(),
                    btc_amount: (balance.confirmed - old_balance) as f64 / 100_000_000.0,
                    usd_amount: (balance.confirmed - old_balance) as f64 / 100_000_000.0 * current_price,
                    current_price,
                    timestamp: chrono::Utc::now().timestamp(),
                    transaction_id: None,
                };
                
                let client = app_state.http_client.clone();
                let service_config = crate::integration::ServiceConfig {
                    hedge_service_url: config.hedge_service_url.clone(),
                    channel_service_url: config.channel_service_url.clone(),
                    api_key: config.api_key.clone(),
                };
                
                tokio::spawn(async move {
                    if let Err(e) = notify_hedge_service(&client, &service_config, &notification).await {
                        log::warn!("Failed to notify hedge service: {}", e);
                    }
                });
            }
            
            // Update UTXOs
            if let Ok(utxos) = wallet.list_utxos() {
                chain.utxos = utxos;
            }
            
            // Check if we have enough confirmations and funds for channel opening
            // In the wallet_sync_task function, replace the simple readiness check with:

// Check if we have enough confirmations and funds for channel opening
let min_confirmations = config.min_confirmations;
let all_confirmed = chain.utxos.iter().all(|u| u.confirmations >= min_confirmations);
let total_confirmed_sats = chain.utxos.iter()
    .filter(|u| u.confirmations >= min_confirmations)
    .map(|u| u.amount)
    .sum::<u64>();

// Convert to USD
let total_confirmed_usd = (total_confirmed_sats as f64 / 100_000_000.0) * current_price;

// Check minimum value threshold (was $420.00)
if total_confirmed_usd < 420.0 {
    log::info!("Accumulated value ${:.2} < $420.00 minimum for channel opening", total_confirmed_usd);
    continue;
}

// Calculate fee percentage for the transaction
let fee_rate = match fetch_fee_rate(&app_state.blockchain, bdk_wallet::chain::Target::Normal).await {
    Ok(rate) => rate,
    Err(_) => 5.0, // Default fallback
};

// Calculate transaction cost for sweep
let input_weight_per_utxo = 245.0;  // ~105 bytes per signature (210 for 2 sigs) + ~35 bytes for redeem script
let total_input_weight = chain.utxos.len() as f64 * input_weight_per_utxo;
let output_and_overhead_weight = 42.0;  // Output (~31 bytes) + overhead (~11 bytes)
let tx_weight = total_input_weight + output_and_overhead_weight;
let tx_vbytes = tx_weight / 4.0;
let tx_cost_sats = (tx_vbytes * fee_rate as f64) as u64;

// Calculate fee percentage of total amount
let fee_percentage = (tx_cost_sats as f64 / total_confirmed_sats as f64) * 100.0;

// Check if fee is too high
if fee_percentage >= 0.21 {
    // Fee too high - don't open channel
    let min_sats = (tx_cost_sats as f64 / 0.0021) as u64;
    let sats_needed = min_sats.saturating_sub(total_confirmed_sats);
    log::warn!("Fees too high ({:.2}% >= 0.21%). Need {} sats more for channel.", 
              fee_percentage, sats_needed);
    continue;
}

// Calculate profitability
// We would call the hedge service to get the current PnL data
// For now, let's assume a simple calculation
let hedge_client = crate::integration::ServiceClient::new(
    &config.hedge_service_url,
    &config.api_key,
);

// Fetch hedge PnL data
let pnl_resp = match hedge_client.get::<serde_json::Value>(
    &format!("/pnl/{}/{}", user_id, false)
).await {
    Ok(resp) => resp,
    Err(e) => {
        log::warn!("Failed to get hedge PnL: {}", e);
        continue;
    }
};

let hedge_pnl = pnl_resp["pnl"].as_f64().unwrap_or(0.0);
let channel_value_usd = total_confirmed_usd;
let total_cost = (tx_cost_sats as f64 / 100_000_000.0) * current_price + 
                 (total_confirmed_sats as f64 / 100_000_000.0) * current_price * 0.00075; // 0.075% opening fee

let profit_percent = (hedge_pnl / channel_value_usd) * 100.0;

// Only proceed if profitable enough (2.1% minimum)
if profit_percent < 2.1 {
    log::warn!("Channel opening not profitable enough: {:.2}% < 2.1%", profit_percent);
    continue;
}

// All criteria met - automatically initiate channel opening
if chain.pending_sweep_txid.is_none() && chain.pending_channel_id.is_none() {
    log::info!("Channel opening criteria met: Fees {:.2}%, Profit {:.2}%", 
              fee_percentage, profit_percent);
    
    // Get LDK node address from config
    let ldk_address = match bitcoin::Address::from_str(&config.ldk_single_sig_address) {
        Ok(addr) => addr,
        Err(e) => {
            log::error!("Invalid LDK address: {}", e);
            continue;
        }
    };
    
    // Create sweep transaction - drain entire multisig to LDK single-sig
    let tx_builder = wallet.build_tx();
    
    let total_sats = chain.accumulated_btc.sats;
    
    match tx_builder
        .add_recipient(ldk_address.script_pubkey(), total_sats)
        .drain_wallet() // Ensures no change output
        .fee_rate(fee_rate)
        .finish() {
        
        Ok(psbt) => {
            // Sign with our key (USER or LSP)
            match wallet.sign(psbt, None) {
                Ok(signed_psbt) => {
                    // For multisig, we can't fully sign here
                    // Store the PSBT for cosigning by other parties
                    let txid = signed_psbt.txid();
                    
                    // Update chain state
                    chain.pending_sweep_txid = Some(txid.to_string());
                    chain.log_event("WalletSync", "SweepInitiated", 
                                   &format!("Initiated multisig sweep to LDK: {}", txid));
                    
                    log::info!("Initiated sweep for user {}: {} sats to LDK", 
                              user_id, total_sats);
                    
                    // In a real implementation, notify the other signers
                    // For LSP mode, this would happen automatically
                    // For USER mode, we'd notify the LSP service
                    
                    // Save PSBT for cosigning
                    let psbt_hex = hex::encode(signed_psbt.serialize());
                    let psbt_path = Path::new(&config.data_dir)
                        .join(&config.storage.wallet_dir)
                        .join(format!("user_{}_psbt.hex", user_id));
                    
                    if let Err(e) = std::fs::write(&psbt_path, psbt_hex) {
                        log::error!("Failed to save PSBT: {}", e);
                    }
                },
                Err(e) => log::error!("Failed to sign PSBT: {}", e),
            }
        },
        Err(e) => log::error!("Failed to create sweep transaction: {}", e),
    }
}
            
            // Save wallet state to disk
            let wallet_path = Path::new(&config.data_dir)
                .join(&config.storage.wallet_dir)
                .join(format!("user_{}.json", user_id));
            
            let wallet_data = match serde_json::to_string_pretty(&chain) {
                Ok(data) => data,
                Err(e) => {
                    log::error!("Failed to serialize wallet state for user {}: {}", user_id, e);
                    continue;
                }
            };
            
            if let Err(e) = std::fs::write(&wallet_path, wallet_data) {
                log::error!("Failed to save wallet state for user {}: {}", user_id, e);
            }
        }
    }
}
