// common/src/wallet_sync.rs
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use bdk_wallet::{Wallet, KeychainKind, Balance};
use bdk_esplora::EsploraAsyncExt;
use bdk_esplora::esplora_client::AsyncClient;
use bdk_chain::spk_client::FullScanResponse;
use crate::{types::{StableChain, UtxoInfo, USD, Bitcoin, Utxo}, error::PulserError, StateManager, price_feed::PriceFeed};
use crate::types::PriceInfo;
use std::str::FromStr;
use log::{info, warn, debug, error, trace};
use bitcoin::Address;
use bitcoin::Network;
use tokio::time::timeout;

const MAX_SCAN_RETRIES: u32 = 3;
const SCAN_TIMEOUT_SECS: u64 = 60; // Increased from 30s to 60s for better reliability

pub async fn sync_and_stabilize_utxos(
    user_id: &str,
    wallet: &mut Wallet,
    esplora: &AsyncClient,
    chain: &mut StableChain,
deribit_price: f64
    price_info: &PriceInfo,
    deposit_addr: &Address,
    change_addr: &Address,
    state_manager: &StateManager,
    min_confirmations: u32,
) -> Result<Vec<UtxoInfo>, PulserError> {
    debug!("Starting sync for user {}", user_id);
    let start_time = Instant::now();

    // Get current BTC/USD price for stabilization 
let stabilization_price = if deribit_price <= 0.0 {
    warn!("Invalid Deribit price passed: {}, falling back to median price", deribit_price);
    price_info.raw_btc_usd
} else {
    deribit_price
};

if stabilization_price <= 0.0 {
    return Err(PulserError::PriceFeedError("No valid price available for stabilization".to_string()));
}

let mut new_utxos: Vec<UtxoInfo> = Vec::new();
    let previous_utxos = chain.utxos.clone();

    // Track previous balance for change detection
    let previous_balance = wallet.balance();

    // Scan with retries
    let mut scan_success = false;
    let mut update = None;
    let mut last_error = None;

    for attempt in 0..MAX_SCAN_RETRIES {
        if attempt > 0 {
            debug!("Retry {} of {} for blockchain scan for user {}", 
                   attempt, MAX_SCAN_RETRIES, user_id);
            // Simple exponential backoff
            tokio::time::sleep(Duration::from_millis(500 * (1 << attempt))).await;
        }

        let request = wallet.start_full_scan();
        match timeout(Duration::from_secs(SCAN_TIMEOUT_SECS), 
                     esplora.full_scan(request, 10, 5)).await {
            Ok(Ok(full_update)) => {
                debug!("Blockchain scan successful for user {} in {} ms", 
                       user_id, start_time.elapsed().as_millis());
                update = Some(full_update);
                scan_success = true;
                break;
            },
            Ok(Err(e)) => {
                warn!("Error scanning blockchain for user {} (attempt {}/{}): {}", 
                      user_id, attempt+1, MAX_SCAN_RETRIES, e);
                last_error = Some(PulserError::ApiError(format!("Blockchain scan error: {}", e)));
            },
            Err(_) => {
                warn!("Timeout scanning blockchain for user {} after {}s (attempt {}/{})", 
                      user_id, SCAN_TIMEOUT_SECS, attempt+1, MAX_SCAN_RETRIES);
                last_error = Some(PulserError::NetworkError(
                    format!("Blockchain scan timeout after {}s", SCAN_TIMEOUT_SECS)));
            }
        }
    }

    // If all retries failed, propagate the error
    if !scan_success {
        return Err(last_error.unwrap_or_else(|| 
            PulserError::ApiError("Failed to scan blockchain after multiple attempts".to_string())));
    }

    // Apply the blockchain update to the wallet
    wallet.apply_update(update.unwrap())?;

    // Process UTXOs
    let wallet_utxos = wallet.list_unspent();
    let mut utxos: Vec<UtxoInfo> = wallet_utxos.into_iter().map(|u| {
        let confirmations = match u.chain_position {
            bdk_chain::ChainPosition::Confirmed { anchor, .. } => 
                wallet.latest_checkpoint().height() - anchor.block_id.height + 1,
            bdk_chain::ChainPosition::Unconfirmed { .. } => 0,
        };
        
        UtxoInfo {
            txid: u.outpoint.txid.to_string(),
            vout: u.outpoint.vout,
            amount_sat: u.txout.value.to_sat(),
            address: match Address::from_script(&u.txout.script_pubkey, Network::Testnet) {
                Ok(addr) => addr.to_string(),
                Err(e) => {
                    warn!("Failed to convert script to address: {}", e);
                    "unknown".to_string()
                }
            },
            confirmations,
            spent: u.is_spent,
            stable_value_usd: 0.0, // Will be updated below
            keychain: "External".to_string(), // Default, will be determined later
            timestamp: chrono::Utc::now().timestamp() as u64,
            participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
            spendable: confirmations >= min_confirmations,
            derivation_path: "".to_string(),
        }
    }).collect();

    // Convert to Utxo format needed for StableChain
    let chain_utxos: Result<Vec<Utxo>, PulserError> = utxos.iter().map(|u| {
        let script_pubkey = match Address::from_str(&u.address) {
            Ok(addr) => addr.require_network(Network::Testnet)?.script_pubkey().to_hex_string(),
            Err(_) => return Err(PulserError::BitcoinError("Invalid address".to_string())),
        };
        
        Ok(Utxo {
            txid: u.txid.clone(),
            vout: u.vout,
            amount: u.amount_sat,
            script_pubkey,
            confirmations: u.confirmations,
            height: None,
            usd_value: None, // Will be updated below
            spent: u.spent,
        })
    }).collect();

    // If any UTXO conversion failed, return the error
    let mut chain_utxos = chain_utxos?;

    // Process new UTXOs and stabilize USD value
    let mut processed_utxos = Vec::new();
    for (i, utxo) in chain_utxos.iter_mut().enumerate() {
        // Check if this is a new UTXO we haven't seen before
        let is_new = !previous_utxos.iter().any(|prev| 
            prev.txid == utxo.txid && prev.vout == utxo.vout);
        
        // For existing UTXOs, preserve the original stabilized value
        if !is_new {
            if let Some(prev) = previous_utxos.iter().find(|p| 
                p.txid == utxo.txid && p.vout == utxo.vout) {
                utxo.usd_value = prev.usd_value.clone();
                utxos[i].stable_value_usd = prev.usd_value.as_ref().map(|usd| usd.0).unwrap_or(0.0);
            }
            continue;
        }
        
        // For new UTXOs with enough confirmations, stabilize the value
        if utxo.confirmations >= min_confirmations {
            let stable_value_usd = (utxo.amount as f64 / 100_000_000.0) * stabilization_price;
            utxo.usd_value = Some(USD(stable_value_usd));
            utxos[i].stable_value_usd = stable_value_usd;
            
            // Add to new UTXOs list for return
            processed_utxos.push(utxos[i].clone());
            
            // Add to history
            chain.history.push(utxos[i].clone());
            
            info!("Stabilized new UTXO for user {}: {}:{} | {} sats (${:.2} at ${:.2}/BTC)",
                  user_id, utxo.txid, utxo.vout, utxo.amount, 
                  stable_value_usd, stabilization_price);
        } else {
            debug!("UTXO {}:{} has only {} confirmations, need {}", 
                   utxo.txid, utxo.vout, utxo.confirmations, min_confirmations);
        }
    }

    // Update chain state
    chain.utxos = chain_utxos;
    let new_balance = wallet.balance();
    chain.accumulated_btc = Bitcoin::from_sats(new_balance.confirmed.to_sat());
    
    // Calculate stabilized USD based on history (only include unspent UTXOs)
    chain.stabilized_usd = USD(chain.history.iter()
        .filter(|h| !h.spent)
        .map(|h| h.stable_value_usd)
        .sum());
    
    chain.raw_btc_usd = stabilization_price;

    // Save state atomically - first to memory, then to disk
    let state_save_result = state_manager.save_stable_chain(user_id, chain).await;
    if let Err(e) = state_save_result {
        error!("Failed to save StableChain for user {}: {}", user_id, e);
        return Err(e);
    }

    // Save wallet changeset if available
    if let Some(changeset) = wallet.take_staged() {
        let cs_save_result = state_manager.save_changeset(user_id, &changeset).await;
        if let Err(e) = cs_save_result {
            error!("Failed to save ChangeSet for user {}: {}", user_id, e);
            return Err(e);
        }
    }

    let duration_ms = start_time.elapsed().as_millis();
    if duration_ms > 5000 {
        warn!("Sync for user {} took {}ms", user_id, duration_ms);
    } else {
        debug!("Completed sync for user {} in {}ms: {} new UTXOs", 
               user_id, duration_ms, processed_utxos.len());
    }

    // Log balance changes
    if previous_balance.confirmed != new_balance.confirmed ||
       previous_balance.untrusted_pending != new_balance.untrusted_pending {
        info!("Balance change for user {}: confirmed {} -> {} sats, pending {} -> {} sats",
              user_id, 
              previous_balance.confirmed.to_sat(), new_balance.confirmed.to_sat(),
              previous_balance.untrusted_pending.to_sat(), new_balance.untrusted_pending.to_sat());
    }

    Ok(processed_utxos)
}
