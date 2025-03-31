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
use bdk_chain::spk_client::SyncRequest;

const MAX_SCAN_RETRIES: u32 = 3;
const SCAN_TIMEOUT_SECS: u64 = 60; // Increased from 30s to 60s for better reliability

pub async fn sync_and_stabilize_utxos(
    user_id: &str,
    wallet: &mut Wallet,
    esplora: &AsyncClient,
    chain: &mut StableChain,
    deribit_price: f64,
    price_info: &PriceInfo,
    deposit_addr: &Address,
    change_addr: &Address,
    state_manager: &StateManager,
    min_confirmations: u32,
) -> Result<Vec<UtxoInfo>, PulserError> {
    debug!("Starting sync for user {} on current and old addresses", user_id);
    let start_time = Instant::now();

    // Get stabilization price
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
    let previous_balance = wallet.balance();

    // Build SPKs for current and old addresses
    let mut spks: Vec<_> = vec![deposit_addr.script_pubkey()];
    for old_addr_str in &chain.old_addresses {
        if let Ok(old_addr) = Address::from_str(old_addr_str).map(|addr| addr.assume_checked()) {
            spks.push(old_addr.script_pubkey());
        } else {
            warn!("Invalid old address for user {}: {}", user_id, old_addr_str);
        }
    }
    debug!("Syncing {} addresses for user {}: {:?}", spks.len(), user_id, spks);

    // Sync all tracked addresses
    let mut scan_success = false;
    let mut update = None;
    let mut last_error = None;

    for attempt in 0..MAX_SCAN_RETRIES {
        if attempt > 0 {
            debug!("Retry {} of {} for blockchain sync for user {}", attempt, MAX_SCAN_RETRIES, user_id);
            tokio::time::sleep(Duration::from_millis(500 * (1 << attempt))).await;
        }

        let request = SyncRequest::builder().spks(spks.iter().cloned()).build();
        match timeout(Duration::from_secs(SCAN_TIMEOUT_SECS), esplora.sync(request, 5)).await {
            Ok(Ok(sync_update)) => {
                debug!("Sync successful for user {} in {} ms", user_id, start_time.elapsed().as_millis());
                update = Some(sync_update);
                scan_success = true;
                break;
            },
            Ok(Err(e)) => {
                warn!("Error syncing blockchain for user {} (attempt {}/{}): {}", user_id, attempt + 1, MAX_SCAN_RETRIES, e);
                last_error = Some(PulserError::ApiError(format!("Sync error: {}", e)));
            },
            Err(_) => {
                warn!("Timeout syncing blockchain for user {} after {}s (attempt {}/{})", user_id, SCAN_TIMEOUT_SECS, attempt + 1, MAX_SCAN_RETRIES);
                last_error = Some(PulserError::NetworkError(format!("Sync timeout after {}s", SCAN_TIMEOUT_SECS)));
            }
        }
    }

    if !scan_success {
        return Err(last_error.unwrap_or_else(|| PulserError::ApiError("Failed to sync blockchain after multiple attempts".to_string())));
    }

    wallet.apply_update(update.unwrap())?;

    // Process UTXOs from all synced addresses
    let wallet_utxos = wallet.list_unspent();
    let mut utxos: Vec<UtxoInfo> = wallet_utxos.into_iter()
        .filter(|u| spks.contains(&u.txout.script_pubkey)) // Only tracked addresses
        .map(|u| {
            let confirmations = match u.chain_position {
                bdk_chain::ChainPosition::Confirmed { anchor, .. } => 
                    wallet.latest_checkpoint().height() - anchor.block_id.height + 1,
                bdk_chain::ChainPosition::Unconfirmed { .. } => 0,
            };
            let address = Address::from_script(&u.txout.script_pubkey, Network::Testnet)
                .map(|addr| addr.to_string())
                .unwrap_or_else(|_| "unknown".to_string());
            UtxoInfo {
                txid: u.outpoint.txid.to_string(),
                vout: u.outpoint.vout,
                amount_sat: u.txout.value.to_sat(),
                address,
                confirmations,
                spent: u.is_spent,
                stable_value_usd: 0.0, // Updated below
                keychain: "External".to_string(),
                timestamp: chrono::Utc::now().timestamp() as u64,
                participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
                spendable: confirmations >= min_confirmations,
                derivation_path: "".to_string(),
            }
        }).collect();

    let chain_utxos: Result<Vec<Utxo>, PulserError> = utxos.iter().map(|u| {
        let script_pubkey = Address::from_str(&u.address)
            .map(|addr| addr.assume_checked().script_pubkey().to_hex_string())
            .map_err(|_| PulserError::BitcoinError("Invalid address".to_string()))?;
        Ok(Utxo {
            txid: u.txid.clone(),
            vout: u.vout,
            amount: u.amount_sat,
            script_pubkey,
            confirmations: u.confirmations,
            height: None,
            usd_value: None, // Updated below
            spent: u.spent,
        })
    }).collect();

    let mut chain_utxos = chain_utxos?;

    // Stabilize new UTXOs
    let mut processed_utxos = Vec::new();
    for (i, utxo) in chain_utxos.iter_mut().enumerate() {
        let is_new = !previous_utxos.iter().any(|prev| prev.txid == utxo.txid && prev.vout == utxo.vout);
        if !is_new {
            if let Some(prev) = previous_utxos.iter().find(|p| p.txid == utxo.txid && p.vout == utxo.vout) {
                utxo.usd_value = prev.usd_value.clone();
                utxos[i].stable_value_usd = prev.usd_value.as_ref().map(|usd| usd.0).unwrap_or(0.0);
            }
            continue;
        }
        if utxo.confirmations >= min_confirmations {
            let stable_value_usd = (utxo.amount as f64 / 100_000_000.0) * stabilization_price;
            utxo.usd_value = Some(USD(stable_value_usd));
            utxos[i].stable_value_usd = stable_value_usd;
            processed_utxos.push(utxos[i].clone());
            chain.history.push(utxos[i].clone());
            info!("Stabilized new UTXO for user {}: {}:{} | {} sats (${:.2} at ${:.2}/BTC)",
                  user_id, utxo.txid, utxo.vout, utxo.amount, stable_value_usd, stabilization_price);
        }
    }

    let new_balance = wallet.balance();
    let btc_delta = (new_balance.confirmed.to_sat() - previous_balance.confirmed.to_sat()) as f64 / 100_000_000.0;

    chain.utxos.extend(chain_utxos);
    chain.accumulated_btc = Bitcoin::from_sats(new_balance.confirmed.to_sat());
    chain.stabilized_usd = USD(chain.history.iter().filter(|h| !h.spent).map(|h| h.stable_value_usd).sum());
    chain.raw_btc_usd = stabilization_price;

    if btc_delta > 0.0 {
        let usd_delta: f64 = processed_utxos.iter().map(|u| u.stable_value_usd).sum();
        chain.log_change(
            "deposit",
            btc_delta,
            usd_delta,
            "deposit-service",
            Some(format!("{} UTXOs confirmed", processed_utxos.len())),
        );
    }

    state_manager.save_stable_chain(user_id, chain).await?;
    if let Some(changeset) = wallet.take_staged() {
        state_manager.save_changeset(user_id, &changeset).await?;
    }

    debug!("Completed sync for user {} in {}ms: {} new UTXOs", user_id, start_time.elapsed().as_millis(), processed_utxos.len());
    Ok(processed_utxos)
}

pub async fn resync_full_history(
    user_id: &str,
    wallet: &mut Wallet,
    esplora: &AsyncClient,
    chain: &mut StableChain,
    price_info: &PriceInfo,
    state_manager: &StateManager,
    min_confirmations: u32,
) -> Result<Vec<UtxoInfo>, PulserError> {
    debug!("Starting full history resync for user {}", user_id);
    let start_time = Instant::now();

    // Use latest price for new stabilizations
    let stabilization_price = price_info.raw_btc_usd;
    if stabilization_price <= 0.0 {
        return Err(PulserError::PriceFeedError("No valid price available for stabilization".to_string()));
    }

    let previous_utxos = chain.utxos.clone();
    let previous_balance = wallet.balance();

    // Full scan of all SPKs in wallet descriptors
    let request = wallet.start_full_scan();
    let mut scan_success = false;
    let mut update = None;
    let mut last_error = None;

    // In resync_full_history
for attempt in 0..MAX_SCAN_RETRIES {
    if attempt > 0 {
        debug!("Retry {} of {} for full scan for user {}", attempt, MAX_SCAN_RETRIES, user_id);
        tokio::time::sleep(Duration::from_millis(500 * (1 << attempt))).await;
    }

    let request = wallet.start_full_scan(); // Rebuild each time
    match timeout(Duration::from_secs(SCAN_TIMEOUT_SECS), esplora.full_scan(request, 10, 5)).await {
        Ok(Ok(full_update)) => {
            debug!("Full scan successful for user {} in {} ms", user_id, start_time.elapsed().as_millis());
            update = Some(full_update);
            scan_success = true;
            break;
        },
        Ok(Err(e)) => {
            warn!("Error full scanning for user {} (attempt {}/{}): {}", user_id, attempt + 1, MAX_SCAN_RETRIES, e);
            last_error = Some(PulserError::ApiError(format!("Full scan error: {}", e)));
        },
        Err(_) => {
            warn!("Timeout full scanning for user {} after {}s (attempt {}/{})", user_id, SCAN_TIMEOUT_SECS, attempt + 1, MAX_SCAN_RETRIES);
            last_error = Some(PulserError::NetworkError(format!("Full scan timeout after {}s", SCAN_TIMEOUT_SECS)));
        }
    }
}

    if !scan_success {
        return Err(last_error.unwrap_or_else(|| PulserError::ApiError("Failed to full scan blockchain after multiple attempts".to_string())));
    }

    wallet.apply_update(update.unwrap())?;

    // Process all unspent UTXOs from the wallet
    let wallet_utxos = wallet.list_unspent();
    let mut new_utxos: Vec<UtxoInfo> = Vec::new();
    let mut chain_utxos: Vec<Utxo> = Vec::new();

for u in wallet_utxos {
    let confirmations = match u.chain_position {
        bdk_chain::ChainPosition::Confirmed { anchor, .. } => wallet.latest_checkpoint().height() - anchor.block_id.height + 1,
        bdk_chain::ChainPosition::Unconfirmed { .. } => 0,
    };
    let address = Address::from_script(&u.txout.script_pubkey, Network::Testnet)
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    let utxo_info = UtxoInfo {
        txid: u.outpoint.txid.to_string(),
        vout: u.outpoint.vout,
        amount_sat: u.txout.value.to_sat(),
        address: address.clone(),
        confirmations,
        spent: u.is_spent,
        stable_value_usd: 0.0,
        keychain: "External".to_string(),
        timestamp: chrono::Utc::now().timestamp() as u64,
        participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
        spendable: confirmations >= min_confirmations,
        derivation_path: "".to_string(),
    };

    let chain_utxo = Utxo {
        txid: utxo_info.txid.clone(),
        vout: utxo_info.vout,
        amount: utxo_info.amount_sat,
        script_pubkey: u.txout.script_pubkey.to_hex_string(),
        confirmations,
        height: None,
        usd_value: None,
        spent: utxo_info.spent,
    };

       let is_new = !previous_utxos.iter().any(|prev| prev.txid == chain_utxo.txid && prev.vout == chain_utxo.vout);

    if is_new && confirmations >= min_confirmations {
        let stable_value_usd = (chain_utxo.amount as f64 / 100_000_000.0) * stabilization_price;
        
        // Log before any moves
        info!("Found and stabilized new UTXO for user {}: {}:{} | {} sats (${:.2} at ${:.2}/BTC)",
              user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount, stable_value_usd, stabilization_price);

        let utxo_info_with_value = UtxoInfo {
            stable_value_usd,
            ..utxo_info
        };
        new_utxos.push(utxo_info_with_value.clone()); // Clone once for new_utxos
        chain.history.push(utxo_info_with_value);     // No clone for history
        
        let chain_utxo_with_value = Utxo {
            usd_value: Some(USD(stable_value_usd)),
            ..chain_utxo
        };
        chain_utxos.push(chain_utxo_with_value);
    } else if !is_new {
        if let Some(prev) = previous_utxos.iter().find(|p| p.txid == chain_utxo.txid && p.vout == chain_utxo.vout) {
            let chain_utxo_with_prev_value = Utxo {
                usd_value: prev.usd_value.clone(),
                ..chain_utxo
            };
            chain_utxos.push(chain_utxo_with_prev_value);
        }
    }
}

    let new_balance = wallet.balance();
    let btc_delta = (new_balance.confirmed.to_sat() - previous_balance.confirmed.to_sat()) as f64 / 100_000_000.0;

    // Append new UTXOs, don’t overwrite existing ones
    for utxo in chain_utxos {
        if !chain.utxos.iter().any(|existing| existing.txid == utxo.txid && existing.vout == utxo.vout) {
            chain.utxos.push(utxo);
        }
    }

    // Update chain totals
    chain.accumulated_btc = Bitcoin::from_sats(new_balance.confirmed.to_sat());
    chain.stabilized_usd = USD(chain.history.iter().filter(|h| !h.spent).map(|h| h.stable_value_usd).sum());
    chain.raw_btc_usd = stabilization_price;

    if btc_delta > 0.0 {
        let usd_delta: f64 = new_utxos.iter().map(|u| u.stable_value_usd).sum();
        chain.log_change(
            "deposit",
            btc_delta,
            usd_delta,
            "deposit-service",
            Some(format!("{} UTXOs recovered via full resync", new_utxos.len())),
        );
    }

    state_manager.save_stable_chain(user_id, chain).await?;
    if let Some(changeset) = wallet.take_staged() {
        state_manager.save_changeset(user_id, &changeset).await?;
    }

    debug!("Completed full resync for user {} in {}ms: {} new UTXOs", 
           user_id, start_time.elapsed().as_millis(), new_utxos.len());
    Ok(new_utxos)
}
