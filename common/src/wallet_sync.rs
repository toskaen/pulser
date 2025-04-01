// common/src/wallet_sync.rs
use std::time::{Duration, Instant};
use bdk_wallet::{Wallet, KeychainKind, Balance};
use bdk_esplora::EsploraAsyncExt;
use bdk_esplora::esplora_client::AsyncClient;
use crate::{types::{StableChain, UtxoInfo, USD, Bitcoin, Utxo}, error::PulserError, StateManager, price_feed::PriceFeed};
use crate::types::PriceInfo;
use log::{info, warn, debug, error};
use bitcoin::{Address, Network};
use tokio::time::timeout;
use std::str::FromStr; // Added import

const MAX_SCAN_RETRIES: u32 = 3;
const SCAN_TIMEOUT_SECS: u64 = 60;

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
    debug!("Starting sync for user {}", user_id);
    let start_time = Instant::now();

    let stabilization_price = if deribit_price <= 0.0 {
        warn!("Invalid Deribit price: {}, using median {}", deribit_price, price_info.raw_btc_usd);
        let median = price_info.raw_btc_usd;
        if median <= 0.0 {
            async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                price_info.raw_btc_usd
            }.await
        } else {
            median
        }
    } else {
        deribit_price
    };
    if stabilization_price <= 0.0 {
        return Err(PulserError::PriceFeedError("No valid price available after retry".to_string()));
    }

    let mut previous_utxos = chain.utxos.clone();
    let previous_balance = wallet.balance();

    // Sync with revealed script pubkeys
    let sync_request = wallet.start_sync_with_revealed_spks();
    let sync_update = esplora.sync(sync_request, 5).await?;
    wallet.apply_update(sync_update)?;
    debug!("Applied sync update for user {}, tip height: {}", user_id, wallet.local_chain().tip().height()); // Fixed

    let wallet_utxos = wallet.list_unspent();
    let mut new_utxos: Vec<UtxoInfo> = Vec::new();
    let mut updated_utxos: Vec<Utxo> = Vec::new();

    for u in wallet_utxos {
        let confirmations = match u.chain_position {
            bdk_chain::ChainPosition::Confirmed { anchor, .. } => 
                wallet.latest_checkpoint().height() - anchor.block_id.height + 1,
            bdk_chain::ChainPosition::Unconfirmed { .. } => 0,
        };
        let is_change = u.keychain == KeychainKind::Internal;

        let utxo_info = UtxoInfo {
            txid: u.outpoint.txid.to_string(),
            vout: u.outpoint.vout,
            amount_sat: u.txout.value.to_sat(),
            address: Address::from_script(&u.txout.script_pubkey, Network::Testnet)?.to_string(),
            confirmations,
            spent: u.is_spent,
            stable_value_usd: 0.0,
            keychain: if is_change { "Internal" } else { "External" }.to_string(),
            timestamp: chrono::Utc::now().timestamp() as u64,
            participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
            spendable: confirmations >= min_confirmations,
            derivation_path: "".to_string(),
        };

        let mut chain_utxo = Utxo {
            txid: utxo_info.txid.clone(),
            vout: utxo_info.vout,
            amount: utxo_info.amount_sat,
            script_pubkey: u.txout.script_pubkey.to_hex_string(),
            confirmations,
            height: None,
            usd_value: None,
            spent: utxo_info.spent,
        };

        if let Some(existing) = previous_utxos.iter_mut().find(|prev| prev.txid == chain_utxo.txid && prev.vout == chain_utxo.vout) {
            chain_utxo.usd_value = existing.usd_value.clone();
            if !is_change && confirmations >= min_confirmations && chain_utxo.usd_value.is_none() {
                let stable_value_usd = (chain_utxo.amount as f64 / 100_000_000.0) * stabilization_price;
                chain_utxo.usd_value = Some(USD(stable_value_usd));
                let stabilized_utxo = UtxoInfo { stable_value_usd, ..utxo_info.clone() };
                new_utxos.push(stabilized_utxo.clone());
                chain.history.push(stabilized_utxo);
                info!("Stabilized existing UTXO for user {}: {}:{} | {} sats (${:.2})", 
                      user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount, stable_value_usd);
            }
            updated_utxos.push(chain_utxo);
        } else if !is_change {
            let stable_value_usd = if confirmations >= min_confirmations {
                (chain_utxo.amount as f64 / 100_000_000.0) * stabilization_price
            } else {
                0.0
            };
            chain_utxo.usd_value = if confirmations >= min_confirmations { Some(USD(stable_value_usd)) } else { None };
            let stabilized_utxo = UtxoInfo { stable_value_usd, ..utxo_info.clone() };
            new_utxos.push(stabilized_utxo.clone());
            chain.history.push(stabilized_utxo);
            info!("Found {} external UTXO for user {}: {}:{} | {} sats (${:.2})", 
                  if confirmations >= min_confirmations { "stabilized" } else { "unconfirmed" }, 
                  user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount, stable_value_usd);
            updated_utxos.push(chain_utxo);
        } else {
            info!("Tracked change UTXO for user {}: {}:{} | {} sats (unstabilized)", 
                  user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount);
            updated_utxos.push(chain_utxo);
        }
    }

    chain.utxos = updated_utxos;
    chain.accumulated_btc = Bitcoin::from_sats(wallet.balance().total().to_sat());
    chain.stabilized_usd = USD(chain.utxos.iter().filter(|u| u.usd_value.is_some()).map(|u| u.usd_value.as_ref().unwrap().0).sum());
    chain.raw_btc_usd = stabilization_price;

    if !new_utxos.is_empty() {
        let btc_delta = (wallet.balance().confirmed.to_sat() - previous_balance.confirmed.to_sat()) as f64 / 100_000_000.0;
        let usd_delta: f64 = new_utxos.iter().map(|u| u.stable_value_usd).sum();
        chain.log_change("deposit", btc_delta, usd_delta, "deposit-service", Some(format!("{} UTXOs confirmed", new_utxos.len())));
        let new_addr = wallet.reveal_next_address(KeychainKind::External).address;
        chain.old_addresses.push(chain.multisig_addr.clone());
        chain.multisig_addr = new_addr.to_string();
        info!("Updated deposit address for user {}: {}", user_id, chain.multisig_addr);
    }

    state_manager.save_stable_chain(user_id, chain).await?;
    if let Some(changeset) = wallet.take_staged() {
        state_manager.save_changeset(user_id, &changeset).await?;
    }

    debug!("Completed sync for user {} in {}ms: {} new UTXOs", user_id, start_time.elapsed().as_millis(), new_utxos.len());
    Ok(new_utxos)
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

    let stabilization_price = price_info.raw_btc_usd;
    if stabilization_price <= 0.0 {
        return Err(PulserError::PriceFeedError("No valid price available for stabilization".to_string()));
    }

    let mut previous_utxos = chain.utxos.clone();
    let previous_balance = wallet.balance();

    let mut scan_success = false;
    let mut update = None;
    let mut last_error = None;

    for attempt in 0..MAX_SCAN_RETRIES {
        if attempt > 0 {
            debug!("Retry {} of {} for full scan for user {}", attempt, MAX_SCAN_RETRIES, user_id);
            tokio::time::sleep(Duration::from_millis(500 * (1 << attempt))).await;
        }

        let request = wallet.start_full_scan();
        match timeout(Duration::from_secs(SCAN_TIMEOUT_SECS), esplora.full_scan(request, 10, 5)).await {
            Ok(Ok(full_update)) => {
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
    debug!("Full scan completed for user {}, tip height: {}", user_id, wallet.local_chain().tip().height()); // Fixed

    let wallet_utxos = wallet.list_unspent();
    let mut new_utxos: Vec<UtxoInfo> = Vec::new();
    let mut updated_utxos: Vec<Utxo> = Vec::new();

    for u in wallet_utxos {
        let confirmations = match u.chain_position {
            bdk_chain::ChainPosition::Confirmed { anchor, .. } => wallet.latest_checkpoint().height() - anchor.block_id.height + 1,
            bdk_chain::ChainPosition::Unconfirmed { .. } => 0,
        };
        let is_change = u.keychain == KeychainKind::Internal;
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
            keychain: if is_change { "Internal" } else { "External" }.to_string(),
            timestamp: chrono::Utc::now().timestamp() as u64,
            participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
            spendable: confirmations >= min_confirmations,
            derivation_path: "".to_string(),
        };

        let mut chain_utxo = Utxo {
            txid: utxo_info.txid.clone(),
            vout: utxo_info.vout,
            amount: utxo_info.amount_sat,
            script_pubkey: u.txout.script_pubkey.to_hex_string(),
            confirmations,
            height: None,
            usd_value: None,
            spent: utxo_info.spent,
        };

        if let Some(existing) = previous_utxos.iter_mut().find(|prev| prev.txid == chain_utxo.txid && prev.vout == chain_utxo.vout) {
            existing.confirmations = confirmations;
            existing.spent = u.is_spent;
            chain_utxo.usd_value = existing.usd_value.clone();
            if !is_change && confirmations >= min_confirmations && chain_utxo.usd_value.is_none() {
                let stable_value_usd = (chain_utxo.amount as f64 / 100_000_000.0) * stabilization_price;
                chain_utxo.usd_value = Some(USD(stable_value_usd));
                let stabilized_utxo = UtxoInfo { stable_value_usd, ..utxo_info.clone() };
                new_utxos.push(stabilized_utxo.clone());
                chain.history.push(stabilized_utxo);
                info!("Stabilized recovered UTXO for user {}: {}:{} | {} sats (${:.2})", 
                      user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount, stable_value_usd);
            }
            updated_utxos.push(chain_utxo);
        } else if !is_change {
            let stable_value_usd = if confirmations >= min_confirmations {
                (chain_utxo.amount as f64 / 100_000_000.0) * stabilization_price
            } else {
                0.0
            };
            chain_utxo.usd_value = if confirmations >= min_confirmations { Some(USD(stable_value_usd)) } else { None };
            let stabilized_utxo = UtxoInfo { stable_value_usd, ..utxo_info.clone() };
            new_utxos.push(stabilized_utxo.clone());
            chain.history.push(stabilized_utxo);
            info!("Recovered {} external UTXO for user {}: {}:{} | {} sats (${:.2})", 
                  if confirmations >= min_confirmations { "stabilized" } else { "unconfirmed" }, 
                  user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount, stable_value_usd);
            updated_utxos.push(chain_utxo);
        } else {
            info!("Recovered change UTXO for user {}: {}:{} | {} sats (unstabilized)", 
                  user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount);
            updated_utxos.push(chain_utxo);
        }
    }

    chain.utxos = updated_utxos;
    chain.accumulated_btc = Bitcoin::from_sats(wallet.balance().total().to_sat());
    chain.stabilized_usd = USD(chain.utxos.iter().filter(|u| u.usd_value.is_some()).map(|u| u.usd_value.as_ref().unwrap().0).sum());
    chain.raw_btc_usd = stabilization_price;

    if !new_utxos.is_empty() {
        let btc_delta = (wallet.balance().confirmed.to_sat() - previous_balance.confirmed.to_sat()) as f64 / 100_000_000.0;
        let usd_delta: f64 = new_utxos.iter().map(|u| u.stable_value_usd).sum();
        chain.log_change(
            "recovery",
            btc_delta,
            usd_delta,
            "full-resync",
            Some(format!("{} UTXOs recovered via full scan", new_utxos.len())),
        );
    }

    let current_addr = wallet.peek_address(KeychainKind::External, wallet.derivation_index(KeychainKind::External).unwrap_or(0)).address;
    if chain.multisig_addr.is_empty() || !wallet.is_mine(Address::from_str(&chain.multisig_addr)?.assume_checked().script_pubkey()) {
        chain.multisig_addr = current_addr.to_string();
        info!("Restored deposit address for user {} after full resync: {}", user_id, chain.multisig_addr);
    }

    state_manager.save_stable_chain(user_id, chain).await?;
    if let Some(changeset) = wallet.take_staged() {
        state_manager.save_changeset(user_id, &changeset).await?;
    }

    debug!("Completed full resync for user {} in {}ms: {} new UTXOs", 
           user_id, start_time.elapsed().as_millis(), new_utxos.len());
    Ok(new_utxos)
}
