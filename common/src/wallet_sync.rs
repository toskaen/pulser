use std::time::{Duration, Instant};
use bdk_wallet::{Wallet, KeychainKind};
use bdk_esplora::EsploraAsyncExt;
use bdk_esplora::esplora_client::AsyncClient;
use crate::types::{StableChain, UtxoInfo, USD, Bitcoin, Utxo, PriceInfo};
use crate::error::PulserError;
use crate::StateManager;
use crate::price_feed::{PriceFeed, PriceFeedExtensions};
use log::{info, warn, debug, error, trace};
use bitcoin::{Address, Network};
use tokio::time::timeout;
use std::str::FromStr;
use std::sync::Arc;
use bdk_chain::spk_client::SyncRequest;
use crate::utils;
use crate::types::ChangeEntry;
use std::collections::HashSet;


const MAX_SCAN_RETRIES: u32 = 3;
const SCAN_TIMEOUT_SECS: u64 = 60;

/// Analyzes market conditions to determine the optimal stabilization price.
/// Always selects the higher of spot or futures price to maximize user value,
/// allowing the operator to improve defense from hedging short at the futures price.
/// Retains detailed logging for contango/backwardation analysis.
async fn determine_stabilization_price(
    price_feed: &Arc<PriceFeed>,
    user_id: &str,
    _is_new_confirmation: bool, // Kept for compatibility, ignored in decision
    _min_advantage_threshold: f64, // Kept for compatibility, ignored in decision
) -> Result<(f64, String, f64), PulserError> {
    // Fetch spot VWAP price
    let vwap_info = match price_feed.get_price().await {
        Ok(info) => info,
        Err(e) => {
            warn!("Failed to get VWAP price for user {}: {}, trying fallback", user_id, e);
            let client = reqwest::Client::new();
            match crate::price_feed::http_sources::fetch_btc_usd_price(&client).await {
                Ok(price) => {
                    let now = crate::utils::now_timestamp();
                    let mut feeds = std::collections::HashMap::new();
                    feeds.insert("HTTP_Fallback".to_string(), price);
                    PriceInfo {
                        raw_btc_usd: price,
                        timestamp: now,
                        price_feeds: feeds,
                    }
                },
                Err(e2) => return Err(PulserError::PriceFeedError(format!("Both VWAP and fallback price fetches failed: {} / {}", e, e2))),
            }
        }
    };
    
    let spot_price = vwap_info.raw_btc_usd;
    if spot_price <= 0.0 {
        return Err(PulserError::PriceFeedError("Invalid VWAP price (zero or negative)".to_string()));
    }
    
    // Fetch futures price from Deribit
    let futures_price = match price_feed.get_deribit_price().await {
        Ok(price) => price,
        Err(e) => {
            warn!("Failed to get Deribit futures price for user {}: {}, defaulting to spot VWAP", user_id, e);
            return Ok((spot_price, "VWAP (futures unavailable)".to_string(), 0.0));
        }
    };
    
    if futures_price <= 0.0 {
        warn!("Invalid futures price (zero or negative), defaulting to spot VWAP");
        return Ok((spot_price, "VWAP (invalid futures)".to_string(), 0.0));
    }
    
    // Calculate basis for logging and analysis
    let basis = ((futures_price / spot_price) - 1.0) * 100.0;
    
    // Always choose the higher price
    let (stab_price, source) = if spot_price >= futures_price {
        info!("Spot price selected for user {}: ${:.2} > futures ${:.2}, basis {:.2}% (backwardation)", 
              user_id, spot_price, futures_price, basis);
        (spot_price, format!("VWAP (backwardation {:.2}%)", basis))
    } else {
        info!("Futures price selected for user {}: ${:.2} > spot ${:.2}, basis {:.2}% (contango)", 
              user_id, futures_price, spot_price, basis);
        (futures_price, format!("Deribit (contango {:.2}%)", basis))
    };
    
    debug!("Market analysis for user {}: Spot=${:.2}, Futures=${:.2}, Basis={:.2}%", 
           user_id, spot_price, futures_price, basis);
    
    Ok((stab_price, source, basis))
}

/// Updates StableChain with newly confirmed deposits applying optimal pricing.
/// Retains detailed logging for hedging strategy analysis.
async fn process_new_confirmation(
    user_id: &str,
    utxo: &mut crate::types::Utxo,
    chain: &mut StableChain,
    price_feed: Arc<PriceFeed>,
) -> Result<(f64, String, f64), PulserError> {
    if utxo.usd_value.is_some() {
        debug!("UTXO {}:{} for user {} already stabilized at ${:.2}", 
               utxo.txid, utxo.vout, user_id, utxo.usd_value.as_ref().unwrap().0); // Borrow with as_ref()
        return Ok((utxo.usd_value.as_ref().unwrap().0, "existing".to_string(), 0.0)); // Borrow with as_ref()
    }

    
    let (stab_price, source, market_basis) = determine_stabilization_price(
        &price_feed,
        user_id,
        true,  // Ignored in new logic, kept for compatibility
        0.25   // Ignored in new logic, kept for compatibility
    ).await?;
    
    let btc_amount = utxo.amount as f64 / 100_000_000.0;
    let stable_value_usd = btc_amount * stab_price;
    utxo.usd_value = Some(crate::types::USD(stable_value_usd));
    
    info!("Stabilized UTXO for user {}: {}:{} | {} sats (${:.2} at ${:.2} using {})", 
          user_id, utxo.txid, utxo.vout, utxo.amount, stable_value_usd, stab_price, source);
    
    // Log potential hedging benefit for analysis, even though price is always max(spot, futures)
    let futures_price = match price_feed.get_deribit_price().await {
        Ok(price) => price,
        Err(_) => {
            warn!("Failed to fetch futures price for benefit calc for user {}, using stab price ${:.2}", user_id, stab_price);
            stab_price
        }
    };
    let futures_value = btc_amount * futures_price;
    let benefit = stable_value_usd - futures_value;
    
    if market_basis < 0.0 { // Backwardation (futures < spot)
        if benefit > 0.0 {
            info!("Operator hedging benefit for user {}: ${:.2} (basis {:.2}% backwardation)", 
                  user_id, benefit, market_basis);
            chain.log_change(
                "hedging_benefit",
                0.0,
                benefit,
                "price-feed",
                Some(format!("Hedging benefit at basis {:.2}% (backwardation)", market_basis))
            );
        }
    } else if market_basis > 0.0 { // Contango (futures > spot)
        if benefit < 0.0 { // Negative benefit means futures hedge costs more
            info!("Operator hedging cost for user {}: ${:.2} (basis {:.2}% contango)", 
                  user_id, -benefit, market_basis);
            chain.log_change(
                "hedging_cost",
                0.0,
                -benefit,
                "price-feed",
                Some(format!("Hedging cost at basis {:.2}% (contango)", market_basis))
            );
        }
    }
    
    chain.stabilized_usd.0 += stable_value_usd;
    chain.raw_btc_usd = stab_price;
    
    Ok((stable_value_usd, source, market_basis))
}

/// Syncs wallet with blockchain and stabilizes any new UTXOs.
/// Maintains extensive logging for market condition analysis.
pub async fn sync_and_stabilize_utxos(
    user_id: &str,
    wallet: &mut Wallet,
    esplora: &AsyncClient,
    chain: &mut StableChain,
    price_feed: Arc<PriceFeed>,
    price_info: &PriceInfo,
    deposit_addr: &Address,
    change_addr: &Address,
    state_manager: &StateManager,
    min_confirmations: u32,
) -> Result<Vec<UtxoInfo>, PulserError> {
    debug!("Starting sync for user {}", user_id);
    let start_time = Instant::now();

    let (stabilization_price, price_source, basis) = determine_stabilization_price(
        &price_feed,
        user_id,
        false,  // Ignored in new logic
        0.25    // Ignored in new logic
    ).await?;
    
    if stabilization_price <= 0.0 {
        return Err(PulserError::PriceFeedError("No valid price available after analysis".to_string()));
    }
    trace!("Initial stabilization price for user {}: ${:.2} (source: {})", user_id, stabilization_price, price_source);

    let mut previous_utxos = chain.utxos.clone();
    let previous_balance = wallet.balance();

    let sync_request = wallet.start_sync_with_revealed_spks();
    let sync_update = esplora.sync(sync_request, 5).await?;
    wallet.apply_update(sync_update)?;
    debug!("Applied sync update for user {}, tip height: {}", user_id, wallet.local_chain().tip().height());

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
            height: Some(confirmations),
            usd_value: None,
            spent: utxo_info.spent,
        };

        if let Some(existing) = previous_utxos.iter_mut().find(|prev| prev.txid == chain_utxo.txid && prev.vout == chain_utxo.vout) {
            chain_utxo.usd_value = existing.usd_value.clone();
            
            if !is_change && confirmations >= min_confirmations && chain_utxo.usd_value.is_none() {
                let (stable_value_usd, source, market_basis) = process_new_confirmation(
                    user_id, &mut chain_utxo, chain, price_feed.clone()
                ).await?;
                let stabilized_utxo = UtxoInfo { stable_value_usd, ..utxo_info.clone() };
                new_utxos.push(stabilized_utxo.clone());
                chain.history.push(stabilized_utxo);
                
                info!("Stabilized existing UTXO for user {}: {}:{} | {} sats (${:.2} using {})", 
                      user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount, stable_value_usd, source);
                
                // Additional logging for hedging analysis
                let futures_price = match price_feed.get_deribit_price().await {
                    Ok(price) => price,
                    Err(_) => stabilization_price,
                };
                let btc_amount = chain_utxo.amount as f64 / 100_000_000.0;
                let futures_value = btc_amount * futures_price;
                let benefit = stable_value_usd - futures_value;
                if market_basis < 0.0 && benefit > 0.0 {
                    info!("Operator hedging benefit (existing UTXO) for user {}: ${:.2} (basis {:.2}% backwardation)", 
                          user_id, benefit, market_basis);
                    chain.log_change(
                        "hedging_benefit",
                        0.0,
                        benefit,
                        "price-feed",
                        Some(format!("Hedging benefit at basis {:.2}% (backwardation)", market_basis))
                    );
                } else if market_basis > 0.0 && benefit < 0.0 {
                    info!("Operator hedging cost (existing UTXO) for user {}: ${:.2} (basis {:.2}% contango)", 
                          user_id, -benefit, market_basis);
                    chain.log_change(
                        "hedging_cost",
                        0.0,
                        -benefit,
                        "price-feed",
                        Some(format!("Hedging cost at basis {:.2}% (contango)", market_basis))
                    );
                }
            }
            updated_utxos.push(chain_utxo);
        } else if !is_change {
            let (stable_value_usd, source) = if confirmations >= min_confirmations {
                let (value, src, market_basis) = process_new_confirmation(
                    user_id, &mut chain_utxo, chain, price_feed.clone()
                ).await?;
                let futures_price = match price_feed.get_deribit_price().await {
                    Ok(price) => price,
                    Err(_) => stabilization_price,
                };
                let btc_amount = chain_utxo.amount as f64 / 100_000_000.0;
                let futures_value = btc_amount * futures_price;
                let benefit = value - futures_value;
                if market_basis < 0.0 && benefit > 0.0 {
                    info!("Operator hedging benefit (new UTXO) for user {}: ${:.2} (basis {:.2}% backwardation)", 
                          user_id, benefit, market_basis);
                    chain.log_change(
                        "hedging_benefit",
                        0.0,
                        benefit,
                        "price-feed",
                        Some(format!("Hedging benefit at basis {:.2}% (backwardation)", market_basis))
                    );
                } else if market_basis > 0.0 && benefit < 0.0 {
                    info!("Operator hedging cost (new UTXO) for user {}: ${:.2} (basis {:.2}% contango)", 
                          user_id, -benefit, market_basis);
                    chain.log_change(
                        "hedging_cost",
                        0.0,
                        -benefit,
                        "price-feed",
                        Some(format!("Hedging cost at basis {:.2}% (contango)", market_basis))
                    );
                }
                (value, src)
            } else {
                (0.0, "pending".to_string())
            };
            
            let stabilized_utxo = UtxoInfo { stable_value_usd, ..utxo_info.clone() };
            new_utxos.push(stabilized_utxo.clone());
            chain.history.push(stabilized_utxo);
            
            info!("Found {} external UTXO for user {}: {}:{} | {} sats (${:.2} {})", 
                  if confirmations >= min_confirmations { "stabilized" } else { "unconfirmed" }, 
                  user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount, stable_value_usd,
                  if confirmations >= min_confirmations { source } else { "pending".to_string() });
            updated_utxos.push(chain_utxo);
        } else {
            info!("Tracked change UTXO for user {}: {}:{} | {} sats (unstabilized)", 
                  user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount);
            updated_utxos.push(chain_utxo);
        }
    }

    chain.utxos = updated_utxos;
    chain.accumulated_btc = Bitcoin::from_sats(wallet.balance().total().to_sat());
    chain.stabilized_usd = USD(chain.utxos.iter()
        .filter(|u| u.usd_value.is_some())
        .map(|u| u.usd_value.as_ref().unwrap().0)
        .sum());
    chain.raw_btc_usd = stabilization_price;

    if !new_utxos.is_empty() {
        let btc_delta = (wallet.balance().confirmed.to_sat() - previous_balance.confirmed.to_sat()) as f64 / 100_000_000.0;
        let usd_delta: f64 = new_utxos.iter().map(|u| u.stable_value_usd).sum();
        
        info!("Deposit detected for user {}: {:.8} BTC (${:.2}) across {} UTXOs, basis {:.2}%", 
              user_id, btc_delta, usd_delta, new_utxos.len(), basis);
        chain.log_change(
            "deposit", 
            btc_delta, 
            usd_delta, 
            "deposit-service", 
            Some(format!("{} UTXOs confirmed using {}, basis {:.2}%", new_utxos.len(), price_source, basis))
        );
        
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

/// Performs a full history resync for a wallet.

pub async fn resync_full_history(
    user_id: &str,
    wallet: &mut Wallet,
    esplora: &AsyncClient,
    chain: &mut StableChain,
    price_feed: Arc<PriceFeed>,
    price_info: &PriceInfo,
    state_manager: &StateManager,
    min_confirmations: u32,
) -> Result<Vec<UtxoInfo>, PulserError> {
    debug!("Starting full history resync for user {}", user_id);
    
    // IMPORTANT: Save existing history and logs before any operations
    let existing_history = chain.history.clone();
    let existing_change_log = chain.change_log.clone();
    
    // Perform all the sync operations as usual
    let mut spks = vec![Address::from_str(&chain.multisig_addr)?.assume_checked().script_pubkey()];
    
    for old_addr in &chain.old_addresses {
        if let Ok(addr) = Address::from_str(old_addr) {
            spks.push(addr.assume_checked().script_pubkey());
        }
    }
    
    // Add all wallet derived addresses
    for i in 0..=wallet.derivation_index(KeychainKind::External).unwrap_or(10) {
        spks.push(wallet.peek_address(KeychainKind::External, i).address.script_pubkey());
    }
    for i in 0..=wallet.derivation_index(KeychainKind::Internal).unwrap_or(10) {
        spks.push(wallet.peek_address(KeychainKind::Internal, i).address.script_pubkey());
    }
    
    let request = SyncRequest::builder().spks(spks.into_iter()).build();
    let update = esplora.sync(request, 5).await?;
    wallet.apply_update(update)?;
    
    let change_addr = wallet.reveal_next_address(KeychainKind::Internal).address;
    let deposit_addr = Address::from_str(&chain.multisig_addr)?.assume_checked();
    
    let new_utxos = sync_and_stabilize_utxos(
        user_id,
        wallet,
        esplora,
        chain,
        price_feed,
        price_info,
        &deposit_addr,
        &change_addr,
        state_manager,
        min_confirmations,
    ).await?;
    
    // IMPORTANT: After sync is complete, merge back the saved history
    
    // Create a set to detect duplicates by (txid, vout) tuple
    let mut txid_set = HashSet::new();
    for utxo in &chain.history {
        txid_set.insert((utxo.txid.clone(), utxo.vout));
    }
    
    // Add back original history that doesn't conflict
    for utxo in existing_history {
        let key = (utxo.txid.clone(), utxo.vout);
        if !txid_set.contains(&key) {
            chain.history.push(utxo.clone());
            txid_set.insert(key);
        }
    }
    
    // Add an entry in the change log for this resync
    let mut merged_logs = existing_change_log;
    merged_logs.push(ChangeEntry {
        timestamp: chrono::Utc::now().timestamp(),
        change_type: "force_resync".to_string(),
        btc_delta: 0.0,
        usd_delta: 0.0,
        service: "deposit-service".to_string(),
        details: Some(format!("Full history resync with {} new UTXOs", new_utxos.len())),
    });
    
    chain.change_log = merged_logs;
    
    info!("Completed full history resync for user {}: {} new UTXOs with history preserved", 
         user_id, new_utxos.len());
    
    Ok(new_utxos)
}

// Helper function to check if an address has transactions in mempool (unchanged)
pub async fn check_address_mempool(
    client: &reqwest::Client,
    esplora_url: &str,
    address: &str
) -> Result<bool, PulserError> {
    let url = format!("{}/address/{}/mempool", esplora_url, address);
    match tokio::time::timeout(Duration::from_secs(10), client.get(&url).send()).await {
        Ok(Ok(response)) => {
            if response.status().is_success() {
                let txs: Vec<serde_json::Value> = response.json().await?;
                Ok(!txs.is_empty())
            } else {
                Ok(false)
            }
        },
        _ => Ok(false) // Default to false on any error
    }
}

// Helper function to validate a wallet is properly initialized (unchanged)
pub async fn validate_wallet_state(
    wallet: &Wallet,
    chain: &StableChain
) -> Result<bool, PulserError> {
    let external_index = wallet.derivation_index(KeychainKind::External).unwrap_or(0);
    let internal_index = wallet.derivation_index(KeychainKind::Internal).unwrap_or(0);
    let tip_height = wallet.latest_checkpoint().height();
    let has_valid_address = Address::from_str(&chain.multisig_addr).is_ok();
    let is_valid = external_index > 0 || internal_index > 0 || tip_height > 0 || has_valid_address;
    Ok(is_valid)
}
