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
use crate::types::{UtxoOrigin, ChangeEntry};
use std::collections::{HashMap, HashSet};
use tokio::fs::File;
use bincode::config;
use crate::types::Event;

pub struct Config {
    pub min_confirmations: u32,
    pub service_min_confirmations: u32,
    pub external_min_confirmations: u32,
}

const MAX_SCAN_RETRIES: u32 = 3;
const SCAN_TIMEOUT_SECS: u64 = 60;

/// Analyzes market conditions and returns the optimal stabilization price.
/// Always selects the higher of aggregated spot or futures price to maximize user value.
async fn determine_stabilization_price(
    price_feed: &Arc<PriceFeed>,
    user_id: &str,
) -> Result<(f64, String, f64), PulserError> {
    // Get the current spot price
    let vwap_info = match price_feed.get_price().await {
        Ok(info) => info,
        Err(e) => {
            warn!("Failed to get aggregated spot price for user {}: {}, trying fallback", user_id, e);
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
                Err(e2) => return Err(PulserError::PriceFeedError(format!("Both aggregated and fallback price fetches failed: {} / {}", e, e2))),
            }
        }
    };
    
    let spot_price = vwap_info.raw_btc_usd;
    if spot_price <= 0.0 {
        return Err(PulserError::PriceFeedError("Invalid spot price (zero or negative)".to_string()));
    }
    
    // Fetch futures price from Deribit
    let futures_price = match price_feed.get_deribit_price().await {
        Ok(price) => price,
        Err(e) => {
            warn!("Failed to get Deribit futures price for user {}: {}, defaulting to spot price", user_id, e);
            return Ok((spot_price, "Spot (futures unavailable)".to_string(), 0.0));
        }
    };
    
    if futures_price <= 0.0 {
        warn!("Invalid futures price (zero or negative), defaulting to spot price");
        return Ok((spot_price, "Spot (invalid futures)".to_string(), 0.0));
    }
    
    // Calculate basis for logging and analysis
    let basis = ((futures_price / spot_price) - 1.0) * 100.0;
    
    // Always choose the higher price
    let (stab_price, source) = if spot_price >= futures_price {
        (spot_price, format!("Spot (backwardation {:.2}%)", basis))
    } else {
        (futures_price, format!("Deribit (contango {:.2}%)", basis))
    };

    // Downgrade to trace! instead of debug!
    trace!(
        "Market analysis for user {}: Spot=${:.2}, Futures=${:.2}, Basis={:.2}%",
        user_id, spot_price, futures_price, basis
    );
    
    return Ok((stab_price, source, basis));
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
               utxo.txid, utxo.vout, user_id, utxo.usd_value.as_ref().unwrap().0); 
        return Ok((utxo.usd_value.as_ref().unwrap().0, "existing".to_string(), 0.0));
    }
    
    let (stab_price, source, market_basis) = determine_stabilization_price(
        &price_feed,
        user_id
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
            trace!("Operator hedging benefit for user {}: ${:.2} (basis {:.2}% backwardation)", 
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
            trace!("Operator hedging cost for user {}: ${:.2} (basis {:.2}% contango)", 
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
    mempool_timestamps: &HashMap<(String, u32), u64>, // Mempool detection times
    config: &Config, // Config for different confirmation requirements
) -> Result<Vec<UtxoInfo>, PulserError> {
    debug!("Starting sync for user {}", user_id);
    let start_time = Instant::now();
    
    // Get config values
    let service_min_confirmations = config.service_min_confirmations;
    let external_min_confirmations = config.external_min_confirmations;
    
    let needs_price_fetch = chain.utxos.iter().any(|utxo| utxo.usd_value.is_none());
    let (stabilization_price, price_source, basis) = if needs_price_fetch {
        determine_stabilization_price(&price_feed, user_id).await?
    } else {
        (price_info.raw_btc_usd, "cached".to_string(), 0.0)
    };
    
    if stabilization_price <= 0.0 {
        return Err(PulserError::PriceFeedError("No valid price available after analysis".to_string()));
    }
    trace!("Initial stabilization price for user {}: ${:.2} (source: {})", user_id, stabilization_price, price_source);

    let mut previous_utxos = chain.utxos.clone();
    let previous_balance = wallet.balance();
    let mut old_addresses_set: HashSet<String> = chain.old_addresses.iter().cloned().collect();
    let mut spks = vec![deposit_addr.script_pubkey()];
    
    for old_addr in &chain.old_addresses {
        if let Ok(addr) = Address::from_str(old_addr) {
            let script_pubkey = addr.assume_checked().script_pubkey();
            if !spks.contains(&script_pubkey) {
                spks.push(script_pubkey);
            }
        }
    }
    
    let sync_request = wallet.start_sync_with_revealed_spks();
    let sync_update = esplora.sync(sync_request, 5).await?;
    wallet.apply_update(sync_update)?;
    debug!("Applied sync update for user {}, tip_height: {}", user_id, wallet.local_chain().tip().height());

    // Collect wallet UTXOs once to avoid iterator consumption issues
    let wallet_utxos_vec: Vec<_> = wallet.list_unspent().collect();
    
    let mut new_utxos: Vec<UtxoInfo> = Vec::new();
    let mut updated_regular_utxos: Vec<Utxo> = Vec::new();
    let mut updated_change_utxos: Vec<Utxo> = Vec::new();
    let mut ignored_utxos: Vec<Utxo> = Vec::new();
    let now = chrono::Utc::now().timestamp() as u64;
    
    // Initialize change address mappings if needed
    if chain.change_address_mappings.is_empty() {
        chain.change_address_mappings = HashMap::new();
    }
    
    // Initialize trusted service addresses if needed
    if chain.trusted_service_addresses.is_empty() {
        chain.trusted_service_addresses = HashSet::new();
    }
    
    // Create a map of existing UTXOs with their block heights for reorg detection
    let existing_heights: HashMap<(String, u32), Option<u32>> = chain.utxos.iter()
        .map(|u| ((u.txid.clone(), u.vout), u.height))
        .collect();

// Map of spent UTXOs for tracking withdrawals and change
let mut spent_utxos: HashMap<String, Vec<Utxo>> = HashMap::new();
for utxo in &previous_utxos {
    if !wallet_utxos_vec.iter().any(|wu| wu.outpoint.txid.to_string() == utxo.txid && wu.outpoint.vout == utxo.vout) {
        // This UTXO was previously tracked but is now spent
        // Clone the utxo instead of storing a reference
        spent_utxos.entry(utxo.txid.clone()).or_default().push(utxo.clone());
    }
}
    // Track any reorgs detected during this sync
    let mut reorgs_detected = Vec::new();

    for u in &wallet_utxos_vec {
        let confirmations = match u.chain_position {
            bdk_chain::ChainPosition::Confirmed { anchor, .. } => 
                wallet.latest_checkpoint().height() - anchor.block_id.height + 1,
            bdk_chain::ChainPosition::Unconfirmed { .. } => 0,
        };
        let is_change = u.keychain == KeychainKind::Internal;
        let block_height = match u.chain_position {
            bdk_chain::ChainPosition::Confirmed { anchor, .. } => Some(anchor.block_id.height),
            bdk_chain::ChainPosition::Unconfirmed { .. } => None,
        };
        
        // Get address string for this UTXO
        let address = match Address::from_script(&u.txout.script_pubkey, Network::Testnet) {
            Ok(addr) => addr.to_string(),
            Err(e) => {
                warn!("Failed to parse address for UTXO {}:{}: {}", u.outpoint.txid, u.outpoint.vout, e);
                continue;
            }
        };
        
        // Determine origin and required confirmations
        let (origin, required_confirmations) = if is_change {
            // Is this a known change address from our withdrawals?
            if chain.change_address_mappings.contains_key(&address) {
                (UtxoOrigin::WithdrawalChange, min_confirmations)
            } else {
                // This is an unexpected deposit to a change address - IGNORE IT
                warn!("Ignoring deposit to change address: {}:{} | {} sats",
                     u.outpoint.txid, u.outpoint.vout, u.txout.value.to_sat());
                
                // Log as unusual activity
               chain.events.push(Event {
                    timestamp: utils::now_timestamp(),
                    source: "deposit-service".to_string(),
                    kind: "unexpected-deposit".to_string(),
                    details: format!("{}-{}", u.outpoint.txid, u.outpoint.vout),
                });
                
                let ignored_utxo = Utxo {
                    txid: u.outpoint.txid.to_string(),
                    vout: u.outpoint.vout,
                    amount: u.txout.value.to_sat(),
                    script_pubkey: u.txout.script_pubkey.to_hex_string(),
                    confirmations,
                    height: block_height,
                    usd_value: None, // No USD value
                    spent: u.is_spent,
                };
                
                ignored_utxos.push(ignored_utxo);
                continue; // Skip further processing of this UTXO
            }
        } else if chain.trusted_service_addresses.contains(&address) {
            // This is from a trusted service address - reduced confirmations
            (UtxoOrigin::ServiceDeposit, service_min_confirmations)
        } else {
            // Regular external deposit - standard or higher confirmations
            (UtxoOrigin::ExternalDeposit, external_min_confirmations.max(min_confirmations))
        };
        
        // Check for reorg - an existing UTXO with a different block height
        let utxo_key = (u.outpoint.txid.to_string(), u.outpoint.vout);
        if let Some(Some(old_height)) = existing_heights.get(&utxo_key) {
            if let Some(new_height) = block_height {
                if *old_height != new_height {
                    warn!("Blockchain reorganization detected for tx {}:{}: block height changed from {} to {}", 
                          u.outpoint.txid, u.outpoint.vout, old_height, new_height);
                    
                    // Track this reorg for later handling
                    reorgs_detected.push((utxo_key.clone(), *old_height, new_height));
                }
            }
        }

        // Get first seen timestamp from mempool tracker or use current time
        let first_seen = mempool_timestamps
            .get(&utxo_key)
            .copied()
            .unwrap_or(now);

        // Clone origin to avoid ownership issues
        let origin_clone = origin.clone();
        
        let utxo_info = UtxoInfo {
            txid: u.outpoint.txid.to_string(),
            vout: u.outpoint.vout,
            amount_sat: u.txout.value.to_sat(),
            address: address.clone(),
            keychain: if is_change { "Internal" } else { "External" }.to_string(),
            first_seen_timestamp: first_seen,
            confirmation_timestamp: if confirmations > 0 { 
                // Use existing timestamp if available, otherwise current time
                Some(
                    previous_utxos.iter()
                        .find(|prev| prev.txid == u.outpoint.txid.to_string() && prev.vout == u.outpoint.vout)
                        .and_then(|_| chain.history.iter()
                            .find(|h| h.txid == u.outpoint.txid.to_string() && h.vout == u.outpoint.vout))
                        .and_then(|h| h.confirmation_timestamp)
                        .unwrap_or(now)
                )
            } else { 
                None 
            },
            block_height,
            confirmations,
            stable_value_usd: 0.0, // Will be set below if stabilized
            spent: u.is_spent,
            origin: origin_clone, // Use cloned value here
            parent_txid: if origin == UtxoOrigin::WithdrawalChange {
                chain.change_address_mappings.get(&address).cloned()
            } else {
                None
            },
            never_spend: false,
            
            // Legacy fields for backward compatibility
            participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
            spendable: confirmations >= required_confirmations,
            derivation_path: "".to_string(),
        };

        let mut chain_utxo = Utxo {
            txid: utxo_info.txid.clone(),
            vout: utxo_info.vout,
            amount: utxo_info.amount_sat,
            script_pubkey: u.txout.script_pubkey.to_hex_string(),
            confirmations,
            height: block_height,
            usd_value: None,
            spent: utxo_info.spent,
        };

        // Process existing UTXOs
        let mut existing_utxo_found = false;
        for existing in &mut previous_utxos {
            if existing.txid == chain_utxo.txid && existing.vout == chain_utxo.vout {
                chain_utxo.usd_value = existing.usd_value.clone();
                existing_utxo_found = true;
                break;
            }
        }
        
        if existing_utxo_found {
            if confirmations >= required_confirmations && chain_utxo.usd_value.is_none() {
                // This UTXO needs to be stabilized
                let (stable_value_usd, source, _market_basis) = process_new_confirmation(
                    user_id, &mut chain_utxo, chain, price_feed.clone()
                ).await?;
                
                let stabilized_utxo = UtxoInfo {
                    stable_value_usd,
                    ..utxo_info.clone()
                };
                
                new_utxos.push(stabilized_utxo.clone());
                chain.history.push(stabilized_utxo);
                
                // Update origin-based accounting
                if !is_change {
                    let origin_key = format!("{:?}", origin);
                    chain.stable_value_by_origin
                        .entry(origin_key)
                        .and_modify(|e| e.0 += stable_value_usd)
                        .or_insert(USD(stable_value_usd));
                }
            }
            
            // Add to the appropriate UTXO list
            if origin == UtxoOrigin::WithdrawalChange {
                updated_change_utxos.push(chain_utxo);
            } else {
                updated_regular_utxos.push(chain_utxo);
            }
        } else {
            // New UTXO that we haven't seen before
            if origin == UtxoOrigin::WithdrawalChange {
                // This is a new change UTXO from a withdrawal
                // We need to calculate its stable value based on the remaining stable balance
                let withdrawal_id = chain.change_address_mappings.get(&address)
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                
                // Calculate stable value for this change UTXO
let (stable_value_usd, parent_tx) = if let Some(spent_inputs) = spent_utxos.get(&u.outpoint.txid.to_string()) {
    if !spent_inputs.is_empty() {
        // Calculate total value of inputs that were spent
        let mut total_input_sats: u64 = 0;
        let mut total_input_usd: f64 = 0.0;
        
        for spent in spent_inputs {
            total_input_sats += spent.amount;
            if let Some(usd_value) = &spent.usd_value {
                total_input_usd += usd_value.0;
            }
        }
                        
                        if total_input_sats > 0 && total_input_usd > 0.0 {
                            // Calculate remaining stable value
                            // (Not using VWAP, but preserving the remaining stable balance)
                            let change_ratio = u.txout.value.to_sat() as f64 / total_input_sats as f64;
                            let change_value_usd = total_input_usd * change_ratio;
                            
                            (change_value_usd, Some(u.outpoint.txid.to_string()))
                        } else {
                            // Fall back to current price
                            let btc_amount = u.txout.value.to_sat() as f64 / 100_000_000.0;
                            (btc_amount * stabilization_price, None)
                        }
                    } else {
                        // Fall back to current price
                        let btc_amount = u.txout.value.to_sat() as f64 / 100_000_000.0;
                        (btc_amount * stabilization_price, None)
                    }
                } else {
                    // Try to load withdrawal details from state manager
                    match state_manager.load_withdrawal_details(user_id, &withdrawal_id).await {
                        Ok(details) if details.remaining_usd.is_some() => {
                            (details.remaining_usd.unwrap(), Some(withdrawal_id.clone()))
                        },
                        _ => {
                            // Fall back to current price
                            let btc_amount = u.txout.value.to_sat() as f64 / 100_000_000.0;
                            (btc_amount * stabilization_price, None)
                        }
                    }
                };
                
                // Set the USD value
                chain_utxo.usd_value = Some(USD(stable_value_usd));
                
                // Create change UtxoInfo with the correct stable value
                let change_utxo_info = UtxoInfo {
                    stable_value_usd,
                    parent_txid: parent_tx,
                    ..utxo_info.clone()
                };
                
                // Add to history
                chain.history.push(change_utxo_info.clone());
                
                // Update accounting for change
                let origin_key = "WithdrawalChange".to_string();
                chain.stable_value_by_origin
                    .entry(origin_key)
                    .and_modify(|e| e.0 += stable_value_usd)
                    .or_insert(USD(stable_value_usd));
                
                // Log the event
                info!("Tracked withdrawal change UTXO for user {}: {}:{} | {} sats (${:.2})", 
                      user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount, stable_value_usd);
                
                chain.log_change(
                    "withdrawal_change",
                    (chain_utxo.amount as f64 / 100_000_000.0),
                    stable_value_usd,
                    "deposit-service",
                    Some(format!("Change from withdrawal {}", withdrawal_id))
                );
                
                // Add to change UTXOs
                updated_change_utxos.push(chain_utxo);
                
            } else {
                // This is a regular external deposit (or service deposit)
                let (stable_value_usd, source) = if confirmations >= required_confirmations {
                    let (value, src, _market_basis) = process_new_confirmation(
                        user_id, &mut chain_utxo, chain, price_feed.clone()
                    ).await?;
                    (value, src)
                } else {
                    (0.0, "pending".to_string())
                };
                
                let stabilized_utxo = UtxoInfo {
                    stable_value_usd,
                    ..utxo_info.clone()
                };
                
                new_utxos.push(stabilized_utxo.clone());
                chain.history.push(stabilized_utxo);
                
                // Update origin-based accounting if confirmed
                if confirmations >= required_confirmations {
                    let origin_key = format!("{:?}", origin);
                    chain.stable_value_by_origin
                        .entry(origin_key)
                        .and_modify(|e| e.0 += stable_value_usd)
                        .or_insert(USD(stable_value_usd));
                }
                
                info!("Found {} UTXO for user {}: {}:{} | {} sats (${:.2} {})", 
                      if confirmations >= required_confirmations { 
                          if origin == UtxoOrigin::ServiceDeposit { "service" } else { "stabilized" }
                      } else { 
                          "unconfirmed" 
                      }, 
                      user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount, stable_value_usd,
                      if confirmations >= required_confirmations { source } else { "pending".to_string() });
                
                updated_regular_utxos.push(chain_utxo);
            }
        }
    }

    // Handle any detected reorgs - add entries to change log and notify via events
    for (utxo_key, old_height, new_height) in &reorgs_detected {
        let height_delta = (*new_height as i64) - (*old_height as i64);
        let (txid, vout) = utxo_key;
        
        chain.log_change(
            "blockchain_reorg",
            0.0, // No BTC change
            0.0, // No USD change
            "deposit-service",
            Some(format!("Reorg detected for UTXO {}:{}, block height changed from {} to {} (delta: {})", 
                  txid, vout, old_height, new_height, height_delta))
        );
        
        chain.events.push(Event {
            timestamp: utils::now_timestamp(),
            source: "deposit-service".to_string(),
            kind: "reorg".to_string(),
            details: format!("{}-{}-{}-{}", txid, vout, old_height, new_height),
        });
        
        // For significant reorgs, log additional warning
        if height_delta.abs() > 3 {
            warn!("Significant blockchain reorganization detected for user {}: tx {}:{} moved by {} blocks",
                  user_id, txid, vout, height_delta.abs());
            
            // Consider additional measures for significant reorgs
            if height_delta < -6 {
                // Major reorg that might invalidate a previously confirmed transaction
                error!("CRITICAL: Major blockchain reorganization affecting confirmed transactions for user {}",
                       user_id);
                
                // Add a service alert
                chain.log_change(
                    "critical_reorg",
                    0.0,
                    0.0,
                    "deposit-service",
                    Some(format!("Critical reorg of {} blocks affecting UTXO {}:{}", 
                          height_delta.abs(), txid, vout))
                );
            }
        }
    }

    // Update StableChain with the new UTXOs
    chain.regular_utxos = updated_regular_utxos;
    chain.change_utxos = updated_change_utxos;
    
    // Update the main utxos field for backward compatibility
    chain.utxos = [chain.regular_utxos.clone(), chain.change_utxos.clone()].concat();
    
    // Update balance calculations
    chain.accumulated_btc = Bitcoin::from_sats(wallet.balance().total().to_sat());
    
    // Use our enhanced calculation that excludes unexpected deposits
    chain.stabilized_usd = chain.usable_stable_value();
    
    // Keep current price
    chain.raw_btc_usd = stabilization_price;

    // Handle any unexpected deposits that were ignored
    if !ignored_utxos.is_empty() {
        warn!("Ignored {} deposits to change addresses for user {}", ignored_utxos.len(), user_id);
        
        // Add a record of this unusual activity
        chain.log_change(
            "ignored_deposits",
            ignored_utxos.iter().map(|u| u.amount as f64 / 100_000_000.0).sum(),
            0.0, // No USD value associated
            "deposit-service",
            Some(format!("Ignored {} unexpected deposits to change addresses", ignored_utxos.len()))
        );
    }

    // Handle new deposit notifications and address rotation
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
        
        // Generate new deposit address and rotate old ones
        let new_addr = wallet.reveal_next_address(KeychainKind::External).address;
        if !old_addresses_set.contains(&chain.multisig_addr) {
            old_addresses_set.insert(chain.multisig_addr.clone());
            info!("Added old address for user {}: {}", user_id, chain.multisig_addr);
        }
        chain.multisig_addr = new_addr.to_string();
        chain.old_addresses = old_addresses_set.into_iter().collect();
        info!("Updated deposit address for user {}: {}", user_id, chain.multisig_addr);
        
        // Send webhook notification about new deposit if configured
        if price_info.raw_btc_usd > 0.0 && chain.hedge_ready {
            // This could trigger hedge service webhook notification
            debug!("User {} has new deposits worth ${:.2} - could trigger hedge", 
                  user_id, usd_delta);
        }
    }

    // Save the updated state
    state_manager.save_stable_chain(user_id, chain).await?;
    if let Some(changeset) = wallet.take_staged() {
        state_manager.save_changeset(user_id, &changeset).await?;
    }

    // Log completion stats
    debug!("Completed sync for user {} in {}ms: {} new UTXOs", 
          user_id, start_time.elapsed().as_millis(), new_utxos.len());
    
    // Return result with reorg information
    if !reorgs_detected.is_empty() {
        debug!("Blockchain reorganizations detected during sync: {}", reorgs_detected.len());
    }
    
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
    
let mempool_timestamps: HashMap<(String, u32), u64> = HashMap::new(); // Empty for now, or track this properly

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
    let mempool_timestamps = HashMap::new(); // Empty for now, or track this properly
let config = &Config {
min_confirmations: min_confirmations, // Use the parameter directly
    service_min_confirmations: 0, // Fast confirmation for service addresses 
    external_min_confirmations: 3, // Higher security for external deposits
};
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
    &mempool_timestamps,
    config, // Pass the config parameter
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
