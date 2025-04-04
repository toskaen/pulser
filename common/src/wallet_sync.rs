// common/src/wallet_sync.rs
use std::time::{Duration, Instant};
use bdk_wallet::{Wallet, KeychainKind};
use bdk_esplora::EsploraAsyncExt;
use bdk_esplora::esplora_client::AsyncClient;
use crate::types::{StableChain, UtxoInfo, USD, Bitcoin, Utxo, PriceInfo};
use crate::error::PulserError;
use crate::StateManager;
use crate::price_feed::{PriceFeed, PriceFeedExtensions}; // Extension trait for price feed
use log::{info, warn, debug, error, trace};
use bitcoin::{Address, Network};
use tokio::time::timeout;
use std::str::FromStr;
use std::sync::Arc;
use bdk_chain::spk_client::SyncRequest;

const MAX_SCAN_RETRIES: u32 = 3;
const SCAN_TIMEOUT_SECS: u64 = 60;

/// Analyzes market conditions to determine the optimal stabilization price.
/// This function implements Pulser's market-aware policy for USD stabilization.
async fn determine_stabilization_price(
    price_feed: &Arc<PriceFeed>,
    user_id: &str,
    is_new_confirmation: bool
) -> Result<(f64, String, f64), PulserError> {
    // Get spot VWAP price (volume-weighted average from multiple exchanges)
    let vwap_info = price_feed.get_price().await?;
    let spot_price = vwap_info.raw_btc_usd;
    
    // Get futures price
    let futures_price = match price_feed.get_deribit_price().await {
        Ok(price) => price,
        Err(e) => {
            warn!("Failed to get Deribit futures price for user {}: {}, defaulting to spot VWAP", user_id, e);
            return Ok((spot_price, "VWAP (fallback)".to_string(), 0.0));
        }
    };
    
    // Calculate basis (percentage difference between futures and spot)
    // Negative basis means market is in backwardation (futures < spot)
    let basis = ((futures_price / spot_price) - 1.0) * 100.0;
    
    // For newly confirmed deposits, use strategic pricing
    if is_new_confirmation {
        if basis < -0.25 {
            // Market is in backwardation - use spot price for stabilization (higher value)
            info!("BACKWARDATION detected for user {}: {:.2}% - using VWAP (${:.2}) > futures (${:.2})", 
                  user_id, basis, spot_price, futures_price);
            
            return Ok((spot_price, format!("VWAP (backwardation {:.2}%)", basis), basis));
        } else if basis > 0.25 {
            // Market is in contango - use futures price for stabilization (matches hedge better)
            info!("CONTANGO detected for user {}: {:.2}% - using futures (${:.2}) > VWAP (${:.2})", 
                  user_id, basis, futures_price, spot_price);
                  
            return Ok((futures_price, format!("Deribit (contango {:.2}%)", basis), basis));
        } else {
            // Markets are close - use VWAP for accuracy
            debug!("Neutral market for user {}: {:.2}% - using VWAP (${:.2})", user_id, basis, spot_price);
            return Ok((spot_price, format!("VWAP (neutral {:.2}%)", basis), basis));
        }
    } else {
        // For general price checks, use VWAP by default
        trace!("Using VWAP (${:.2}) for user {} price check", spot_price, user_id);
        return Ok((spot_price, "VWAP".to_string(), basis));
    }
}

/// Syncs wallet with blockchain and stabilizes any new UTXOs.
/// This is the core function for Pulser's USD stabilization feature.
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

    // Get the optimal stabilization price from our market-aware algorithm
    let (stabilization_price, price_source, basis) = determine_stabilization_price(
        &price_feed, 
        user_id,
        false // Not for new confirmations yet, just initialization
    ).await?;
    
    if stabilization_price <= 0.0 {
        return Err(PulserError::PriceFeedError("No valid price available after analysis".to_string()));
    }

    let mut previous_utxos = chain.utxos.clone();
    let previous_balance = wallet.balance();

    // Sync with revealed script pubkeys
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
            height: None,
            usd_value: None,
            spent: utxo_info.spent,
        };

        // Check if this is an existing UTXO
        if let Some(existing) = previous_utxos.iter_mut().find(|prev| prev.txid == chain_utxo.txid && prev.vout == chain_utxo.vout) {
            chain_utxo.usd_value = existing.usd_value.clone();
            
            // Check if it needs stabilization (newly confirmed but not yet stabilized)
            if !is_change && confirmations >= min_confirmations && chain_utxo.usd_value.is_none() {
                // For newly confirmed UTXOs, determine optimal price based on market conditions
                let (stab_price, source, market_basis) = determine_stabilization_price(
                    &price_feed, 
                    user_id,
                    true // This is a new confirmation
                ).await?;
                
                let stable_value_usd = (chain_utxo.amount as f64 / 100_000_000.0) * stab_price;
                chain_utxo.usd_value = Some(USD(stable_value_usd));
                let stabilized_utxo = UtxoInfo { stable_value_usd, ..utxo_info.clone() };
                new_utxos.push(stabilized_utxo.clone());
                chain.history.push(stabilized_utxo);
                
                // Log stabilization details with market information
                info!("Stabilized existing UTXO for user {}: {}:{} | {} sats (${:.2} using {})", 
                      user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount, stable_value_usd, source);
                
                let futures_price = match price_feed.get_deribit_price().await {
                    Ok(price) => price,
                    Err(_) => stab_price, // Fall back to the stabilization price
                };
                
                // Record market conditions for analytics
                if market_basis < -0.25 {
                    // Calculate the benefit gained from backwardation
                    let futures_value = (chain_utxo.amount as f64 / 100_000_000.0) * futures_price;
                    let benefit = stable_value_usd - futures_value;
                    info!("Backwardation benefit for user {}: ${:.2} ({:.2}% of ${:.2})", 
                          user_id, benefit, -market_basis, stable_value_usd);
                          
                    // Add to change log
                    chain.log_change(
                        "backwardation",
                        0.0,
                        benefit,
                        "price-feed",
                        Some(format!("Backwardation benefit {:.2}%", -market_basis))
                    );
                }
            }
            updated_utxos.push(chain_utxo);
        } else if !is_change {
            // New external UTXO (received from someone)
            
            // For newly received UTXOs that have enough confirmations, stabilize them
            let (stable_value_usd, stab_price_source) = if confirmations >= min_confirmations {
                // Determine optimal price based on market conditions
                let (stab_price, source, market_basis) = determine_stabilization_price(
                    &price_feed, 
                    user_id,
                    true // This is a new confirmation
                ).await?;
                
                let value = (chain_utxo.amount as f64 / 100_000_000.0) * stab_price;
                
                let futures_price = match price_feed.get_deribit_price().await {
                    Ok(price) => price,
                    Err(_) => stab_price, // Fall back to the stabilization price
                };
                
                // Record market conditions for analytics
                if market_basis < -0.25 {
                    // Calculate the benefit gained from backwardation
                    let futures_value = (chain_utxo.amount as f64 / 100_000_000.0) * futures_price;
                    let benefit = value - futures_value;
                    info!("Backwardation benefit for user {}: ${:.2} ({:.2}% of ${:.2})", 
                          user_id, benefit, -market_basis, value);
                          
                    // Add to change log
                    chain.log_change(
                        "backwardation",
                        0.0,
                        benefit,
                        "price-feed",
                        Some(format!("Backwardation benefit {:.2}%", -market_basis))
                    );
                }
                
                (value, source)
            } else {
                // Not enough confirmations yet
                (0.0, "pending".to_string())
            };
            
            chain_utxo.usd_value = if confirmations >= min_confirmations { 
                Some(USD(stable_value_usd)) 
            } else { 
                None 
            };
            
            let stabilized_utxo = UtxoInfo { stable_value_usd, ..utxo_info.clone() };
            new_utxos.push(stabilized_utxo.clone());
            chain.history.push(stabilized_utxo);
            
            info!("Found {} external UTXO for user {}: {}:{} | {} sats (${:.2} {})", 
                  if confirmations >= min_confirmations { "stabilized" } else { "unconfirmed" }, 
                  user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount, stable_value_usd,
                  if confirmations >= min_confirmations { format!("using {}", stab_price_source) } else { "pending".to_string() });
                  
            updated_utxos.push(chain_utxo);
        } else {
            // Track change outputs but don't stabilize them
            info!("Tracked change UTXO for user {}: {}:{} | {} sats (unstabilized)", 
                  user_id, chain_utxo.txid, chain_utxo.vout, chain_utxo.amount);
            updated_utxos.push(chain_utxo);
        }
    }

    // Update chain with new information
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
        
        // Add entry to change log
        chain.log_change(
            "deposit", 
            btc_delta, 
            usd_delta, 
            "deposit-service", 
            Some(format!("{} UTXOs confirmed using {}", new_utxos.len(), price_source))
        );
        
        // Generate a new address for future deposits
        let new_addr = wallet.reveal_next_address(KeychainKind::External).address;
        chain.old_addresses.push(chain.multisig_addr.clone());
        chain.multisig_addr = new_addr.to_string();
        info!("Updated deposit address for user {}: {}", user_id, chain.multisig_addr);
    }

    // Save the updated state
    state_manager.save_stable_chain(user_id, chain).await?;
    if let Some(changeset) = wallet.take_staged() {
        state_manager.save_changeset(user_id, &changeset).await?;
    }

    debug!("Completed sync for user {} in {}ms: {} new UTXOs", user_id, start_time.elapsed().as_millis(), new_utxos.len());
    Ok(new_utxos)
}

/// Performs a full history resync for a wallet
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
    
    // Get all used addresses
    let mut addresses = vec![Address::from_str(&chain.multisig_addr)?.assume_checked()];
    for old_addr in &chain.old_addresses {
        if let Ok(addr) = Address::from_str(old_addr) {
            addresses.push(addr.assume_checked());
        }
    }
    
    // Collect all script pubkeys to watch
    let mut spks = vec![];
    for addr in &addresses {
        spks.push(addr.script_pubkey());
    }
    
    // Also include all potential derivation paths
    for i in 0..=wallet.derivation_index(KeychainKind::External).unwrap_or(10) {
        spks.push(wallet.peek_address(KeychainKind::External, i).address.script_pubkey());
    }
    for i in 0..=wallet.derivation_index(KeychainKind::Internal).unwrap_or(10) {
        spks.push(wallet.peek_address(KeychainKind::Internal, i).address.script_pubkey());
    }
    
    // Create and execute sync request
    let request = SyncRequest::builder().spks(spks.into_iter()).build();
    let update = esplora.sync(request, 5).await?;
    wallet.apply_update(update)?;
    
    let change_addr = wallet.reveal_next_address(KeychainKind::Internal).address;
    let deposit_addr = Address::from_str(&chain.multisig_addr)?.assume_checked();
    
    // Now stabilize any found UTXOs
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
    
    debug!("Full resync completed for user {}: {} new UTXOs", user_id, new_utxos.len());
    
    Ok(new_utxos)
}

// Helper function to check if an address has any transactions in mempool
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

// Helper function to validate a wallet is properly initialized
pub async fn validate_wallet_state(
    wallet: &Wallet,
    chain: &StableChain
) -> Result<bool, PulserError> {
    // Check if wallet has derivation indices
    let external_index = wallet.derivation_index(KeychainKind::External).unwrap_or(0);
    let internal_index = wallet.derivation_index(KeychainKind::Internal).unwrap_or(0);
    
    // Check if wallet has a valid tip height
    let tip_height = wallet.latest_checkpoint().height();
    
    // Check if chain has a valid multisig address
    let has_valid_address = Address::from_str(&chain.multisig_addr).is_ok();
    
    // Consider wallet valid if we have basic requirements met
    let is_valid = external_index > 0 || internal_index > 0 || tip_height > 0 || has_valid_address;
    
    Ok(is_valid)
}
