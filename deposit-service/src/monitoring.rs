// In deposit-service/src/monitoring.rs
use crate::wallet::DepositWallet;
use crate::types::{StableChain, Bitcoin, Utxo, Event};
use crate::blockchain::create_esplora_client;
use common::error::PulserError;
use common::types::USD;
use log::{info, warn, error, debug};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use std::path::Path;

/// Structure to hold all monitoring state
pub struct MonitoringService {
    pub wallets: Arc<RwLock<HashMap<u32, (DepositWallet, StableChain)>>>,
    pub config: Arc<RwLock<crate::config::Config>>,
    pub current_price: Arc<RwLock<f64>>,
    pub synthetic_price: Arc<RwLock<f64>>,
    pub should_stop: Arc<RwLock<bool>>,
}

impl MonitoringService {
    pub fn new(
        wallets: Arc<RwLock<HashMap<u32, (DepositWallet, StableChain)>>>,
        config: Arc<RwLock<crate::config::Config>>,
        current_price: Arc<RwLock<f64>>,
        synthetic_price: Arc<RwLock<f64>>,
    ) -> Self {
        Self {
            wallets,
            config,
            current_price,
            synthetic_price,
            should_stop: Arc::new(RwLock::new(false)),
        }
    }
    
    /// Start the monitoring service
    pub async fn start(&self) {
        let wallets = self.wallets.clone();
        let config = self.config.clone();
        let current_price = self.current_price.clone();
        let synthetic_price = self.synthetic_price.clone();
        let should_stop = self.should_stop.clone();
        
        // Start the sync loop
        tokio::spawn(async move {
            info!("Starting wallet sync loop");
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // Every minute
            
            loop {
                interval.tick().await;
                
                // Check if we should stop
                if *should_stop.read().unwrap() {
                    info!("Stopping wallet sync loop");
                    break;
                }
                
                // Sync all wallets
                if let Err(e) = Self::sync_all_wallets(
                    &wallets, 
                    &config, 
                    *current_price.read().unwrap(), 
                    *synthetic_price.read().unwrap()
                ).await {
                    error!("Error syncing wallets: {}", e);
                }
            }
        });
        
        info!("Monitoring service started");
    }
    
    /// Stop the monitoring service
    pub fn stop(&self) {
        *self.should_stop.write().unwrap() = true;
        info!("Monitoring service stopping...");
    }
    
    /// Sync all wallets and check for deposits
    async fn sync_all_wallets(
        wallets: &Arc<RwLock<HashMap<u32, (DepositWallet, StableChain)>>>,
        config: &Arc<RwLock<crate::config::Config>>,
        current_price: f64,
        synthetic_price: f64,
    ) -> Result<(), PulserError> {
        let mut wallets_to_write = wallets.write().map_err(|_| {
            PulserError::InternalError("Failed to lock wallets for writing".to_string())
        })?;
        
        let config = config.read().map_err(|_| {
            PulserError::InternalError("Failed to lock config for reading".to_string())
        })?;
        
        // Iterate over all wallets
        for (user_id, (wallet, chain)) in wallets_to_write.iter_mut() {
            debug!("Syncing wallet for user {}", user_id);
            
            // Fetch current UTXOs from blockchain
            let address = &chain.multisig_addr;
            let client = &wallet.blockchain;
            
            // Refresh UTXOs from blockchain
            match Self::refresh_utxos(client, address, chain, current_price, synthetic_price).await {
                Ok(_) => {
                    debug!("UTXOs refreshed for user {}", user_id);
                    
                    // Check for sufficient funds
                    if Self::check_sufficient_funds(chain, &config) {
                        info!("Sufficient funds detected for user {}", user_id);
                        
                        // Mark ready for sweep
                        if chain.pending_sweep_txid.is_none() {
                            chain.events.push(Event {
                                timestamp: common::utils::now_timestamp(),
                                source: "MonitoringService".to_string(),
                                kind: "SufficientFunds".to_string(),
                                details: format!(
                                    "Sufficient funds detected: {} sats (${:.2})",
                                    chain.accumulated_btc.sats,
                                    chain.stabilized_usd.0
                                ),
                            });
                            
                            // Save wallet state
                            Self::save_wallet_state(*user_id, chain, &config)?;
                            
                            // Trigger sweep - this would typically call an API endpoint
                            // but for now we'll just log that a sweep is needed
                            info!("User {} ready for sweep to single-sig wallet", user_id);
                            // TODO: Call sweep endpoint
                        }
                    }
                },
                Err(e) => {
                    warn!("Failed to refresh UTXOs for user {}: {}", user_id, e);
                }
            }
        }
        
        Ok(())
    }
    
    /// Refresh UTXOs from blockchain
    async fn refresh_utxos(
        client: &bdk_esplora::esplora_client::AsyncClient,
        address: &str,
        chain: &mut StableChain,
        current_price: f64,
        synthetic_price: f64,
    ) -> Result<(), PulserError> {
        use bitcoin::Address;
        use std::str::FromStr;
        
        // Parse address
        let addr = Address::from_str(address)
            .map_err(|e| PulserError::InvalidRequest(format!("Invalid address: {}", e)))?;
        
        // Get script pubkey
        let script = addr.assume_checked().script_pubkey();
        
        // Fetch UTXOs
        let utxos = client.get_scriptpubkey_utxos(&script)
            .await
            .map_err(|e| PulserError::ApiError(format!("Failed to fetch UTXOs: {}", e)))?;
        
        // Convert to our UTXO format
        let mut new_utxos = Vec::new();
        for utxo in utxos {
            // Get tx info to check confirmations
            let tx_info = client.get_tx(&utxo.outpoint.txid)
                .await
                .map_err(|e| PulserError::ApiError(format!("Failed to get tx info: {}", e)))?;
                
            let confirmations = if let Some(confirmation_time) = &tx_info.confirmation_time {
                let current_height = client.get_height()
                    .await
                    .map_err(|e| PulserError::ApiError(format!("Failed to get height: {}", e)))?;
                    
                if current_height > confirmation_time.height {
                    current_height - confirmation_time.height + 1
                } else {
                    0
                }
            } else {
                0
            };
            
            new_utxos.push(Utxo {
                txid: utxo.outpoint.txid.to_string(),
                vout: utxo.outpoint.vout,
                amount: utxo.txout.value,
                confirmations,
                script_pubkey: hex::encode(utxo.txout.script_pubkey.as_bytes()),
                height: tx_info.confirmation_time.as_ref().map(|c| c.height),
                address: address.to_string(),
            });
        }
        
        // Detect new UTXOs
        let old_utxo_count = chain.utxos.len();
        if new_utxos.len() > old_utxo_count {
            info!("New UTXOs detected! {} -> {}", old_utxo_count, new_utxos.len());
            
            // Log new deposits
            for utxo in &new_utxos {
                if !chain.utxos.iter().any(|u| u.txid == utxo.txid && u.vout == utxo.vout) {
                    chain.events.push(Event {
                        timestamp: common::utils::now_timestamp(),
                        source: "MonitoringService".to_string(),
                        kind: "DepositReceived".to_string(),
                        details: format!(
                            "New deposit detected: {} sats (txid: {}:{})",
                            utxo.amount, utxo.txid, utxo.vout
                        ),
                    });
                }
            }
        }
        
        // Update chain state
        chain.utxos = new_utxos;
        chain.accumulated_btc = Bitcoin::from_sats(
            chain.utxos.iter().map(|u| u.amount).sum()
        );
        chain.stabilized_usd = USD(
            chain.accumulated_btc.sats as f64 / 100_000_000.0 * current_price
        );
        chain.raw_btc_usd = current_price;
        chain.synthetic_price = Some(synthetic_price);
        
        Ok(())
    }
    
    /// Check if there are sufficient funds for a channel
    fn check_sufficient_funds(chain: &StableChain, config: &crate::config::Config) -> bool {
        // Must have confirmed UTXOs
        let confirmed_sats = chain.utxos.iter()
            .filter(|u| u.confirmations >= config.min_confirmations)
            .map(|u| u.amount)
            .sum::<u64>();
            
        // Check against threshold
        let confirmed_usd = confirmed_sats as f64 / 100_000_000.0 * chain.raw_btc_usd;
        
        confirmed_usd >= config.channel_threshold_usd
    }
    
    /// Save wallet state to disk
    fn save_wallet_state(
        user_id: u32, 
        chain: &StableChain,
        config: &crate::config::Config,
    ) -> Result<(), PulserError> {
        let wallet_dir = Path::new(&config.data_dir).join(&config.wallet_dir);
        let wallet_path = wallet_dir.join(format!("user_{}.json", user_id));
        
        // Create directories if they don't exist
        std::fs::create_dir_all(&wallet_dir)
            .map_err(|e| PulserError::StorageError(format!("Failed to create wallet directory: {}", e)))?;
        
        // Write state to file
        let wallet_data = serde_json::to_string_pretty(chain)
            .map_err(|e| PulserError::StorageError(format!("Failed to serialize wallet data: {}", e)))?;
        
        std::fs::write(&wallet_path, wallet_data)
            .map_err(|e| PulserError::StorageError(format!("Failed to save wallet data: {}", e)))?;
        
        Ok(())
    }
}
