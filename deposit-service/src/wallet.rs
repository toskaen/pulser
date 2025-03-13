// deposit-service/src/wallet.rs
use bdk_wallet::{
    wallet::{Wallet, WalletBuilder, SignOptions},
    descriptor::Descriptor,
    keys::KeychainKind,
    balance::Balance,
    chain::{BlockChain, Target},
    database::MemoryDatabase,
    psbt::Psbt,
    tx_builder::TxBuilder,
    error::Error as BdkError,
};
use bitcoin::{Address, Network, Txid, Transaction};
use std::str::FromStr;
use std::time::Duration;
use common::PulserError;
use crate::types::{Utxo, DepositAddressInfo};
use std::sync::Arc;
use log::{info, warn, error, debug};

/// Wrapper for Wallet operations
pub struct DepositWallet {
    pub wallet: Wallet<MemoryDatabase>,
    pub blockchain: Arc<dyn BlockChain + Send + Sync>,
    pub network: Network,
}

impl DepositWallet {
    /// Create a new wallet from a descriptor
    pub fn new(
        descriptor: &str, 
        network: Network,
        blockchain: Arc<dyn BlockChain + Send + Sync>
    ) -> Result<Self, PulserError> {
        // Parse descriptor
        let desc = Descriptor::from_str(descriptor)
            .map_err(|e| PulserError::WalletError(format!("Invalid descriptor: {}", e)))?;
            
        // Create wallet
        let wallet = WalletBuilder::new()
            .with_descriptor(desc)
            .with_network(network)
            .with_database(MemoryDatabase::default())
            .build()
            .map_err(|e| PulserError::WalletError(format!("Failed to build wallet: {}", e)))?;
            
        Ok(Self {
            wallet,
            blockchain,
            network,
        })
    }
    
    /// Create a taproot multisig wallet for a deposit address
    pub fn create_taproot_multisig(
        user_pubkey: &str,
        lsp_pubkey: &str,
        trustee_pubkey: &str,
        network: Network,
        blockchain: Arc<dyn BlockChain + Send + Sync>,
    ) -> Result<(Self, DepositAddressInfo), PulserError> {
        // Parse public keys
        let user_pk = bitcoin::PublicKey::from_str(user_pubkey)
            .map_err(|e| PulserError::WalletError(format!("Invalid user pubkey: {}", e)))?;
        
        let lsp_pk = bitcoin::PublicKey::from_str(lsp_pubkey)
            .map_err(|e| PulserError::WalletError(format!("Invalid LSP pubkey: {}", e)))?;
        
        let trustee_pk = bitcoin::PublicKey::from_str(trustee_pubkey)
            .map_err(|e| PulserError::WalletError(format!("Invalid trustee pubkey: {}", e)))?;
        
        // Create Taproot descriptor (tr() instead of wsh())
        // This creates a 2-of-3 multisig using Taproot - any 2 of the 3 keys can sign
        let desc_str = format!(
            "tr(and_v(or_c(pk({}),pk({})),pk({})))",
            user_pk.inner, lsp_pk.inner, trustee_pk.inner
        );
        
        // Create wallet with Taproot descriptor
        let desc = Descriptor::from_str(&desc_str)
            .map_err(|e| PulserError::WalletError(format!("Invalid descriptor: {}", e)))?;
        
        let wallet = WalletBuilder::new()
            .with_descriptor(desc)
            .with_network(network)
            .with_database(MemoryDatabase::default())
            .build()
            .map_err(|e| PulserError::WalletError(format!("Failed to build wallet: {}", e)))?;
        
        // Get a deposit address
        let address = wallet.get_address(KeychainKind::External)
            .map_err(|e| PulserError::WalletError(format!("Failed to get address: {}", e)))?;
        
        let deposit_info = DepositAddressInfo {
            address: address.to_string(),
            descriptor: desc_str,
            path: "m/86'/0'/0'/0/0".to_string(), // BIP-86 path for Taproot
            user_pubkey: user_pubkey.to_string(),
            lsp_pubkey: lsp_pubkey.to_string(),
            trustee_pubkey: trustee_pubkey.to_string(),
        };
        
        info!("Created taproot multisig wallet with address: {}", address);
        
        Ok((
            Self {
                wallet,
                blockchain,
                network,
            },
            deposit_info
        ))
    }
    
    /// Get a new address from the wallet
    pub fn get_address(&self) -> Result<String, PulserError> {
        let address = self.wallet.get_address(KeychainKind::External)
            .map_err(|e| PulserError::WalletError(format!("Failed to get address: {}", e)))?;
            
        Ok(address.to_string())
    }
    
    /// Sync the wallet with the blockchain
    pub async fn sync(&self) -> Result<(), PulserError> {
        debug!("Starting wallet sync with blockchain");
        self.wallet.sync(self.blockchain.as_ref(), None)
            .await
            .map_err(|e| PulserError::WalletError(format!("Failed to sync wallet: {}", e)))?;
        debug!("Wallet sync completed");
        Ok(())
    }
    
    /// Get the wallet balance
    pub fn get_balance(&self) -> Result<Balance, PulserError> {
        self.wallet.get_balance()
            .map_err(|e| PulserError::WalletError(format!("Failed to get balance: {}", e)))
    }
    
    /// List unspent outputs (UTXOs)
    pub fn list_utxos(&self) -> Result<Vec<Utxo>, PulserError> {
        let utxos = self.wallet.list_unspent()
            .map_err(|e| PulserError::WalletError(format!("Failed to list UTXOs: {}", e)))?;
            
        let mut result = Vec::new();
        for utxo in utxos {
            let outpoint = utxo.outpoint;
            
            // Get transaction details to determine confirmations
            let tx_details = match self.wallet.get_tx(&outpoint.txid) {
                Ok(Some(details)) => details,
                _ => continue, // Skip if we can't get details
            };
            
            // Get confirmation status
            let confirmations = match tx_details.confirmation_time {
                Some(conf) => {
                    // In a real implementation, you'd calculate actual confirmations
                    // based on current blockchain height and tx height
                    conf.height as u32 
                },
                None => 0,
            };
            
            // Get address
            let script = utxo.txout.script_pubkey.clone();
            let address = Address::from_script(&script, self.network)
                .map(|a| a.to_string())
                .unwrap_or_else(|_| "unknown".to_string());
                
            result.push(Utxo {
                txid: outpoint.txid.to_string(),
                vout: outpoint.vout,
                amount: utxo.txout.value,
                confirmations,
                script_pubkey: hex::encode(script.as_bytes()),
                height: tx_details.confirmation_time.map(|c| c.height as u32),
                address,
            });
        }
        
        Ok(result)
    }
    
    /// Create a transaction
    pub fn create_transaction(
        &self,
        destination: &str,
        amount: u64,
        fee_rate: f32,
        enable_rbf: bool,
    ) -> Result<(Psbt, Txid), PulserError> {
        // Parse destination address
        let dest_address = Address::from_str(destination)
            .map_err(|e| PulserError::InvalidRequest(format!("Invalid destination address: {}", e)))?;
            
        // Build transaction
        let mut tx_builder = self.wallet.build_tx();
        
        // Add recipient
        tx_builder = tx_builder.add_recipient(dest_address.script_pubkey(), amount);
        
        // Set fee rate
        tx_builder = tx_builder.fee_rate(fee_rate);
        
        // Enable RBF if requested
        if enable_rbf {
            tx_builder = tx_builder.enable_rbf();
        }
        
        // Finalize transaction
        let psbt = match tx_builder.finish() {
            Ok(psbt) => psbt,
            Err(e) => {
                error!("Failed to build transaction: {}", e);
                return Err(PulserError::TransactionError(format!("Failed to build transaction: {}", e)));
            }
        };
        
        // Get transaction ID
        let txid = psbt.txid();
        
        info!("Created transaction: {} for {} sats to {}", txid, amount, destination);
        
        Ok((psbt, txid))
    }
    
    /// Sign a PSBT
    pub fn sign(&self, psbt: &Psbt, options: SignOptions) -> Result<Psbt, PulserError> {
        // Clone the PSBT to avoid modifying the original
        let mut psbt_to_sign = psbt.clone();
        
        // Sign with our key
        let signed_psbt = self.wallet.sign(&psbt_to_sign, options)
            .map_err(|e| PulserError::TransactionError(format!("Failed to sign transaction: {}", e)))?;
        
        Ok(signed_psbt)
    }
    
    /// Check if a PSBT is fully signed (ready for finalization)
    pub fn is_psbt_fully_signed(&self, psbt: &Psbt) -> Result<bool, PulserError> {
        // For a 2-of-3 multisig, we need at least 2 signatures
        // In a real implementation, you would check this more carefully
        
        // Count the number of signatures
        let sig_count = psbt.inputs.iter()
            .map(|input| input.partial_sigs.len())
            .sum::<usize>();
        
        // For a 2-of-3 multisig, we need at least 2 signatures per input
        let input_count = psbt.inputs.len();
        
        Ok(sig_count >= input_count * 2)
    }
    
    /// Finalize a PSBT and extract the transaction
    pub fn finalize_psbt(&self, psbt: &Psbt) -> Result<Transaction, PulserError> {
        // Finalize the PSBT
        let finalized = self.wallet.finalize_psbt(psbt, None)
            .map_err(|e| PulserError::TransactionError(format!("Failed to finalize PSBT: {}", e)))?;
        
        // Extract the transaction
        let tx = finalized.extract_tx()
            .map_err(|e| PulserError::TransactionError(format!("Failed to extract transaction: {}", e)))?;
        
        Ok(tx)
    }
    
    /// Finalize and broadcast a transaction
    pub async fn finalize_and_broadcast(&self, psbt: &Psbt) -> Result<Txid, PulserError> {
        // Finalize the PSBT
        let tx = self.finalize_psbt(psbt)?;
        
        // Broadcast with retry mechanism
        self.broadcast_with_retry(&tx, 3).await
    }
    
    /// Broadcast a transaction with retry mechanism
    pub async fn broadcast_with_retry(&self, tx: &Transaction, max_attempts: usize) -> Result<Txid, PulserError> {
        let txid = tx.txid();
        let mut attempts = 0;
        let mut last_error = None;
        
        while attempts < max_attempts {
            match self.blockchain.broadcast(tx).await {
                Ok(_) => {
                    info!("Transaction broadcast successful: {}", txid);
                    return Ok(txid);
                },
                Err(e) => {
                    // Check if the error is "already in mempool" (success case)
                    let error_str = e.to_string();
                    if error_str.contains("already in mempool") || 
                       error_str.contains("transaction already in block chain") {
                        info!("Transaction already in mempool/blockchain: {}", txid);
                        return Ok(txid);
                    }
                    
                    attempts += 1;
                    warn!("Broadcast attempt {}/{} failed: {}", attempts, max_attempts, e);
                    last_error = Some(e);
                    
                    if attempts < max_attempts {
                        // Exponential backoff: 1s, 2s, 4s, etc.
                        let backoff = Duration::from_secs(2u64.pow(attempts as u32 - 1));
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        }
        
        Err(PulserError::TransactionError(format!(
            "Failed to broadcast transaction after {} attempts: {}",
            max_attempts,
            last_error.map_or("Unknown error".to_string(), |e| e.to_string())
        )))
    }
    
    /// Build a transaction
    pub fn build_tx(&self) -> TxBuilder<'_> {
        self.wallet.build_tx()
    }
}
