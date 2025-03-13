// deposit-service/src/wallet.rs
use bdk_wallet::{
    wallet::{Wallet, WalletBuilder},
    descriptor::{Descriptor, DescriptorBuilder},
    keys::{KeychainKind, PublicKey, DerivationPath},
    balance::Balance,
    chain::{BlockChain, NoopBlockchain},
    database::{MemoryDatabase, Database},
    psbt::{PsbtBuilder, Psbt},
    tx_builder::{TxBuilder, TxBuilderResult},
    error::Error as BdkError,
};
use bitcoin::{Address, Network, Txid, ScriptBuf};
use std::str::FromStr;
use common::PulserError;
use crate::types::{StableChain, Bitcoin, Utxo, DepositAddressInfo};
use std::sync::Arc;

/// Wrapper for Wallet operations
pub struct DepositWallet {
    wallet: Wallet<MemoryDatabase, NoopBlockchain>,
    blockchain: Arc<dyn BlockChain + Send + Sync>,
    network: Network,
}

impl DepositWallet {
    /// Create a new wallet from a descriptor
    pub fn new(
        descriptor: &str, 
        network: Network,
        blockchain: Arc<dyn BlockChain + Send + Sync>
    ) -> Result<Self, PulserError> {
        let desc = Descriptor::from_str(descriptor)
            .map_err(|e| PulserError::WalletError(format!("Invalid descriptor: {}", e)))?;
            
        let mut wallet_builder = WalletBuilder::default();
        wallet_builder
            .with_descriptor(desc)
            .with_network(network);
            
        let wallet = wallet_builder.build()
            .map_err(|e| PulserError::WalletError(format!("Failed to build wallet: {}", e)))?;
            
        Ok(Self {
            wallet,
            blockchain,
            network,
        })
    }
    
    /// Create a multisig wallet for a deposit address
    pub fn create_multisig(
        user_pubkey: &str,
        lsp_pubkey: &str,
        trustee_pubkey: &str,
        network: Network,
        blockchain: Arc<dyn BlockChain + Send + Sync>,
    ) -> Result<(Self, DepositAddressInfo), PulserError> {
        // Create multisig descriptor (2-of-3)
        let descriptor = format!(
            "wsh(multi(2,{},{},{}))",
            user_pubkey, lsp_pubkey, trustee_pubkey
        );
        
        let wallet = Self::new(&descriptor, network, blockchain)?;
        
        // Get a deposit address
        let address = wallet.get_address()?;
        
        let deposit_info = DepositAddressInfo {
            address: address.to_string(),
            descriptor: descriptor.clone(),
            path: "m/84'/0'/0'/0/0".to_string(), // Default path
            user_pubkey: user_pubkey.to_string(),
            lsp_pubkey: lsp_pubkey.to_string(),
            trustee_pubkey: trustee_pubkey.to_string(),
        };
        
        Ok((wallet, deposit_info))
    }
    
    /// Get a new address from the wallet
    pub fn get_address(&self) -> Result<Address, PulserError> {
        self.wallet.get_address(KeychainKind::External)
            .map_err(|e| PulserError::WalletError(format!("Failed to get address: {}", e)))
    }
    
    /// Sync the wallet with the blockchain
    pub async fn sync(&self) -> Result<(), PulserError> {
        self.wallet.sync(self.blockchain.as_ref(), None)
            .await
            .map_err(|e| PulserError::WalletError(format!("Failed to sync wallet: {}", e)))
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
    
    /// Create and sign a transaction
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
        tx_builder
            .add_recipient(dest_address.script_pubkey(), amount);
            
        if enable_rbf {
            tx_builder.enable_rbf();
        }
        
        // Set fee rate
        tx_builder.fee_rate(fee_rate);
        
        // Finalize transaction
        let TxBuilderResult { psbt, .. } = tx_builder.finish()
            .map_err(|e| PulserError::TransactionError(format!("Failed to build transaction: {}", e)))?;
            
        // Sign with our keys
        let signed_psbt = self.wallet.sign(psbt, None)
            .map_err(|e| PulserError::TransactionError(format!("Failed to sign transaction: {}", e)))?;
            
        // Extract transaction for txid
        let txid = signed_psbt.unsigned_tx.txid();
        
        Ok((signed_psbt, txid))
    }
    
    /// Finalize a PSBT and extract the transaction
    pub fn finalize_psbt(&self, psbt: Psbt) -> Result<Txid, PulserError> {
        let tx = psbt.extract_tx()
            .map_err(|e| PulserError::TransactionError(format!("Failed to extract transaction: {}", e)))?;
            
        Ok(tx.txid())
    }
    
    /// Broadcast a transaction
    pub async fn broadcast(&self, psbt: Psbt) -> Result<Txid, PulserError> {
        let tx = psbt.extract_tx()
            .map_err(|e| PulserError::TransactionError(format!("Failed to extract transaction: {}", e)))?;
            
        self.blockchain.broadcast(&tx)
            .await
            .map_err(|e| PulserError::TransactionError(format!("Failed to broadcast transaction: {}", e)))?;
            
        Ok(tx.txid())
    }
}
