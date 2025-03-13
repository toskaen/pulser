// deposit-service/src/blockchain.rs
use bdk_wallet::chain::{BlockChain, BlockChainConfig, Confirmation, TxStatus, Target};
use bdk_wallet::esplora::EsploraBlockchain;
use bitcoin::{Transaction, Txid, BlockHash};
use common::PulserError;
use std::sync::Arc;

/// Create an Esplora blockchain client
pub fn create_esplora_client(network: Network) -> Result<Arc<dyn BlockChain + Send + Sync>, PulserError> {
    let url = match network {
        Network::Bitcoin => "https://blockstream.info/api/",
        Network::Testnet => "https://blockstream.info/testnet/api/",
        Network::Signet => "https://mempool.space/signet/api/",
        Network::Regtest => "http://localhost:3002/",
    };
    
    let config = BlockChainConfig::Esplora {
        base_url: url.to_string(),
        concurrency: Some(4),
        timeout: Some(Duration::from_secs(30)),
        proxy: None,
    };
    
    let blockchain = EsploraBlockchain::from_config(&config)
        .map_err(|e| PulserError::ConfigError(format!("Failed to create blockchain client: {}", e)))?;
        
    Ok(Arc::new(blockchain))
}

/// Fetch UTXOs for an address
pub async fn fetch_address_utxos(
    blockchain: &dyn BlockChain,
    address: &str,
) -> Result<Vec<(String, u64, u32)>, PulserError> {
    // Convert the address to bitcoin::Address
    let addr = bitcoin::Address::from_str(address)
        .map_err(|e| PulserError::InvalidRequest(format!("Invalid address: {}", e)))?;
        
    // Get the script pubkey for the address
    let script = addr.script_pubkey();
    
    // Get UTXOs for the script
    let utxos = blockchain.list_unspent(&[&script])
        .await
        .map_err(|e| PulserError::ApiError(format!("Failed to fetch UTXOs: {}", e)))?;
        
    // Format the output
    let result = utxos.into_iter()
        .map(|utxo| {
            // Get the number of confirmations (or 0 if unconfirmed)
            let confirmations = match blockchain.get_tx_status(&utxo.outpoint.txid).await {
                Ok(TxStatus { confirmation, .. }) => match confirmation {
                    Confirmation::Confirmed { height, .. } => {
                        // In a real implementation, you'd have to check the current block height
                        // Here we're just using a simple estimation
                        height as u32
                    },
                    Confirmation::Unconfirmed => 0,
                },
                Err(_) => 0,
            };
            
            (utxo.outpoint.txid.to_string(), utxo.txout.value, confirmations)
        })
        .collect();
        
    Ok(result)
}

/// Fetch network fee rate (in satoshis per vbyte)
pub async fn fetch_fee_rate(
    blockchain: &dyn BlockChain,
    target: Target,
) -> Result<f32, PulserError> {
    blockchain.get_fee_rate(target)
        .await
        .map_err(|e| PulserError::ApiError(format!("Failed to fetch fee rate: {}", e)))
}

/// Broadcast a transaction
pub async fn broadcast_transaction(
    blockchain: &dyn BlockChain,
    tx: &Transaction,
) -> Result<Txid, PulserError> {
    blockchain.broadcast(tx)
        .await
        .map_err(|e| PulserError::TransactionError(format!("Failed to broadcast transaction: {}", e)))
}
