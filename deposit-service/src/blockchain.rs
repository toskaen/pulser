use bitcoin::Network;
use bdk_esplora::{
    Builder as EsploraBuilder, 
    EsploraClient,
    Error as EsploraError,
    EsploraExt
};
use common::PulserError;
use std::time::Duration;
use std::str::FromStr;
use bitcoin::{Transaction, Txid, Address, Script};
use sha2::Digest;

/// Create an Esplora blockchain client
pub fn create_esplora_client(network: Network) -> Result<EsploraClient, PulserError> {
    let url = match network {
        Network::Bitcoin => "https://blockstream.info/api/",
        Network::Testnet => "https://blockstream.info/testnet/api/",
        Network::Signet => "https://mempool.space/signet/api/",
        Network::Regtest => "http://localhost:3002/",
    };
    
    let client = EsploraBuilder::new(url)
        .timeout(Duration::from_secs(30))
        .build();
    
    Ok(client)
}

/// Fetch UTXOs for an address
pub async fn fetch_address_utxos(
    blockchain: &EsploraClient,
    address: &str,
) -> Result<Vec<(String, u64, u32)>, PulserError> {
    // Convert the address to bitcoin::Address
    let addr = Address::from_str(address)
        .map_err(|e| PulserError::InvalidRequest(format!("Invalid address: {}", e)))?;
    
    // Get the script pubkey for the address
    let script = addr.assume_checked().script_pubkey();
    
    // Get UTXOs for the script
    let utxos = blockchain.get_scriptpubkey_utxos(&script)
        .await
        .map_err(|e| PulserError::ApiError(format!("Failed to fetch UTXOs: {}", e)))?;
        
    // Format the output
    let mut result = Vec::new();
    
    for utxo in utxos {
        // Get confirmation status
        let tx_info = blockchain.get_tx(&utxo.txid)
            .await
            .map_err(|e| PulserError::ApiError(format!("Failed to get tx: {}", e)))?;
        
        let confirmations = match tx_info.confirmation_time {
            Some(conf_time) => {
                // Get current height
                let current_height = blockchain.get_height()
                    .await
                    .map_err(|e| PulserError::ApiError(format!("Failed to get height: {}", e)))?;
                
                if current_height > conf_time.height {
                    current_height - conf_time.height + 1
                } else {
                    0
                }
            },
            None => 0, // Unconfirmed
        };
        
        result.push((
            utxo.txid.to_string(),
            utxo.vout,
            confirmations as u32
        ));
    }
        
    Ok(result)
}

/// Fetch fee rate in satoshis per virtual byte
pub async fn fetch_fee_rate(
    blockchain: &EsploraClient,
) -> Result<f32, PulserError> {
    let fee_estimates = blockchain.get_fee_estimates()
        .await
        .map_err(|e| PulserError::ApiError(format!("Failed to fetch fee estimates: {}", e)))?;
    
    // Get fee for 2 blocks confirmation as a common target
    let fee_rate = fee_estimates.get(&2).cloned().unwrap_or(5.0);
    
    Ok(fee_rate)
}

/// Broadcast a transaction
pub async fn broadcast_transaction(
    blockchain: &EsploraClient,
    tx: &Transaction,
) -> Result<Txid, PulserError> {
    blockchain.broadcast(tx)
        .await
        .map_err(|e| PulserError::TransactionError(format!("Failed to broadcast transaction: {}", e)))
}
