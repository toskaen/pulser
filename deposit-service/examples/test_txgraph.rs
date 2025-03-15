// In deposit-service/examples/test_txgraph.rs

use bdk_chain::{TxUpdate, BlockId, TxPosInBlock};
use bdk_wallet::{Wallet, KeychainKind};
use bitcoin::{Transaction, Txid, Network};
use std::sync::Arc;
use std::str::FromStr;
use bdk_esplora::esplora_client::Builder as EsploraBuilder;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    
    // Create Esplora client for testnet
    let client = EsploraBuilder::new("https://blockstream.info/testnet/api/")
        .build();
    
    // Create a sample wallet (using a simple descriptor for testing)
    let descriptor = "wpkh(tprv8ZgxMBicQKsPe73PBRSmNbTfbcsZnwWhz5eVmhHpi31HW29Z7mc9B4cWGRQzopNUzZUT391DeDJxL2PefNunWyLgqCKRMDkU1s2s8bAfoSk/84'/1'/0'/0/*)";
    let change_descriptor = "wpkh(tprv8ZgxMBicQKsPe73PBRSmNbTfbcsZnwWhz5eVmhHpi31HW29Z7mc9B4cWGRQzopNUzZUT391DeDJxL2PefNunWyLgqCKRMDkU1s2s8bAfoSk/84'/1'/0'/1/*)";
    
    let wallet = Wallet::create(descriptor, change_descriptor)
        .network(Network::Testnet)
        .create_wallet_no_persist()?;
    
    // Get an address to check
    let address = wallet.reveal_next_address(KeychainKind::External);
    println!("Checking for transactions to address: {}", address.address);
    
    // Test transaction query
    let script = address.address.script_pubkey();
    println!("Looking for UTXOs for script: {}", script);
    
    // Build a TxUpdate manually - in real code this would come from querying the blockchain
    let mut update = TxUpdate::default();
    
    // Mock timestamp for unconfirmed transactions
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs();
    
    // Check if there's a known TXID to test with
    if let Ok(txid) = Txid::from_str("a7b92e69410a10bacf28b96e31ada8871fc8348c7b4607a100e1c677cd3e313e") {
        if let Ok(tx_details) = client.get_tx(&txid).await {
            let tx = tx_details.transaction;
            
            // Add to update based on confirmation status
            if let Some(conf_time) = tx_details.confirmation_time {
                println!("Found confirmed transaction: {}", txid);
                update.confirmed_txs.push((
                    Arc::new(tx),
                    TxPosInBlock {
                        block_id: BlockId {
                            height: conf_time.height,
                            hash: tx_details.block_hash.unwrap(),
                        },
                        pos: 0, // Position unknown
                    },
                ));
            } else {
                println!("Found unconfirmed transaction: {}", txid);
                update.unconfirmed_txs.push((Arc::new(tx), timestamp));
            }
        }
    }
    
    // Apply update (if we added any transactions)
    if !update.confirmed_txs.is_empty() || !update.unconfirmed_txs.is_empty() {
        println!("Applying update to wallet...");
        wallet.apply_update(update)?;
        println!("Update applied successfully!");
    } else {
        println!("No transactions found to update.");
    }
    
    println!("\nTxGraph test completed successfully!");
    Ok(())
}
