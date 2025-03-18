// In deposit-service/src/bin/check_utxos.rs
use bitcoin::Network;
use deposit_service::blockchain::create_esplora_client;
use deposit_service::wallet::create_taproot_multisig;
use deposit_service::keys::load_or_generate_key_material;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Checking UTXOs for Taproot Multisig");
    println!("==================================");
    
    // Setup data directory
    let data_dir = "./test_data";
    std::fs::create_dir_all(data_dir)?;
    
    // Set up the network
    let network = Network::Testnet;
    
    // Load keys (or generate if they don't exist)
    let user_key = load_or_generate_key_material(
        "user", Some(1), network, Path::new(data_dir), true
    )?;
    
    let lsp_key = load_or_generate_key_material(
        "lsp", None, network, Path::new(data_dir), false
    )?;
    
    let trustee_key = load_or_generate_key_material(
        "trustee", None, network, Path::new(data_dir), false
    )?;
    
    println!("Creating blockchain client...");
    match create_esplora_client(network) {
        Ok(blockchain) => {
            // Create the multisig wallet
            let (wallet, info) = create_taproot_multisig(
                &user_key.public_key,
                &lsp_key.public_key,
                &trustee_key.public_key,
                network,
                blockchain,
                data_dir,
            )?;
            
            println!("Monitoring address: {}", info.address);
            
            // Sync with the blockchain
            println!("Syncing with blockchain...");
            wallet.sync().await?;
            
            // List UTXOs
            let utxos = wallet.list_utxos()?;
            
            if utxos.is_empty() {
                println!("No UTXOs found. Send some funds to the address to see them.");
            } else {
                println!("Found {} UTXOs:", utxos.len());
                
                let mut total_unconfirmed = 0;
                let mut total_confirmed = 0;
                
                for (i, utxo) in utxos.iter().enumerate() {
                    println!("UTXO #{}: {} sats, confirmations: {}", 
                             i+1, utxo.amount, utxo.confirmations);
                             
                    if utxo.confirmations > 0 {
                        total_confirmed += utxo.amount;
                    } else {
                        total_unconfirmed += utxo.amount;
                    }
                }
                
                println!("\nTotal confirmed: {} sats", total_confirmed);
                println!("Total unconfirmed: {} sats", total_unconfirmed);
                println!("Total balance: {} sats", total_confirmed + total_unconfirmed);
            }
        },
        Err(e) => {
            println!("Error: Failed to create blockchain client: {}", e);
        }
    }
    
    Ok(())
}
