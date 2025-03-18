// In deposit-service/src/bin/monitor_address.rs
use bitcoin::Network;
use deposit_service::blockchain::create_esplora_client;
use deposit_service::wallet::create_taproot_multisig;
use deposit_service::keys::load_or_generate_key_material;
use std::path::Path;
use tokio::time::sleep;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting UTXO Monitor for Taproot Multisig");
    println!("=========================================");
    
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
            println!("Press Ctrl+C to stop monitoring");
            
            let mut prev_total = 0;
            
            // Continuous monitoring loop
            loop {
                // Sync with blockchain
                wallet.sync().await?;
                
                // Get UTXOs
                let mut utxos = wallet.list_utxos()?;
                wallet.update_utxo_confirmations(&mut utxos).await?;
                
                let total_sats: u64 = utxos.iter().map(|u| u.amount).sum();
                
                // Print status
                let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
                println!("\n[{}] Status update:", now);
                
                if utxos.is_empty() {
                    println!("No UTXOs found. Waiting for deposits...");
                } else {
                    println!("Found {} UTXOs:", utxos.len());
                    
                    let total_confirmed: u64 = utxos.iter()
                        .filter(|u| u.confirmations > 0)
                        .map(|u| u.amount)
                        .sum();
                        
                    let total_unconfirmed = total_sats - total_confirmed;
                    
                    for (i, utxo) in utxos.iter().enumerate() {
                        println!("UTXO #{}: {} sats, confirmations: {}", 
                                 i+1, utxo.amount, utxo.confirmations);
                    }
                    
                    println!("\nTotal confirmed: {} sats", total_confirmed);
                    println!("Total unconfirmed: {} sats", total_unconfirmed);
                    println!("Total balance: {} sats", total_sats);
                    
                    // Check for new deposits
                    if total_sats > prev_total {
                        println!("\n🎉 NEW DEPOSIT DETECTED! +{} sats", total_sats - prev_total);
                    }
                    
                    prev_total = total_sats;
                }
                
                // Wait for 30 seconds before checking again
                sleep(Duration::from_secs(30)).await;
            }
        },
        Err(e) => {
            println!("Error: Failed to create blockchain client: {}", e);
        }
    }
    
    Ok(())
}
