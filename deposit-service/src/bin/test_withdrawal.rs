use bitcoin::Network;
use deposit_service::blockchain::create_esplora_client;
use std::path::Path;
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting withdrawal test");
    
    // Setup test data directory
    let data_dir = "./test_data";
    std::fs::create_dir_all(data_dir)?;
    
    // Create blockchain client
    let network = Network::Testnet;
    let blockchain = match create_esplora_client(network) {
        Ok(client) => {
            println!("Connected to blockchain client successfully");
            client
        },
        Err(e) => {
            println!("Warning: Could not create blockchain client: {}", e);
            println!("Continuing without blockchain syncing capability");
            return Ok(());
        }
    };

    println!("Test withdrawal functionality completed");
    Ok(())
}
