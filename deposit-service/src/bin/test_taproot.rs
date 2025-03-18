// In deposit-service/src/bin/test_taproot.rs
use bitcoin::Network;
use deposit_service::blockchain::create_esplora_client;
use deposit_service::wallet::create_taproot_multisig;
use deposit_service::keys::{load_or_generate_key_material, store_wallet_recovery_info};
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Taproot Multisig Test - Full Implementation");
    println!("===========================================");
    
    // Setup test data directory
    let data_dir = "./test_data";
    std::fs::create_dir_all(data_dir)?;
    println!("Created data directory: {}", data_dir);

    // Set up the network
    let network = Network::Testnet;
    
    // Generate or load keys for all participants
    println!("\nGenerating or loading keys...");
    let mut user_key = load_or_generate_key_material(
        "user", Some(1), network, Path::new(data_dir), true
    )?;
    
    let lsp_key = load_or_generate_key_material(
        "lsp", None, network, Path::new(data_dir), false
    )?;
    
    let trustee_key = load_or_generate_key_material(
        "trustee", None, network, Path::new(data_dir), false
    )?;
    
    println!("User Public Key:    {}", user_key.public_key);
    println!("LSP Public Key:     {}", lsp_key.public_key);
    println!("Trustee Public Key: {}", trustee_key.public_key);
    
    println!("\nCreating blockchain client...");
    let blockchain = match create_esplora_client(network) {
        Ok(client) => {
            println!("Blockchain client created successfully.");
            client
        },
        Err(e) => {
            println!("Warning: Failed to create blockchain client: {}. Proceeding without blockchain sync.", e);
            return Ok(());
        }
    };
    
    println!("\nCreating taproot multisig wallet...");
    
    // Create the multisig wallet
    let (_, deposit_info) = create_taproot_multisig(
        &user_key.public_key,
        &lsp_key.public_key,
        &trustee_key.public_key,
        network,
        blockchain,
        data_dir,
    )?;
    
    // Store wallet recovery info
    store_wallet_recovery_info(
        &mut user_key,
        &deposit_info.descriptor,
        &lsp_key.public_key,
        &trustee_key.public_key,
        Path::new(data_dir)
    )?;
    
    println!("\nTaproot Multisig Address: {}", deposit_info.address);
    println!("Descriptor: {}", deposit_info.descriptor);
    
    println!("\nTest completed successfully!");
    Ok(())
}
