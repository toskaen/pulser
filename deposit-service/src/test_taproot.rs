// In deposit-service/src/bin/test_taproot.rs
use bitcoin::Network;
use deposit_service::blockchain::create_esplora_client;
use deposit_service::wallet::create_taproot_multisig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Taproot Multisig Test");
    
    // Setup test data directory
    let data_dir = "./test_data";
    std::fs::create_dir_all(data_dir)?;

    // Use hardcoded valid x-only pubkeys for testing
    let user_pubkey = "cc8a4bc64d897bddc5fbc2f670f7a8ba0b386779106cf1223c6fc5d7cd6fc115";
    let lsp_pubkey = "fff97bd5755eeea420453a14355235d382f6472f8568a18b2f057a1460297556";
    let trustee_pubkey = "a8c5bac163e3f2bd359d95654a95a569c4dd241d3bb9ff9a14063dba0c5d5d10";
    
    println!("User Public Key: {}", user_pubkey);
    println!("LSP Public Key: {}", lsp_pubkey);
    println!("Trustee Public Key: {}", trustee_pubkey);
    
    // Create blockchain client
    let network = Network::Testnet;
    let blockchain = create_esplora_client(network)?;
    
    // Create the multisig wallet
    let (wallet, deposit_info) = create_taproot_multisig(
        user_pubkey,
        lsp_pubkey,
        trustee_pubkey,
        network,
        blockchain,
        data_dir,
    )?;
    
    println!("\nTaproot Multisig Address: {}", deposit_info.address);
    println!("Descriptor: {}", deposit_info.descriptor);
    
    println!("\nTest completed successfully!");
    Ok(())
}
