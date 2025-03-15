// In deposit-service/examples/test_wallet.rs

use deposit_service::wallet::DepositWallet;
use bdk_esplora::esplora_client::Builder as EsploraBuilder;
use bitcoin::Network;
use std::sync::Arc;
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    
    // Create Esplora client for testnet
    let blockchain = Arc::new(
        EsploraBuilder::new("https://blockstream.info/testnet/api/")
            .build()
    );
    
    // Test keys (replace with your own or generate random ones)
    let user_pubkey = "02e6642fd69bd211f93f7f1f36ca51a26a5290eb2dd1b0d8279a87bb0d480c8443";
    let lsp_pubkey = "0289a923c637a162a87a92a7bde38a80eead4cb107310750f0ebf9dd851eb1509a";
    let trustee_pubkey = "03a621e6f6f5f5b89e3b048d518d5a25eb62b79bf35fa88743753e6c56725d9518";
    
    println!("Creating multisig wallet with keys:");
    println!("User: {}", user_pubkey);
    println!("LSP: {}", lsp_pubkey);
    println!("Trustee: {}", trustee_pubkey);
    
    // Create temporary data directory
    let temp_dir = tempdir::TempDir::new("wallet_test")?;
    let data_dir = temp_dir.path().to_str().unwrap();
    
    // Create the multisig wallet
    let (wallet, deposit_info) = DepositWallet::create_taproot_multisig(
        user_pubkey,
        lsp_pubkey,
        trustee_pubkey,
        Network::Testnet,
        blockchain.clone(),
        data_dir,
    )?;
    
    println!("\nWallet created successfully!");
    println!("Deposit address: {}", deposit_info.address);
    println!("Descriptor: {}", deposit_info.descriptor);
    
    // Test syncing
    println!("\nSyncing wallet...");
    wallet.sync().await?;
    println!("Sync completed!");
    
    // List UTXOs if any
    let utxos = wallet.list_utxos()?;
    println!("\nFound {} UTXOs:", utxos.len());
    for (i, utxo) in utxos.iter().enumerate() {
        println!("UTXO #{}: {} sats, confirmations: {}", 
                 i+1, utxo.amount, utxo.confirmations);
    }
    
    println!("\nWallet test completed successfully!");
    Ok(())
}
