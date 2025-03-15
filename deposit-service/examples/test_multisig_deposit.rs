// deposit-service/src/bin/test_multisig.rs

use deposit_service::wallet::DepositWallet;  // This assumes your wallet module is public
use bdk_esplora::esplora_client::EsploraClient;
use bitcoin::Network;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use log::{info, warn};
use std::path::Path;
use std::fs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::Builder::new().filter_level(log::LevelFilter::Info).init();
    
    info!("Starting multisig wallet test");
    
    // Create Esplora client for testnet
    let blockchain = Arc::new(
        EsploraClient::from_url("https://blockstream.info/testnet/api")
    );
    
    // Test keys
    let user_pubkey = "02e6642fd69bd211f93f7f1f36ca51a26a5290eb2dd1b0d8279a87bb0d480c8443";
    let lsp_pubkey = "0289a923c637a162a87a92a7bde38a80eead4cb107310750f0ebf9dd851eb1509a";
    let trustee_pubkey = "03a621e6f6f5f5b89e3b048d518d5a25eb62b79bf35fa88743753e6c56725d9518";
    
    info!("Using test keys:");
    info!("User:    {}", user_pubkey);
    info!("LSP:     {}", lsp_pubkey);
    info!("Trustee: {}", trustee_pubkey);
    
    // Create data directory
    let data_dir = "./test_data";
    fs::create_dir_all(data_dir)?;
    
    // Create the multisig wallet
    let (wallet, deposit_info) = DepositWallet::create_taproot_multisig(
        user_pubkey,
        lsp_pubkey,
        trustee_pubkey,
        Network::Testnet,
        blockchain.clone(),
        data_dir,
    )?;
    
    info!("Wallet created successfully!");
    info!("Deposit address: {}", deposit_info.address);
    info!("External descriptor: {}", deposit_info.descriptor);
    
    // Monitor for deposits
    info!("Monitoring address for deposits. Send some testnet BTC to {} and wait...", deposit_info.address);
    
    let mut iteration = 0;
    loop {
        iteration += 1;
        info!("Sync iteration {}...", iteration);
        
        // Sync wallet with blockchain
        match wallet.sync().await {
            Ok(_) => info!("Sync completed"),
            Err(e) => warn!("Sync error: {}", e),
        }
        
        // Get and display UTXOs
        let mut utxos = match wallet.list_utxos() {
            Ok(u) => u,
            Err(e) => {
                warn!("Failed to list UTXOs: {}", e);
                vec![]
            }
        };
        
        // Update confirmations
        if !utxos.is_empty() {
            if let Err(e) = wallet.update_utxo_confirmations(&mut utxos).await {
                warn!("Failed to update UTXO confirmations: {}", e);
            }
        }
        
        if utxos.is_empty() {
            info!("No UTXOs found yet. Waiting...");
        } else {
            info!("Found {} UTXOs:", utxos.len());
            
            let mut total_confirmed = 0;
            let mut total_unconfirmed = 0;
            
            for (i, utxo) in utxos.iter().enumerate() {
                info!("UTXO #{}: {} sats, confirmations: {}", 
                     i+1, utxo.amount, utxo.confirmations);
                
                if utxo.confirmations > 0 {
                    total_confirmed += utxo.amount;
                } else {
                    total_unconfirmed += utxo.amount;
                }
            }
            
            info!("Total confirmed:   {} sats", total_confirmed);
            info!("Total unconfirmed: {} sats", total_unconfirmed);
        }
        
        // Check every 30 seconds
        sleep(Duration::from_secs(30)).await;
    }
