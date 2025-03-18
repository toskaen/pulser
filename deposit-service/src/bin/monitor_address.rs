use deposit_service::wallet::WalletManager;
use deposit_service::monitoring::start_monitoring;
use deposit_service::config::Config;
use deposit_service::error::PulserError;
use std::path::PathBuf;
use tokio::time::{self, Duration};
use log::{info, error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    println!("Starting UTXO Monitor for Taproot Multisig");
    println!("=========================================");

    // Setup data directory and config
    let data_dir = PathBuf::from("./test_data");
    std::fs::create_dir_all(&data_dir)?;
    let config = Config {
        network: bitcoin::Network::Testnet,
        lsp_pubkey: None, // Not used here; keys are hardcoded in WalletManager
        trustee_pubkey: None,
        ..Default::default()
    };

    // Create WalletManager for user 1
    let wallet_manager = Arc::new(WalletManager::new(&config, data_dir, Some(1)).await?);
    let address = wallet_manager.get_new_address().await?;
    println!("Monitoring address: {}", address);
    println!("Press Ctrl+C to stop monitoring");

    // Spawn monitoring task
    let wallet_manager_clone = wallet_manager.clone();
    tokio::spawn(async move {
        start_monitoring(wallet_manager_clone).await;
    });

    // Custom monitoring loop for detailed output
    let mut prev_total = 0;
    loop {
        if let Err(e) = wallet_manager.sync_wallet().await {
            error!("Sync failed: {}", e);
            continue;
        }

        let utxos = wallet_manager.list_unspent().await?;
        let total_sats: u64 = utxos.iter().map(|u| u.txout.value).sum();
        let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");

        println!("\n[{}] Status update:", now);
        if utxos.is_empty() {
            println!("No UTXOs found. Waiting for deposits...");
        } else {
            println!("Found {} UTXOs:", utxos.len());
            for (i, utxo) in utxos.iter().enumerate() {
                println!(
                    "UTXO #{}: {} sats, confirmations: N/A", // Confirmations not directly available in BDK 1.1.0 Utxo
                    i + 1,
                    utxo.txout.value
                );
            }
            println!("Total balance: {} sats", total_sats);

            if total_sats > prev_total {
                println!("\n🎉 NEW DEPOSIT DETECTED! +{} sats", total_sats - prev_total);
            }
            prev_total = total_sats;
        }

        time::sleep(Duration::from_secs(30)).await;
    }
}
