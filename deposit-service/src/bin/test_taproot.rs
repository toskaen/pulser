use bitcoin::Network;
use deposit_service::blockchain::create_esplora_client;
use deposit_service::wallet::{DepositWallet, create_taproot_multisig};
use deposit_service::keys::{load_or_generate_key_material, store_wallet_recovery_info};
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Taproot Multisig Test - Full Implementation");
    println!("===========================================");

    let data_dir = "./test_data";
    std::fs::create_dir_all(data_dir)?;

    let network = Network::Testnet;

    println!("\nGenerating or loading keys...");
    let mut user_key = load_or_generate_key_material("user", Some(1), network, Path::new(data_dir), true)?;
    let lsp_key = load_or_generate_key_material("lsp", None, network, Path::new(data_dir), false)?;
    let trustee_key = load_or_generate_key_material("trustee", None, network, Path::new(data_dir), false)?;

    println!("User Public Key:    {}", user_key.public_key);
    println!("LSP Public Key:     {}", lsp_key.public_key);
    println!("Trustee Public Key: {}", trustee_key.public_key);

    println!("\nCreating blockchain client...");
    let blockchain = create_esplora_client(network)?;

    println!("\nCreating taproot multisig wallet...");
    let (mut deposit_wallet, deposit_info) = create_taproot_multisig(
        &user_key.public_key,
        &lsp_key.public_key,
        &trustee_key.public_key,
        network,
        blockchain,
        data_dir,
    )?;

    println!("\nTaproot Multisig Address: {}", deposit_info.address);
    println!("Descriptor: {}", deposit_info.descriptor);

    // Store recovery info
    store_wallet_recovery_info(
        &mut user_key,
        &deposit_info.descriptor,
        &lsp_key.public_key,
        &trustee_key.public_key,
        Path::new(data_dir),
    )?;

    // Persist wallet
    deposit_wallet.persist()?;
    println!("Wallet persisted to: {}", deposit_wallet.wallet_path);

    // Sync and test deposit detection
    println!("\nSyncing with blockchain...");
    deposit_wallet.sync().await?;
    let utxos = deposit_wallet.list_utxos()?;
    println!("Initial UTXOs: {:?}", utxos);

    // Simulate monitoring (run for a few iterations)
    println!("\nMonitoring for deposits (5 iterations, 10s interval)...");
    for i in 1..=5 {
        deposit_wallet.sync().await?;
        let mut utxos = deposit_wallet.list_utxos()?;
        deposit_wallet.update_utxo_confirmations(&mut utxos).await?;
        println!("Iteration {} - UTXOs: {:?}", i, utxos);
        if !utxos.is_empty() {
            println!("Deposit detected!");
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }

    println!("\nTest completed successfully!");
    Ok(())
}
