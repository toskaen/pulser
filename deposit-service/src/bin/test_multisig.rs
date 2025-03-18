// deposit-service/src/bin/test_multisig.rs
use deposit_service::wallet::DepositWallet;
use std::path::Path;
use serde_json;
use std::fs;
use env_logger;

#[derive(serde::Deserialize)]
struct UserData {
    public_key: String,
    lsp_pubkey: String,
    trustee_pubkey: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    println!("Multisig Deposit Test - Isolated Case");
    println!("=====================================");

    let data_dir = "data/wallets";
    fs::create_dir_all(data_dir)?;

    let user_json = fs::read_to_string(Path::new("test_data").join("user1.json"))?;
    let user_data: UserData = serde_json::from_str(&user_json)?;
    println!("User Public Key: {}", user_data.public_key);

    let config_path = "config/service_config.toml";
    let (mut wallet, deposit_info) = DepositWallet::from_config(config_path, &user_data.public_key)?;
    println!("Deposit Address: {}", deposit_info.address);

    println!("\nInitial sync...");
    wallet.sync().await?;
    let mut utxos = wallet.list_utxos()?;
    wallet.update_utxo_confirmations(&mut utxos).await?;
    println!("Initial UTXOs: {:?}", utxos);

    wallet.persist()?;
    println!("Wallet persisted to: {}", wallet.wallet_path);

    println!("\nMonitoring deposits (5 iterations, 10s interval)...");
    for i in 1..=5 {
        wallet.sync().await?;
        let mut utxos = wallet.list_utxos()?;
        wallet.update_utxo_confirmations(&mut utxos).await?;
        println!("Iteration {} - UTXOs: {:?}", i, utxos);
        if !utxos.is_empty() {
            println!("Deposit detected! Amount: {} BTC", utxos[0].amount as f64 / 100_000_000.0);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }

    println!("\nTest completed!");
    Ok(())
}
