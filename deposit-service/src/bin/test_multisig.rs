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

    let user_json = fs::read_to_string(Path::new("test_data/secrets").join("user_1_key.json"))?;
    let user_data: UserData = serde_json::from_str(&user_json)?;
    println!("User Public Key: {}", user_data.public_key);

    let config_path = "config/service_config.toml"; 
let user_id = "test_user";
let pubkey = deposit_service::wallet::secure_init(user_id, data_dir)?;
println!("Generated X-only pubkey: {}", pubkey);
let (mut wallet, deposit_info) = DepositWallet::from_config(config_path, &pubkey)?;
    let (mut wallet, deposit_info) = DepositWallet::from_config(config_path, &user_data.public_key)?;
    println!("Deposit Address: {}", deposit_info.address);

    println!("\nInitial sync...");
    wallet.sync().await?;
   let mut old_utxos = Vec::new(); // Track previous state
loop {
    wallet.sync().await?;
    let mut utxos = wallet.list_utxos()?;
    wallet.update_utxo_confirmations(&mut utxos).await?;
    println!("UTXOs: {:?}", utxos);
    if utxos != old_utxos && !utxos.is_empty() {
        let amount = utxos.last().unwrap().amount; // Latest deposit
        fs::write("deposit_notification.txt", 
            format!("New deposit: {} sats at {}", amount, utxos.last().unwrap().txid))?;
    }
for utxo in &utxos {
    if utxo.confirmations > 0 {
        println!("Deposit detected! TXID: {}, Amount: {:.8} BTC", utxo.txid, utxo.amount as f64 / 100_000_000.0);
    }
}
let total_amount = utxos.iter().map(|u| u.amount).sum::<u64>();
if total_amount > 0 {
    println!("Total deposits detected! Amount: {:.8} BTC", total_amount as f64 / 100_000_000.0);
}
    old_utxos = utxos; // Update state
    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
}

    Ok(())
}
