use deposit_service::wallet::DepositWallet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user_id = "user1";
    let (mut wallet, deposit_info) = DepositWallet::from_config("config/service_config.toml", user_id)?;
    println!("Monitoring address: {}", deposit_info.address);
    wallet.monitor_deposits(user_id).await?;
    Ok(())
}
