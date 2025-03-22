use withdraw_service::withdraw::WithdrawManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let usd_amount = std::env::args().nth(1).expect("USD amount").parse::<f64>()?;
    let mut manager = WithdrawManager::new("service_config.toml", "1")?;
    manager.withdraw_user(usd_amount).await?;
    Ok(())
}
