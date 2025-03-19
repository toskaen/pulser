use crate::wallet::DepositWallet;
use common::error::PulserError;

pub async fn monitor_deposits(mut wallet: DepositWallet) -> Result<(), PulserError> {
    loop {
        wallet.sync().await?;
        let utxos = wallet.list_utxos()?;
        for utxo in utxos {
            if utxo.confirmations > 0 {
                println!("Confirmed deposit: {} BTC at {}", utxo.amount as f64 / 100_000_000.0, utxo.txid);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
