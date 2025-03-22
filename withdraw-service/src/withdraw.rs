use deposit_service::wallet::DepositWallet;
use common::error::PulserError;
use common::types::{StableChain, USD, Bitcoin};
use std::fs;

pub struct WithdrawManager {
    wallet: DepositWallet,
}

impl WithdrawManager {
    pub fn new(config_path: &str, user_id: &str) -> Result<Self, PulserError> {
        let (wallet, _) = DepositWallet::from_config(config_path, user_id)?;
        Ok(WithdrawManager { wallet })
    }

    pub async fn withdraw_user(&mut self, usd_amount: f64) -> Result<(), PulserError> {
        let sc_path = format!("{}/stable_chain_1.json", self.wallet.wallet_path);
        let mut chain: StableChain = serde_json::from_str(&fs::read_to_string(&sc_path)?)?;
        if chain.stabilized_usd.0 < 10.0 {
            return Err(PulserError::InvalidInput("Balance below $10 minimum".to_string()));
        }

        let client = reqwest::Client::new();
        let price_info = fetch_btc_usd_price(&client).await?;
        let raw_btc_usd = price_info.raw_btc_usd;
        let btc_needed = usd_amount / raw_btc_usd;
        let total_btc_value = chain.accumulated_btc.to_btc() * raw_btc_usd;

        let config_str = fs::read_to_string("service_config.toml")?;
        let config: Config = toml::from_str(&config_str)?;

        if total_btc_value >= usd_amount {
            let mut tx_builder = self.wallet.build_tx();
            tx_builder.add_recipient(
                Address::from_str(&config.threshold_btc_address)?.assume_checked().script_pubkey(),
                Amount::from_sat((btc_needed * 100_000_000.0) as u64)
            );
            let mut psbt = tx_builder.finish()?;
            self.wallet.sign(&mut psbt, Default::default())?;
            let tx = psbt.extract_tx()?;
            let txid = self.wallet.blockchain.broadcast(&tx).await?;
            println!("User {}: Sent {:.6} BTC to threshold address (txid: {})", chain.user_id, btc_needed, txid);
            println!("User {}: Mock withdrawal: ${:.2} to user", chain.user_id, usd_amount);

            chain.accumulated_btc = Bitcoin::from_sats(chain.accumulated_btc.to_sat() - (btc_needed * 100_000_000.0) as u64);
            chain.stabilized_usd = USD(chain.utxos.iter().map(|u| u.usd_value.as_ref().unwrap_or(&USD(0.0)).0).sum());
            chain.short_reduction_amount = Some(btc_needed); // Signal hedge-service
 } else {
            let mut tx_builder = self.wallet.build_tx();
            tx_builder.add_recipient(
                Address::from_str(&config.deribit_testnet_address)?.assume_checked().script_pubkey(),
                Amount::from_sat(chain.accumulated_btc.to_sats())
            );
            let mut psbt = tx_builder.finish()?;
            self.wallet.sign(&mut psbt, Default::default())
                .map_err(|e| PulserError::SigningError(e.to_string()))?;
            let tx = psbt.extract_tx()?;
            let txid = self.wallet.blockchain.broadcast(&tx).await?;
            println!("User {}: Sent {:.6} BTC to Deribit address (txid: {})", chain.user_id, chain.accumulated_btc.to_btc(), txid);
            println!("User {}: Mock hedge settled: ${:.2}", chain.user_id, usd_amount);

            chain.short_reduction_amount = Some(chain.accumulated_btc.to_btc());
            chain.accumulated_btc = Bitcoin::from_sats(0);
            chain.stabilized_usd = USD(0.0);
        }

        fs::write(&sc_path, serde_json::to_string_pretty(&chain)?)?;
        Ok(())
    }
}

#[derive(Deserialize)]
pub struct Config {
    pub network: Network,
    pub esplora_url: String,
    pub lsp_pubkey: String,
    pub trustee_pubkey: String,
    pub data_dir: String,
    pub wallet_dir: String,
    pub threshold_btc_address: String,
    pub deribit_testnet_address: String,
}
