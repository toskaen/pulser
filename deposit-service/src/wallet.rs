// deposit-service/src/wallet.rs
use std::collections::HashMap;
use bdk_wallet::{Wallet, KeychainKind};
use bdk_esplora::esplora_client;
use bdk_wallet::bitcoin::{Network, Address};
use common::error::PulserError;
use common::types::{USD, Event, PriceInfo, Utxo};
use crate::types::{StableChain, UtxoInfo, Bitcoin, DepositAddressInfo};
use crate::storage::StateManager;
use log::info;
use chrono::Utc;
use tokio::sync::Mutex;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref LOGGED_ADDRESSES: Mutex<HashMap<String, Address>> = Mutex::new(HashMap::new());
}

pub struct DepositWallet {
    pub wallet: Wallet,
    pub blockchain: esplora_client::AsyncClient,
    pub network: Network,
    pub wallet_path: String,
    pub events: Vec<Event>,
    pub stable_chain: StableChain,
}

impl DepositWallet {
use crate::storage::StateManager;

pub async fn from_config(
    config_path: &str,
    user_id: &str,
    state_manager: &StateManager, // Add this
) -> Result<(Self, DepositAddressInfo, StableChain), PulserError> {
    let config_str = fs::read_to_string(config_path)?;
    let config: wallet_init::Config = toml::from_str(&config_str)?;
    let init_result = wallet_init::init_wallet(&config, user_id)?;

    let wallet_path = format!("{}/user_{}/multisig", config.data_dir, user_id);
    let initial_addr = Address::from_str(&init_result.deposit_info.address)?;
    let wallet = DepositWallet::from_descriptors(
        init_result.external_descriptor.clone(),
        init_result.internal_descriptor.clone(),
        Network::from_str(&config.network)?,
        &config.esplora_url,
        &wallet_path,
        user_id,
        initial_addr.clone(),
        state_manager,
    ).await?;

    let deposit_info = DepositAddressInfo {
        address: initial_addr.to_string(),
        descriptor: init_result.deposit_info.descriptor,
        path: init_result.deposit_info.path,
        user_pubkey: init_result.public_data.user_pubkey,
        lsp_pubkey: init_result.public_data.lsp_pubkey,
        trustee_pubkey: init_result.public_data.trustee_pubkey,
    };

    let stable_chain = state_manager.load_or_init_stable_chain(user_id, &wallet_path, initial_addr.to_string()).await?;
    Ok((wallet, deposit_info, stable_chain))
}

    pub async fn update_stable_chain(&mut self, price_info: &PriceInfo, state_manager: &StateManager) -> Result<Vec<Utxo>, PulserError> {
        let logged = LOGGED_ADDRESSES.lock().await;
        let current_addr = logged.get(&self.stable_chain.user_id.to_string())
            .ok_or(PulserError::WalletError("No logged address".into()))?;
        let utxos = self.check_address(current_addr, price_info).await?;
        self.stable_chain.utxos = utxos.clone();
        self.stable_chain.accumulated_btc = Bitcoin { sats: self.stable_chain.utxos.iter().map(|u| u.amount).sum() };
        self.stable_chain.stabilized_usd = USD((self.stable_chain.accumulated_btc.sats as f64 / 100_000_000.0) * price_info.raw_btc_usd);
        self.stable_chain.raw_btc_usd = price_info.raw_btc_usd;
        self.stable_chain.timestamp = Utc::now().timestamp();
        self.stable_chain.formatted_datetime = Utc::now().to_rfc3339();
        self.stable_chain.prices = price_info.price_feeds.clone();
        self.stable_chain.multisig_addr = current_addr.to_string();
        state_manager.save_stable_chain(&self.stable_chain.user_id.to_string(), &self.stable_chain).await?;
        info!("Updated stable chain for user {}: {} BTC", self.stable_chain.user_id, self.stable_chain.accumulated_btc.to_btc());
        Ok(utxos)
    }

    pub async fn reveal_new_address(&mut self, state_manager: &StateManager) -> Result<Address, PulserError> {
        let new_addr = self.wallet.reveal_next_address(KeychainKind::External).address;
        let mut logged = LOGGED_ADDRESSES.lock().await;
        logged.insert(self.stable_chain.user_id.to_string(), new_addr.clone());
        self.stable_chain.multisig_addr = new_addr.to_string();
        state_manager.save_stable_chain(&self.stable_chain.user_id.to_string(), &self.stable_chain).await?;
        info!("Revealed new deposit address for user {}: {}", self.stable_chain.user_id, new_addr);
        Ok(new_addr)
    }

    // ... (check_address, list_utxos unchanged) ...
}

    pub async fn check_address(&self, address: &Address, price_info: &PriceInfo) -> Result<Vec<Utxo>, PulserError> {
        let script = address.script_pubkey();
        let base_utxos = self.list_utxos()?;
        let current_height = self.blockchain.get_height().await?;
        let mut validated_utxos = Vec::new();
        for u in base_utxos.into_iter().filter(|u| u.script_pubkey == script.to_hex_string()) {
            let tx_status = self.blockchain.get_tx_status(&u.txid.parse().unwrap()).await?;
            let confirmations = tx_status.confirmed.then(|| current_height.saturating_sub(tx_status.block_height.unwrap_or(0)) + 1).unwrap_or(0);
            if tx_status.confirmed && confirmations >= 1 {
                if let Some(tx) = self.blockchain.get_tx(&u.txid.parse().unwrap()).await? {
                    if tx.output.iter().any(|out| out.script_pubkey == script && out.value.to_sat() == u.amount) {
                        validated_utxos.push(Utxo {
                            txid: u.txid,
                            vout: u.vout,
                            amount: u.amount,
                            confirmations: confirmations as u32,
                            script_pubkey: u.script_pubkey,
                            height: tx_status.block_height.map(|h| h as u32),
                            usd_value: Some(USD((u.amount as f64 / 100_000_000.0) * price_info.raw_btc_usd)),
                        });
                    }
                }
            }
        }
        Ok(validated_utxos)
    }
    pub fn list_utxos(&self) -> Result<Vec<Utxo>, PulserError> {
        let utxos: Vec<Utxo> = self.wallet.list_unspent().into_iter().map(|utxo| {
            Utxo {
                txid: utxo.outpoint.txid.to_string(),
                vout: utxo.outpoint.vout,
                amount: utxo.txout.value.to_sat(),
                confirmations: 0,
                script_pubkey: utxo.txout.script_pubkey.to_hex_string(),
                height: None,
                usd_value: None,
            }
        }).collect();
        Ok(utxos)
    }
