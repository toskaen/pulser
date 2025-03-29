// deposit-service/src/wallet.rs
use std::fs;
use std::str::FromStr;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use bdk_wallet::{Wallet, KeychainKind};
use bdk_esplora::esplora_client;
use bdk_esplora::EsploraAsyncExt;
use bdk_wallet::bitcoin::{Network, Address, Txid};
use bdk_chain::ChainPosition;
use common::error::PulserError;
use common::types::{USD, Event, PriceInfo, Utxo};
use crate::wallet_init::{Config, init_wallet};
use log::{info, warn, debug};
use chrono::Utc;
use tokio::sync::Mutex;
use tokio::time::sleep;
use common::price_feed::PriceFeed;
use common::types::UtxoInfo;
use common::StateManager;
use common::{StableChain, Bitcoin};
use common::types::DepositAddressInfo;
use common::wallet_utils;

lazy_static::lazy_static! {
    pub static ref LOGGED_ADDRESSES: Mutex<HashMap<String, Address>> = Mutex::new(HashMap::new());
}

#[derive(Debug)]
pub struct DepositWallet {
    pub wallet: Wallet,
    pub blockchain: esplora_client::AsyncClient,
    pub network: Network,
    pub wallet_path: String,
    pub events: Vec<Event>,
    pub stable_chain: StableChain,
    pub state_manager: Arc<StateManager>,
    pub price_feed: Arc<PriceFeed>,
}

impl DepositWallet {
    pub async fn from_config(
        config_path: &str,
        user_id: &str,
        state_manager: &Arc<StateManager>,
        price_feed: Arc<PriceFeed>,
    ) -> Result<(Self, DepositAddressInfo, StableChain), PulserError> {
        let config_str = fs::read_to_string(config_path)?;
        let config: Config = toml::from_str(&config_str)?;
        let init_result = init_wallet(&config, user_id)?;

        let wallet_path = format!("{}/user_{}", config.data_dir, user_id);
        let initial_addr = Address::from_str(&init_result.deposit_info.address)?.assume_checked();
        let wallet = Self::from_descriptors(
            init_result.external_descriptor.clone(),
            init_result.internal_descriptor.clone(),
            Network::from_str(&config.network)?,
            &config.esplora_url,
            &wallet_path,
            user_id,
            initial_addr.clone(),
            state_manager,
            price_feed.clone(),
        ).await?;

        let deposit_info = DepositAddressInfo {
            address: initial_addr.to_string(),
            user_id: user_id.parse().unwrap_or(0),
            multisig_type: "2-of-3".to_string(),
            participants: vec![
                "user_pubkey".to_string(),
                "lsp_pubkey".to_string(),
                "trustee_pubkey".to_string(),
            ],
            descriptor: init_result.deposit_info.descriptor.clone(),
            path: init_result.deposit_info.path.clone(),
            user_pubkey: init_result.deposit_info.user_pubkey.clone(),
            lsp_pubkey: init_result.deposit_info.lsp_pubkey.clone(),
            trustee_pubkey: init_result.deposit_info.trustee_pubkey.clone(),
        };

        let stable_chain = state_manager.load_or_init_stable_chain(user_id, &wallet_path, initial_addr.to_string()).await?;
        Ok((wallet, deposit_info, stable_chain))
    }

    pub async fn from_descriptors(
        external_descriptor: String,
        internal_descriptor: String,
        network: Network,
        esplora_url: &str,
        wallet_path: &str,
        user_id: &str,
        initial_addr: Address,
        state_manager: &Arc<StateManager>,
        price_feed: Arc<PriceFeed>,
    ) -> Result<Self, PulserError> {
        let blockchain = esplora_client::Builder::new(esplora_url).build_async()?;
        let wallet = Wallet::create(external_descriptor, internal_descriptor)
            .network(network)
            .create_wallet_no_persist()?;

        let mut logged = LOGGED_ADDRESSES.lock().await;
        logged.insert(user_id.to_string(), initial_addr.clone());

        Ok(DepositWallet {
            wallet,
            blockchain,
            network,
            wallet_path: wallet_path.to_string(),
            events: Vec::new(),
            stable_chain: state_manager.load_or_init_stable_chain(user_id, wallet_path, initial_addr.to_string()).await?,
            state_manager: state_manager.clone(),
            price_feed,
        })
    }

    pub async fn get_deposit_address(&mut self) -> Result<Address, PulserError> {
        let addr_info = self.wallet.reveal_next_address(KeychainKind::External);
        if let Some(changeset) = self.wallet.take_staged() {
            self.state_manager.save_changeset(&self.stable_chain.user_id.to_string(), &changeset).await?;
        }
        Ok(addr_info.address)
    }

    pub async fn update_stable_chain(&mut self, price_info: &PriceInfo) -> Result<Vec<Utxo>, PulserError> {
        let logged = LOGGED_ADDRESSES.lock().await;
        let current_addr = logged.get(&self.stable_chain.user_id.to_string())
            .ok_or_else(|| {
                warn!("No logged address for user {}", self.stable_chain.user_id);
                PulserError::WalletError("No logged address".into())
            })?.clone();

        let change_addr = self.wallet.reveal_next_address(KeychainKind::Internal).address;
        let config = Config::from_toml(&toml::from_str(&fs::read_to_string("config/service_config.toml")?)?)?;
        let new_utxos = wallet_utils::sync_and_stabilize_utxos(
            &self.stable_chain.user_id.to_string(),
            &mut self.wallet,
            &self.blockchain,
            &mut self.stable_chain,
            self.price_feed.clone(),
            price_info,
            &current_addr,
            &change_addr,
            &self.state_manager,
            None,
            config.min_confirmations,
        ).await?;

        let total_sats: u64 = self.stable_chain.utxos.iter().map(|u| u.amount).sum();
        self.stable_chain.accumulated_btc = Bitcoin::from_sats(total_sats);
        info!("Updated StableChain for user {}: {} BTC (${:.2}), {} UTXOs",
            self.stable_chain.user_id, self.stable_chain.accumulated_btc.to_btc(), self.stable_chain.stabilized_usd.0, self.stable_chain.utxos.len());
        Ok(self.stable_chain.utxos.clone())
    }

    pub fn list_utxos(&self) -> Result<Vec<Utxo>, PulserError> {
        match Address::from_str(&self.stable_chain.multisig_addr) {
            Ok(address) => self.list_unspent_for_address(&address.assume_checked()),
            Err(e) => {
                warn!("Failed to parse address {}: {}", self.stable_chain.multisig_addr, e);
                Ok(Vec::new())
            }
        }
    }

    pub async fn reveal_new_address(&mut self) -> Result<Address, PulserError> {
        let new_addr = self.wallet.reveal_next_address(KeychainKind::External).address;
        let mut logged = LOGGED_ADDRESSES.lock().await;
        logged.insert(self.stable_chain.user_id.to_string(), new_addr.clone());
        self.stable_chain.old_addresses.push(self.stable_chain.multisig_addr.clone());
        if self.stable_chain.old_addresses.len() > 4 {
            self.stable_chain.old_addresses.remove(0);
        }
        self.stable_chain.multisig_addr = new_addr.to_string();
        self.state_manager.save_stable_chain(&self.stable_chain.user_id.to_string(), &self.stable_chain).await?;
        info!("Revealed new deposit address for user {}: {}", self.stable_chain.user_id, new_addr);
        Ok(new_addr)
    }

    pub fn get_cached_utxos(&self) -> Vec<Utxo> {
        if Utc::now().timestamp() - self.stable_chain.timestamp > 3600 {
            warn!("Cached UTXOs for user {} may be stale (>1h)", self.stable_chain.user_id);
        }
        self.stable_chain.utxos.clone()
    }

    fn list_unspent_for_address(&self, address: &Address) -> Result<Vec<Utxo>, PulserError> {
        let script = address.script_pubkey();
        let utxos = self.wallet.list_unspent()
            .into_iter()
            .filter(|utxo| utxo.txout.script_pubkey == script)
            .map(|utxo| Utxo {
                txid: utxo.outpoint.txid.to_string(),
                vout: utxo.outpoint.vout,
                amount: utxo.txout.value.to_sat(),
                script_pubkey: utxo.txout.script_pubkey.to_hex_string(),
                confirmations: 0,
                height: None,
                usd_value: None,
                spent: utxo.is_spent,
            })
            .collect();
        Ok(utxos)
    }
}
