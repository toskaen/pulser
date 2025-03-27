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
use crate::types::{StableChain, Bitcoin, DepositAddressInfo};
use crate::storage::StateManager;
use crate::wallet_init::{Config, init_wallet};
use log::{info, warn, debug};
use chrono::Utc;
use tokio::sync::Mutex;
use tokio::time::sleep;
use common::price_feed::PriceFeed;
use crate::types::UtxoInfo;

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

        let wallet_path = format!("{}/user_{}/multisig", config.data_dir, user_id);
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
            price_feed,
        ).await?;

        let deposit_info = DepositAddressInfo {
            address: initial_addr.to_string(),
            descriptor: init_result.deposit_info.descriptor,
            path: init_result.deposit_info.path,
            user_pubkey: init_result.public_data["user_pubkey"].as_str().unwrap_or("").to_string(),
            lsp_pubkey: init_result.public_data["lsp_pubkey"].as_str().unwrap_or("").to_string(),
            trustee_pubkey: init_result.public_data["trustee_pubkey"].as_str().unwrap_or("").to_string(),
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

    pub async fn update_stable_chain(&mut self, price_info: &PriceInfo, price_feed: &PriceFeed) -> Result<Vec<Utxo>, PulserError> {
        let logged = LOGGED_ADDRESSES.lock().await;
        let current_addr = logged.get(&self.stable_chain.user_id.to_string())
            .ok_or(PulserError::WalletError("No logged address".into()))?;
        let utxos = self.check_address(current_addr, price_info, price_feed).await?;
        
        for utxo in &utxos {
            let is_external = utxo.script_pubkey == current_addr.script_pubkey().to_hex_string();
            self.stable_chain.history.push(UtxoInfo {
                txid: utxo.txid.clone(),
                vout: utxo.vout,
                amount_sat: utxo.amount,
                address: current_addr.to_string(),
                keychain: if is_external { "External".to_string() } else { "Internal".to_string() },
                timestamp: Utc::now().timestamp() as u64,
                confirmations: utxo.confirmations,
                participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
                stable_value_usd: utxo.usd_value.as_ref().unwrap_or(&USD(0.0)).0,
                spendable: utxo.confirmations >= 1,
                derivation_path: if is_external { "m/86'/1'/0'/0/0".to_string() } else { "m/86'/1'/0'/1/0".to_string() },
                spent: false,
            });
        }

        self.stable_chain.utxos = utxos.clone();
        self.stable_chain.accumulated_btc = Bitcoin { sats: self.stable_chain.utxos.iter().map(|u| u.amount).sum() };
        let deribit_price = price_feed.get_deribit_price().await.unwrap_or(price_info.raw_btc_usd);
        self.stable_chain.stabilized_usd = USD((self.stable_chain.accumulated_btc.to_btc()) * deribit_price);
        self.stable_chain.raw_btc_usd = price_info.raw_btc_usd;
        self.stable_chain.timestamp = Utc::now().timestamp();
        self.stable_chain.formatted_datetime = Utc::now().to_rfc3339();
        self.stable_chain.prices = price_info.price_feeds.clone();
        self.stable_chain.multisig_addr = current_addr.to_string();
        self.state_manager.save_stable_chain(&self.stable_chain.user_id.to_string(), &self.stable_chain)
            .await
            .map_err(|e| { warn!("Save stable chain failed: {}", e); e })?;
        info!("Updated stable chain for user {}: {} BTC", self.stable_chain.user_id, self.stable_chain.accumulated_btc.to_btc());
        Ok(utxos)
    }

pub async fn check_address(
    &mut self,
    address: &Address,
    price_info: &PriceInfo,
    price_feed: &PriceFeed,
) -> Result<Vec<Utxo>, PulserError> {
    let sync_height = self.wallet.latest_checkpoint().height();
    debug!("Checking address {} with sync height {}", address, sync_height);

    let mut attempts = 0;
    const MAX_ATTEMPTS: u32 = 3;
    let script = address.script_pubkey();

    let mut utxos = self.list_unspent_for_address(address)?;
if utxos.is_empty() {
        debug!("No UTXOs, sync height {} too low, resyncing", sync_height);
        let mut current_blockchain = esplora_client::Builder::new("https://blockstream.info/testnet/api")
            .build_async()?;
        while attempts < MAX_ATTEMPTS {
            let request = self.wallet.start_full_scan();
            match current_blockchain.full_scan(request, 10, 5).await {
                Ok(update) => {
                    self.wallet.apply_update(update)?;
                    debug!("Re-synced to height {}", self.wallet.latest_checkpoint().height());
                    utxos = self.list_unspent_for_address(address)?;
                    break;
                }
                Err(e) => {
                    attempts += 1;
                    warn!("Sync failed for {} (attempt {}/{}): {}", address, attempts, MAX_ATTEMPTS, e);
                    if attempts == 1 {
                        current_blockchain = esplora_client::Builder::new("https://mempool.space/testnet/api")
                            .build_async()?;
                        info!("Switched to fallback Esplora");
                    }
                    if attempts == MAX_ATTEMPTS {
                        warn!("Exhausted retries, using cached state");
                        break;
                    }
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }

    let current_height = self.blockchain.get_height().await.unwrap_or(sync_height);
    let deribit_price = price_feed.get_deribit_price().await.unwrap_or(price_info.raw_btc_usd);

    let validated_utxos = utxos.into_iter().map(|mut utxo| {
        utxo.confirmations = match self.wallet.get_tx(Txid::from_str(&utxo.txid).unwrap()).map(|tx| tx.chain_position) {
            Some(ChainPosition::Confirmed { anchor, .. }) => current_height.saturating_sub(anchor.block_id.height) + 1,
            _ => 0,
        };
        utxo.usd_value = Some(USD((utxo.amount as f64 / 100_000_000.0) * deribit_price));
        utxo
    }).collect();

    Ok(validated_utxos)
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
                confirmations: 0, // Updated in check_address
                height: None,
                usd_value: None, // Updated in check_address
            })
            .collect();
        Ok(utxos)
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
}
