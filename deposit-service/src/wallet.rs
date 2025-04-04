use std::fs;
use std::str::FromStr;
use std::collections::HashMap;
use std::sync::Arc;
use bdk_wallet::{Wallet, KeychainKind, ChangeSet};
use bdk_esplora::esplora_client;
use bdk_wallet::bitcoin::{Network, Address};
use bdk_wallet::bitcoin::bip32::{Xpriv, Xpub};
use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::keys::bip39::{Mnemonic, Language, WordCount};
use bdk_wallet::keys::{DerivableKey, GeneratableKey, ExtendedKey, GeneratedKey};
use common::error::PulserError;
use common::types::{PriceInfo, UtxoInfo, Event, DepositAddressInfo};
use crate::init_pulser_wallet::{init_pulser_wallet, WalletInitResult};
use crate::apply_changeset::apply_changeset;
use log::{info, warn, debug};
use chrono::Utc;
use tokio::sync::Mutex;
use common::price_feed::PriceFeed;
use common::StateManager;
use common::{StableChain, Bitcoin};
use common::wallet_sync;
use crate::config::Config;
use bdk_wallet::chain::local_chain::ChangeSet as LocalChainChangeSet;
use std::collections::BTreeMap;
use common::price_feed::PriceFeedExtensions;

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
    pub config: Arc<Config>,
}

impl DepositWallet {
    pub async fn from_config(
        config: &Config,
        user_id: &str,
        state_manager: &Arc<StateManager>,
        price_feed: Arc<PriceFeed>,
    ) -> Result<(Self, DepositAddressInfo, StableChain, Option<String>), PulserError> {
        let blockchain = esplora_client::Builder::new(&config.esplora_url).build_async()?;
        let tip = blockchain.get_height().await?;
        let tip_hash = blockchain.get_block_hash(tip).await?;
        let genesis_hash = blockchain.get_block_hash(0).await?; // Fetch genesis block for chain connection

        // Build an initial chain that includes height 0 to avoid chain connection errors
        let mut blocks = BTreeMap::new();
        blocks.insert(0, Some(genesis_hash)); // Include genesis block
        blocks.insert(tip, Some(tip_hash));   // Include current tip

        let initial_changeset = ChangeSet {
            descriptor: None,
            change_descriptor: None,
            network: Some(Network::from_str(&config.network)?),
            local_chain: LocalChainChangeSet { blocks },
            tx_graph: Default::default(),
            indexer: Default::default(),
        };

        let mut logged = LOGGED_ADDRESSES.lock().await;
        let is_new_user = !logged.contains_key(user_id);

        let (mnemonic, init_result) = if is_new_user {
            let secp = Secp256k1::new();
            // Generate mnemonic and derive xpub securely
            let mnemonic: GeneratedKey<Mnemonic, miniscript::Tap> = Mnemonic::generate((WordCount::Words12, Language::English))
                .map_err(|e| PulserError::WalletError(format!("Mnemonic generation failed: {:?}", e)))?;
            let extended_key: ExtendedKey<miniscript::Tap> = mnemonic.clone()
                .into_extended_key()
                .map_err(|e| PulserError::WalletError(format!("Failed to get extended key: {:?}", e)))?;
            let xpriv = extended_key.into_xprv(Network::from_str(&config.network)?)
                .ok_or(PulserError::WalletError("Failed to get xprv".into()))?;
            let xpub = Xpub::from_priv(&secp, &xpriv); // Correctly derive Xpub from Xpriv
            let user_xpub = xpub.to_string();
            let init_result = init_pulser_wallet(config, user_id, Some(user_xpub))?;
            (Some(mnemonic), init_result)
        } else {
            let init_result = init_pulser_wallet(config, user_id, None)?;
            (None, init_result)
        };

        let (wallet, deposit_info, _unused) = apply_changeset(
            config,
            user_id,
            init_result.external_descriptor,
            init_result.internal_descriptor,
            init_result.user_xpub.clone(),
            Some(initial_changeset),
        )?;

        let wallet_path = format!("{}/user_{}", config.data_dir, user_id);
        let initial_addr = Address::from_str(&deposit_info.address)?.assume_checked();
        logged.insert(user_id.to_string(), initial_addr.clone());

        let stable_chain = state_manager
            .load_or_init_stable_chain(user_id, &wallet_path, initial_addr.to_string())
            .await?;

let recovery_doc = mnemonic.map(|m| {
    format!(
        "# PULSER WALLET RECOVERY DOCUMENT\n\n\
         IMPORTANT: KEEP THIS DOCUMENT SECURE - ONE-TIME RECOVERY TOOL\n\n\
         User ID: {}\n\
         Network: {}\n\
         Created: {}\n\
         ## Wallet Information\n\
         Wallet Descriptor: {}\n\n\
         ## Participants\n\
         User Public Key: {}\n\
         LSP Public Key: {}\n\
         Trustee Public Key: {}\n\n\
         ## Recovery Seed\n\
         {}\n\n\
         ## Recovery Instructions\n\
         1. Use this descriptor with a compatible wallet (BlueWallet, Sparrow, Electrum)\n\
         2. Import the descriptor and seed if available\n\
         3. Requires 2-of-3 signatures to spend\n\
         4. Contact support if needed\n\
         5. This is a one-time recovery tool; store it securely and do not share",
        user_id,
        config.network,
        Utc::now().timestamp(),
        deposit_info.descriptor.as_ref().unwrap_or(&"".to_string()),
        deposit_info.user_pubkey,
        config.lsp_pubkey,
        config.trustee_pubkey,
        m.to_string() // Added corresponding placeholder {}
    )
});

        let network = Network::from_str(&config.network)?;
        let deposit_wallet = DepositWallet {
            wallet,
            blockchain,
            network,
            wallet_path,
            events: Vec::new(),
            stable_chain,
            state_manager: state_manager.clone(),
            price_feed,
            config: Arc::new(config.clone()),
        };

        let stable_chain_clone = deposit_wallet.stable_chain.clone();
        Ok((deposit_wallet, deposit_info, stable_chain_clone, recovery_doc))
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
        config: &Config,
    ) -> Result<Self, PulserError> {
        let blockchain = esplora_client::Builder::new(esplora_url).build_async()?;
        let wallet = Wallet::create(external_descriptor, internal_descriptor)
            .network(network)
            .create_wallet_no_persist()?;

        let mut logged = LOGGED_ADDRESSES.lock().await;
        logged.insert(user_id.to_string(), initial_addr.clone());

        let stable_chain = state_manager
            .load_or_init_stable_chain(user_id, wallet_path, initial_addr.to_string())
            .await?;

        Ok(DepositWallet {
            wallet,
            blockchain,
            network,
            wallet_path: wallet_path.to_string(),
            events: Vec::new(),
            stable_chain,
            state_manager: state_manager.clone(),
            price_feed,
            config: Arc::new(config.clone()),
        })
    }

    pub async fn get_deposit_address(&mut self) -> Result<Address, PulserError> {
        let addr_info = self.wallet.reveal_next_address(KeychainKind::External);
        if let Some(changeset) = self.wallet.take_staged() {
            self.state_manager
                .save_changeset(&self.stable_chain.user_id.to_string(), &changeset)
                .await?;
        }
        Ok(addr_info.address)
    }

    pub async fn get_change_address(&mut self) -> Result<Address, PulserError> {
        let addr_info = self.wallet.reveal_next_address(KeychainKind::Internal);
        if let Some(changeset) = self.wallet.take_staged() {
            self.state_manager
                .save_changeset(&self.stable_chain.user_id.to_string(), &changeset)
                .await?;
        }
        Ok(addr_info.address)
    }

pub async fn update_stable_chain(&mut self, price_info: &PriceInfo) -> Result<Vec<UtxoInfo>, PulserError> {
    let logged = LOGGED_ADDRESSES.lock().await;
    let current_addr = logged
        .get(&self.stable_chain.user_id.to_string())
        .ok_or_else(|| {
            warn!("No logged address for user {}", self.stable_chain.user_id);
            PulserError::WalletError("No logged address".into())
        })?
        .clone();

    let change_addr = self.get_change_address().await?;

    let new_utxos = wallet_sync::sync_and_stabilize_utxos(
        &self.stable_chain.user_id.to_string(),
        &mut self.wallet,
        &self.blockchain,
        &mut self.stable_chain,
        self.price_feed.clone(), // Pass the price_feed directly
        price_info,              // Pass the price_info
        &current_addr,
        &change_addr,
        &self.state_manager,
        self.config.min_confirmations,
    )
    .await?;

        self.stable_chain.timestamp = chrono::Utc::now().timestamp();
        self.stable_chain.formatted_datetime = chrono::Utc::now().to_rfc3339();
        info!(
            "Updated StableChain for user {}: {} BTC (${:.2}), {} UTXOs",
            self.stable_chain.user_id,
            self.stable_chain.accumulated_btc.to_btc(),
            self.stable_chain.stabilized_usd,
            self.stable_chain.utxos.len()
        );
        Ok(new_utxos)
    }

    pub fn list_utxos(&self) -> Result<Vec<UtxoInfo>, PulserError> {
        match Address::from_str(&self.stable_chain.multisig_addr) {
            Ok(address) => self.list_unspent_for_address(&address.assume_checked()),
            Err(e) => {
                warn!(
                    "Failed to parse address {}: {}",
                    self.stable_chain.multisig_addr, e
                );
                Ok(Vec::new())
            }
        }
    }

    pub async fn reveal_new_address(&mut self) -> Result<Address, PulserError> {
        let new_addr = self.wallet.reveal_next_address(KeychainKind::External).address;
        let mut logged = LOGGED_ADDRESSES.lock().await;
        logged.insert(self.stable_chain.user_id.to_string(), new_addr.clone());

        let old_addr = self.stable_chain.multisig_addr.clone();
        self.stable_chain.old_addresses.push(old_addr.clone());
        self.stable_chain.multisig_addr = new_addr.to_string();
        self.stable_chain.log_change(
            "address_change",
            0.0,
            0.0,
            "deposit-service",
            Some(format!("Old: {}, New: {}", old_addr, new_addr.to_string())),
        );

        self.stable_chain.timestamp = chrono::Utc::now().timestamp();
        self.stable_chain.formatted_datetime = chrono::Utc::now().to_rfc3339();

        self.state_manager
            .save_stable_chain(&self.stable_chain.user_id.to_string(), &self.stable_chain)
            .await?;
        if let Some(changeset) = self.wallet.take_staged() {
            self.state_manager
                .save_changeset(&self.stable_chain.user_id.to_string(), &changeset)
                .await?;
        }

        info!(
            "Revealed new deposit address for user {}: {}",
            self.stable_chain.user_id, new_addr
        );
        Ok(new_addr)
    }

    pub fn get_cached_utxos(&self) -> Vec<UtxoInfo> {
        if Utc::now().timestamp() - self.stable_chain.timestamp > 3600 {
            warn!(
                "Cached UTXOs for user {} may be stale (>1h)",
                self.stable_chain.user_id
            );
        }
        self.stable_chain
            .utxos
            .iter()
            .map(|utxo| UtxoInfo {
                txid: utxo.txid.clone(),
                vout: utxo.vout,
                amount_sat: utxo.amount,
                address: self.stable_chain.multisig_addr.clone(),
                confirmations: utxo.confirmations,
                spent: utxo.spent,
                stable_value_usd: utxo.usd_value.as_ref().map_or(0.0, |usd| usd.0),
                keychain: "External".to_string(),
                timestamp: chrono::Utc::now().timestamp() as u64,
                participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
                spendable: utxo.confirmations >= 1,
                derivation_path: "".to_string(),
            })
            .collect()
    }

    fn list_unspent_for_address(&self, address: &Address) -> Result<Vec<UtxoInfo>, PulserError> {
        let script = address.script_pubkey();
        let utxos = self
            .wallet
            .list_unspent()
            .into_iter()
            .filter(|utxo| utxo.txout.script_pubkey == script)
            .map(|utxo| {
                let confirmations = match utxo.chain_position {
                    bdk_chain::ChainPosition::Confirmed { anchor, .. } => self
                        .wallet
                        .latest_checkpoint()
                        .height()
                        .saturating_sub(anchor.block_id.height)
                        + 1,
                    bdk_chain::ChainPosition::Unconfirmed { .. } => 0,
                };
                UtxoInfo {
                    txid: utxo.outpoint.txid.to_string(),
                    vout: utxo.outpoint.vout,
                    amount_sat: utxo.txout.value.to_sat(),
                    address: address.to_string(),
                    confirmations,
                    spent: utxo.is_spent,
                    stable_value_usd: 0.0,
                    keychain: "External".to_string(),
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
                    spendable: confirmations >= 1,
                    derivation_path: "".to_string(),
                }
            })
            .collect();
        Ok(utxos)
    }
}
