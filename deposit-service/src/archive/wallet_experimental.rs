use std::fs::{self, OpenOptions};
use std::io::{Write, Read};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration as StdDuration};
use std::os::unix::fs::OpenOptionsExt;
use bdk_esplora::{esplora_client, EsploraAsyncExt};
use bdk_wallet::{Wallet, KeychainKind};
use bdk_chain::{spk_client::SyncRequest, BlockId};
use bdk_wallet::bitcoin::{Network, Txid};
use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::bitcoin::bip32::{DerivationPath, Xpub};
use common::error::PulserError;
use crate::types::{DepositAddressInfo, Utxo, StableChain, Bitcoin};
use crate::keys::create_multisig_descriptor;
use log::{info, warn, debug};
use common::types::USD;
use common::utils::now_timestamp;
use serde::Deserialize;
use bdk_wallet::keys::bip39::{Mnemonic, Language, WordCount};
use bdk_wallet::keys::{DerivableKey, GeneratableKey};
use tokio::time::{timeout, sleep};
use common::price_feed::{fetch_btc_usd_price, get_cached_price};
use lazy_static::lazy_static;

// File lock singleton for concurrency control
lazy_static! {
    static ref FILE_LOCKS: Arc<Mutex<std::collections::HashMap<String, Arc<Mutex<()>>>>> = 
        Arc::new(Mutex::new(std::collections::HashMap::new()));
}

// Constants
const RETRY_MAX: u32 = 3;
const SYNC_TIMEOUT_SECS: u64 = 30;
const MAX_TOTAL_RETRY_SECS: u64 = 120;
const MAX_UTXOS_TO_KEEP: usize = 1000;

// Configuration for wallet initialization
#[derive(Deserialize)]
pub struct Config {
    pub network: String,
    pub esplora_url: String,
    pub fallback_esplora_url: Option<String>,
    pub lsp_pubkey: String,
    pub trustee_pubkey: String,
    pub data_dir: String,
    pub wallet_dir: String,
}

// Main wallet structure
pub struct DepositWallet {
    wallet: Wallet,
    blockchain: esplora_client::AsyncClient,
    fallback_blockchain: Option<esplora_client::AsyncClient>,
    network: Network,
    wallet_path: String,
    events: Vec<common::types::Event>,
    last_sync_time: Option<i64>,
    last_price: Option<f64>,
}

impl DepositWallet {
    // Create from existing wallet
    pub fn from_wallet(
        wallet: Wallet,
        blockchain: esplora_client::AsyncClient,
        fallback_blockchain: Option<esplora_client::AsyncClient>,
        network: Network,
        wallet_path: String,
    ) -> Self {
        DepositWallet {
            wallet,
            blockchain,
            fallback_blockchain,
            network,
            wallet_path,
            events: Vec::new(),
            last_sync_time: None,
            last_price: None,
        }
    }

    // Initialize from configuration
    pub fn from_config(config_path: &str, user_id: &str) -> Result<(Self, DepositAddressInfo, StableChain), PulserError> {
        let config_str = fs::read_to_string(config_path)
            .map_err(|e| PulserError::ConfigError(format!("Failed to read config: {}", e)))?;
        let config: Config = toml::from_str(&config_str)
            .map_err(|e| PulserError::ConfigError(format!("Invalid config format: {}", e)))?;
        let network = Network::from_str(&config.network)
            .map_err(|e| PulserError::ConfigError(format!("Invalid network: {}", e)))?;
        let lsp_xpub = Xpub::from_str(&config.lsp_pubkey)
            .map_err(|e| PulserError::ConfigError(format!("Invalid LSP pubkey: {}", e)))?;
        let trustee_xpub = Xpub::from_str(&config.trustee_pubkey)
            .map_err(|e| PulserError::ConfigError(format!("Invalid trustee pubkey: {}", e)))?;

        let wallet_path = format!("{}/{}", config.data_dir, config.wallet_dir);
        let secp = Secp256k1::new();
        let key_path = format!("{}/secrets/user_{}_key.json", config.data_dir, user_id);

        let mnemonic = if Path::new(&key_path).exists() {
            debug!("Loading existing key for user {}", user_id);
            let key_json = fs::read_to_string(&key_path)
                .map_err(|e| PulserError::StorageError(format!("Failed to read key file: {}", e)))?;
            let key_material: common::types::TaprootKeyMaterial = serde_json::from_str(&key_json)
                .map_err(|e| PulserError::StorageError(format!("Invalid key JSON: {}", e)))?;
            let secret_key = key_material.secret_key
                .ok_or_else(|| PulserError::WalletError("No secret key found".to_string()))?;
            Mnemonic::parse_in(Language::English, secret_key.trim())
                .map_err(|e| PulserError::WalletError(format!("Invalid mnemonic: {:?}", e)))?
        } else {
            info!("Generating new mnemonic for user {}", user_id);
let generated = <Mnemonic as GeneratableKey<miniscript::Tap>>::generate((WordCount::Words12, Language::English))
.map_err(|e| PulserError::WalletError(format!("Failed to generate mnemonic: {:?}", e)))?;
            generated.into_key()
        };

        let external_path = DerivationPath::from_str("m/84'/1'/0'/0/0")
            .map_err(|e| PulserError::WalletError(format!("Invalid derivation path: {}", e)))?;
        let internal_path = DerivationPath::from_str("m/84'/1'/0'/1/0")
            .map_err(|e| PulserError::WalletError(format!("Invalid derivation path: {}", e)))?;
        let unspendable_key_external = "4d54bb9928a0683b7e383de72943b214b0716f58aa54c7ba6bcea2328bc9c768";
        let unspendable_key_internal = "03a34b99f22c790c4e36b2b3c2c35a36db06226e41c692fc82b8b56ac1c540c5";

      let user_xpriv = mnemonic.clone().into_extended_key::<miniscript::Tap>()?
    .into_xprv(network).ok_or(PulserError::WalletError("Failed to convert to xprv".to_string()))?
    .derive_priv(&secp, &external_path)?;
let user_xpub = Xpub::from_priv(&secp, &user_xpriv);
let user_xpriv_internal = mnemonic.into_extended_key::<miniscript::Tap>()?
    .into_xprv(network).ok_or(PulserError::WalletError("Failed to convert to xprv".to_string()))?
    .derive_priv(&secp, &internal_path)?;
            .map_err(|e| PulserError::WalletError(format!("Failed to derive private key: {}", e)))?;
        let user_xpub_internal = Xpub::from_priv(&secp, &user_xpriv_internal);

        let external_descriptor = create_multisig_descriptor(
            &secp, &user_xpub, &lsp_xpub, &trustee_xpub, &external_path, unspendable_key_external, false
        )?;
        let internal_descriptor = create_multisig_descriptor(
            &secp, &user_xpub_internal, &lsp_xpub, &trustee_xpub, &internal_path, unspendable_key_internal, true
        )?;

        let mut wallet = Wallet::create(external_descriptor.clone(), internal_descriptor.clone())
            .network(network)
            .create_wallet_no_persist()
            .map_err(|e| PulserError::WalletError(format!("Failed to create wallet: {:?}", e)))?;
        let blockchain = esplora_client::Builder::new(&config.esplora_url)
            .timeout(30)
            .build_async()
            .map_err(|e| PulserError::NetworkError(format!("Failed to create blockchain client: {}", e)))?;
        let fallback_blockchain = config.fallback_esplora_url.as_ref().map(|url| {
            esplora_client::Builder::new(url)
                .timeout(30)
                .build_async()
                .map_err(|e| PulserError::NetworkError(format!("Failed to create fallback blockchain client: {}", e)))
        }).transpose()?;

        let address_info = wallet.reveal_next_address(KeychainKind::External);
        let multisig_addr = address_info.address.to_string();
        Self::verify_multisig_address(&external_descriptor, &multisig_addr, network)?;
        debug!("Multisig address verified for user {}: {}", user_id, multisig_addr);

        let deposit_info = DepositAddressInfo {
            address: multisig_addr.clone(),
            descriptor: external_descriptor,
            lsp_pubkey: lsp_xpub.to_string(),
            trustee_pubkey: trustee_xpub.to_string(),
            user_pubkey: user_xpub.to_string(),
            path: external_path.to_string(),
        };
        let mut stable_chain = StableChain::load_or_create(user_id, &multisig_addr, &wallet_path)?;
        let raw_price = get_cached_price().unwrap_or(0.0);
        stable_chain.raw_btc_usd = raw_price;

        Ok((
            Self {
                wallet,
                blockchain,
                fallback_blockchain,
                network,
                wallet_path,
                events: Vec::new(),
                last_sync_time: None,
                last_price: Some(raw_price),
            },
            deposit_info,
            stable_chain
        ))
    }

    // File lock helper
    fn get_file_lock(path: &str) -> Arc<Mutex<()>> {
        let mut locks = FILE_LOCKS.lock().unwrap();
        locks.entry(path.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    // Verify multisig address
    fn verify_multisig_address(descriptor: &str, address: &str, network: Network) -> Result<(), PulserError> {
        let mut wallet = Wallet::create(descriptor.to_string(), descriptor.to_string())
            .network(network)
            .create_wallet_no_persist()
            .map_err(|e| PulserError::WalletError(format!("Failed to create verification wallet: {:?}", e)))?;
        let derived_address = wallet.reveal_next_address(KeychainKind::External).address.to_string();
        if derived_address != address {
            return Err(PulserError::WalletError(format!(
                "Multisig address verification failed. Expected: {}, Got: {}", 
                derived_address, address
            )));
        }
        Ok(())
    }

    // Sync wallet with blockchain
    pub async fn sync(&mut self) -> Result<(), PulserError> {
        debug!("Starting wallet sync...");
        let spk_iters = self.wallet.all_unbounded_spk_iters();
        let external_spks: Vec<_> = spk_iters.get(&KeychainKind::External).unwrap().clone()
            .take(5)
            .map(|(_, spk)| spk)
            .collect();
        let internal_spks: Vec<_> = spk_iters.get(&KeychainKind::Internal).unwrap().clone()
            .take(5)
            .map(|(_, spk)| spk)
            .collect();
        let all_spks: Vec<_> = external_spks.into_iter().chain(internal_spks).collect();
        let request = SyncRequest::builder().spks(all_spks).build();

        let start_time = Instant::now();
        let max_duration = StdDuration::from_secs(MAX_TOTAL_RETRY_SECS);
        let timeout_duration = StdDuration::from_secs(SYNC_TIMEOUT_SECS);
        let mut last_error = None;

        for retry in 0..RETRY_MAX {
            if start_time.elapsed() >= max_duration {
                warn!("Exceeded maximum retry time of {}s, aborting sync", MAX_TOTAL_RETRY_SECS);
                break;}
            if retry > 0 {
                let backoff = StdDuration::from_millis(500 * 2u64.pow(retry));
                debug!("Sync retry #{} after {}ms", retry + 1, backoff.as_millis());
                sleep(backoff).await;
                if start_time.elapsed() >= max_duration {
                    warn!("Exceeded maximum retry time of {}s after backoff, aborting sync", MAX_TOTAL_RETRY_SECS);
                    break;
                }
            }
    let request = SyncRequest::builder().spks(all_spks.clone()).build(); // Clone all_spks if needed
            let primary_response = timeout(timeout_duration, self.blockchain.sync(request, 10)).await;
            match primary_response {
                Ok(Ok(response)) => {
                    debug!("Primary blockchain sync successful");
                    return self.apply_sync_response(response).await;
                },
                Ok(Err(e)) => {
                    warn!("Primary blockchain sync failed: {}", e);
                    last_error = Some(PulserError::NetworkError(format!("Primary sync failed: {}", e)));
                },
                Err(_) => {
                    warn!("Primary blockchain sync timed out");
                    last_error = Some(PulserError::NetworkError("Primary sync timed out".to_string()));
                }
            }

            if let Some(fallback) = &self.fallback_blockchain {
                    let request = SyncRequest::builder().spks(all_spks.clone()).build();
                debug!("Trying fallback blockchain sync");
                match timeout(timeout_duration, fallback.sync(request, 10)).await {
                    Ok(Ok(response)) => {
                        info!("Fallback blockchain sync successful");
                        return self.apply_sync_response(response).await;
                    },
                    Ok(Err(e)) => {
                        warn!("Fallback blockchain sync failed: {}", e);
                        last_error = Some(PulserError::NetworkError(format!("Both primary and fallback sync failed: {}", e)));
                    },
                    Err(_) => {
                        warn!("Fallback blockchain sync timed out");
                        last_error = Some(PulserError::NetworkError("Both primary and fallback sync timed out".to_string()));
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| PulserError::NetworkError("All sync attempts failed".to_string())))
    }

    // Monitor deposits
    pub async fn monitor_deposits(&mut self, user_id: &str, stable_chain: &mut StableChain) -> Result<(), PulserError> {
        info!("Syncing deposits for user {} at address {}", user_id, stable_chain.multisig_addr);
        self.sync().await?;
        let mut utxos = self.list_utxos()?;
        self.update_utxo_confirmations(&mut utxos).await?;

        let raw_price = get_cached_price().unwrap_or(0.0);
        stable_chain.raw_btc_usd = raw_price;

        if stable_chain.utxos.len() + utxos.len() > MAX_UTXOS_TO_KEEP {
            self.prune_stable_chain_utxos(stable_chain, utxos.len())?;
        }

        for utxo in utxos {
            if !stable_chain.utxos.iter().any(|u| u.txid == utxo.txid && u.vout == utxo.vout) {
                let btc = Bitcoin::from_sats(utxo.amount);
                let usd_at_deposit = USD(btc.sats as f64 / 100_000_000.0 * raw_price);
                let utxo_with_usd = Utxo {
                    txid: utxo.txid.clone(),
                    vout: utxo.vout,
                    amount: utxo.amount,
                    confirmations: utxo.confirmations,
                    script_pubkey: utxo.script_pubkey,
                    height: utxo.height,
                    usd_value: Some(usd_at_deposit.clone()),
                };
                stable_chain.utxos.push(utxo_with_usd);
                info!("User {}: Deposit detected: {} BTC (${:.2}) at {}", user_id, btc, usd_at_deposit.0, &utxo.txid);
                stable_chain.log_event("deposit", "new_utxo", &format!("{} sats (${:.2}) at {}", utxo.amount, usd_at_deposit.0, utxo.txid));
            } else {
                for existing_utxo in stable_chain.utxos.iter_mut() {
                    if existing_utxo.txid == utxo.txid && existing_utxo.vout == utxo.vout {
                        if existing_utxo.confirmations != utxo.confirmations {
                            debug!("UTXO {}:{} confirmations updated: {} -> {}", utxo.txid, utxo.vout, existing_utxo.confirmations, utxo.confirmations);
                            existing_utxo.confirmations = utxo.confirmations;
                            existing_utxo.height = utxo.height;
                            if existing_utxo.confirmations >= 1 && utxo.confirmations >= 1 {
                                stable_chain.log_event("deposit", "confirmation", &format!("UTXO {}:{} now has {} confirmations", utxo.txid, utxo.vout, utxo.confirmations));
                            }
                        }
                        break;
                    }
                }
            }
        }

        stable_chain.accumulated_btc = Bitcoin::from_sats(stable_chain.utxos.iter().map(|u| u.amount).sum());
        stable_chain.stabilized_usd = USD(stable_chain.utxos.iter()
            .map(|u| match &u.usd_value {
                Some(usd) => usd.0,
                None => u.amount as f64 / 100_000_000.0 * raw_price
            })
            .sum());
        self.save_stable_chain(stable_chain, user_id)?;
        Ok(())
    }

    // Prune UTXOs
    fn prune_stable_chain_utxos(&self, stable_chain: &mut StableChain, new_utxo_count: usize) -> Result<(), PulserError> {
        let total_utxos = stable_chain.utxos.len() + new_utxo_count;
        if total_utxos <= MAX_UTXOS_TO_KEEP {
            return Ok(());
        }
        let to_remove = total_utxos - MAX_UTXOS_TO_KEEP;
        debug!("Pruning {} UTXOs from StableChain to stay within limit of {}", to_remove, MAX_UTXOS_TO_KEEP);

        let mut scored_utxos: Vec<(usize, f64)> = stable_chain.utxos.iter().enumerate()
            .map(|(idx, utxo)| {
                let confirmation_score = if utxo.confirmations == 0 { 1000.0 } else { 0.0 };
                let value_score = utxo.amount as f64 / 100_000.0;
                (idx, confirmation_score + value_score)
            })
            .collect();
        scored_utxos.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let remove_indices: Vec<usize> = scored_utxos.iter().take(to_remove).map(|(idx, _)| *idx).collect();
        let mut removed_utxos = Vec::new();
        for idx in remove_indices.into_iter().rev() {
            if idx < stable_chain.utxos.len() {
                let removed = stable_chain.utxos.remove(idx);
                removed_utxos.push(removed);
            }
        }

        if !removed_utxos.is_empty() {
            let total_pruned_value: u64 = removed_utxos.iter().map(|u| u.amount).sum();
            stable_chain.log_event("maintenance", "utxo_pruning", &format!(
                "Pruned {} UTXOs totaling {} sats to stay within limit", removed_utxos.len(), total_pruned_value
            ));
        }
        Ok(())
    }

    // Save stable chain
    fn save_stable_chain(&self, stable_chain: &StableChain, user_id: &str) -> Result<(), PulserError> {
        let sc_path = Path::new(&stable_chain.sc_dir).join(format!("stable_chain_{}.json", user_id));
        debug!("Saving StableChain to: {:?}", sc_path);
let lock = Self::get_file_lock(&lock_key);
let _guard = lock.lock().unwrap();
fs::create_dir_all(&stable_chain.sc_dir)?;
let temp_path = sc_path.with_extension("tmp");
let json = serde_json::to_string_pretty(stable_chain)?;
let file = {
    let mut opts = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true);
    #[cfg(unix)]
    opts.mode(0o600);
    opts.open(&temp_path)
        .map_err(|e| PulserError::StorageError(format!("Failed to create temp file: {}", e)))?
};
file.write_all(json.as_bytes())
    .map_err(|e| PulserError::StorageError(format!("Failed to write temp file: {}", e)))?;
file.sync_all()
    .map_err(|e| PulserError::StorageError(format!("Failed to sync file: {}", e)))?;
        Ok(())
    }

    // Create for user
    pub fn create_for_user(
        user_id: &str,
        config_path: &str,
        _lsp_pubkey: &str,
        _trustee_pubkey: &str,
        _user_pubkey: &str,
        _network: Network,
    ) -> Result<(Self, DepositAddressInfo, StableChain), PulserError> {
        Self::from_config(config_path, user_id)
    }

    // Apply sync response
    async fn apply_sync_response(&mut self, response: bdk_chain::spk_client::SyncResponse) -> Result<(), PulserError> {
        let result = self.get_blockchain_tip().await;
        match result {
            Ok((chain_tip, chain_tip_hash)) => {
                let mut chain = self.wallet.local_chain().clone();
                for (anchor, txid) in &response.tx_update.anchors {
                    if let Err(e) = chain.insert_block(anchor.block_id) {
                        warn!("Anchor insert failed for {}: {}", txid, e);
                    }
                }
                if let Err(e) = chain.insert_block(BlockId { height: chain_tip, hash: chain_tip_hash }) {
                    return Err(PulserError::WalletError(format!("Tip insert failed: {}", e)));
                }
                let update = bdk_wallet::Update {
                    tx_update: response.tx_update,
                    chain: Some(chain.tip()),
                    last_active_indices: Default::default(),
                };
                self.wallet.apply_update(update)
                    .map_err(|e| PulserError::WalletError(format!("Failed to apply update: {}", e)))?;
                self.last_sync_time = Some(now_timestamp());
                Ok(())
            },
            Err(e) => {
                warn!("Failed to get blockchain tip: {}. Applying partial update.", e);
                let update = bdk_wallet::Update {
                    tx_update: response.tx_update,
                    chain: None,
                    last_active_indices: Default::default(),
                };
                self.wallet.apply_update(update)
                    .map_err(|e| PulserError::WalletError(format!("Failed to apply partial update: {}", e)))?;
                self.last_sync_time = Some(now_timestamp());
                warn!("Applied partial update without blockchain tip");
                Ok(())
            }
        }
    }

async fn get_blockchain_tip(&self) -> Result<(u32, bitcoin::BlockHash), PulserError> {
    let start_time = Instant::now();
    let max_duration = StdDuration::from_secs(MAX_TOTAL_RETRY_SECS);
    let timeout_duration = StdDuration::from_secs(SYNC_TIMEOUT_SECS);

    for retry in 0..RETRY_MAX {
        if start_time.elapsed() >= max_duration {
            warn!("Exceeded maximum retry time of {}s for blockchain tip", MAX_TOTAL_RETRY_SECS);
            break;
        }
        if retry > 0 {
            let backoff = StdDuration::from_millis(500 * 2u64.pow(retry));
            sleep(backoff).await;
            if start_time.elapsed() >= max_duration {
                break;
            }
        }

        // Try primary client
        if let Ok(height) = timeout(timeout_duration, self.blockchain.get_height()).await {
            match height {
                Ok(h) => {
                    if let Ok(hash) = timeout(timeout_duration, self.blockchain.get_block_hash(h)).await {
                        match hash {
                            Ok(hash) => return Ok((h, hash)),
                            Err(e) => warn!("Failed to get block hash from primary: {}", e),
                        }
                    } else {
                        warn!("Timeout getting block hash from primary");
                    }
                },
                Err(e) => warn!("Failed to get height from primary: {}", e),
            }
        } else {
            warn!("Timeout getting height from primary");
        }

        // Try fallback if available
        if let Some(fallback) = &self.fallback_blockchain {
            if let Ok(height) = timeout(timeout_duration, fallback.get_height()).await {
                match height {
                    Ok(h) => {
                        if let Ok(hash) = timeout(timeout_duration, fallback.get_block_hash(h)).await {
                            match hash {
                                Ok(hash) => return Ok((h, hash)),
                                Err(e) => warn!("Failed to get block hash from fallback: {}", e),
                            }
                        } else {
                            warn!("Timeout getting block hash from fallback");
                        }
                    },
                    Err(e) => warn!("Failed to get height from fallback: {}", e),
                }
            } else {
                warn!("Timeout getting height from fallback");
            }
        }
    }

    Err(PulserError::NetworkError("Failed to get blockchain tip after retries".to_string()))
}

    // List UTXOs
    pub fn list_utxos(&self) -> Result<Vec<Utxo>, PulserError> {
        let utxos: Vec<Utxo> = self.wallet.list_unspent().into_iter().map(|utxo| {
            debug!("UTXO: {}:{} = {} sats", utxo.outpoint.txid, utxo.outpoint.vout, utxo.txout.value.to_sat());
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
        info!("Found {} UTXOs", utxos.len());
        Ok(utxos)
    }

    // Update UTXO confirmations
    pub async fn update_utxo_confirmations(&self, utxos: &mut Vec<Utxo>) -> Result<(), PulserError> {
        if utxos.is_empty() {
            return Ok(());
        }
        let start_time = Instant::now();
        let max_duration = StdDuration::from_secs(MAX_TOTAL_RETRY_SECS);
        let timeout_duration = StdDuration::from_secs(SYNC_TIMEOUT_SECS);

        let mut current_height = 0;
        let mut height_found = false;
        for retry in 0..RETRY_MAX {
            if start_time.elapsed() >= max_duration {
                warn!("Exceeded maximum retry time of {}s when getting height", MAX_TOTAL_RETRY_SECS);
                break;
            }
            if retry > 0 {
                let backoff = StdDuration::from_millis(500 * 2u64.pow(retry));
                sleep(backoff).await;
                if start_time.elapsed() >= max_duration {
                    break;
                }
            }
            match self.blockchain.get_height().await {
                Ok(height) => {
                    current_height = height;
                    height_found = true;
                    break;
                },
                Err(e) => {
                    warn!("Failed to get blockchain height: {}, trying fallback", e);
                    if let Some(fallback) = &self.fallback_blockchain {
                        match fallback.get_height().await {
                            Ok(height) => {
                                current_height = height;
                                height_found = true;
                                break;
                            },
                            Err(e) => warn!("Fallback get_height failed: {}", e),
                        }
                    }
                }
            }
        }
        if !height_found {
            return Err(PulserError::NetworkError("Failed to get blockchain height after retries".to_string()));
        }

        let mut updated_count = 0;
        for utxo in utxos.iter_mut() {
            if let Some(height) = utxo.height {
                if height > 0 && height <= current_height {
                    utxo.confirmations = current_height - height + 1;
                    updated_count += 1;
                } else {
                    utxo.confirmations = 0;
                }
                continue;
            }

            let txid = Txid::from_str(&utxo.txid)
                .map_err(|e| PulserError::WalletError(format!("Invalid txid {}: {}", utxo.txid, e)))?;
            if start_time.elapsed() >= max_duration {
                warn!("Exceeded maximum retry time of {}s when processing UTXOs, processed {} of {}", 
                      MAX_TOTAL_RETRY_SECS, updated_count, utxos.len());
                break;
            }

            let mut tx_found = false;
            for tx_retry in 0..RETRY_MAX {
                if start_time.elapsed() >= max_duration {
                    break;
                }
                if tx_retry > 0 {
                    let backoff = StdDuration::from_millis(500 * 2u64.pow(tx_retry));
                    sleep(backoff).await;
                    if start_time.elapsed() >= max_duration {
                        break;
                    }
                }
              match timeout(timeout_duration, self.blockchain.get_tx_status(&txid)).await {
    Ok(Ok(status)) => {
        utxo.height = status.block_height;
                        if let Some(height) = utxo.height {
                            if height > 0 && height <= current_height {
                                utxo.confirmations = current_height - height + 1;
                            } else {
                                utxo.confirmations = 0;
                            }
                        } else {
                            utxo.confirmations = 0;
                        }
                        tx_found = true;
                        updated_count += 1;
                        break;
                    },
                    Ok(Err(e)) => {
                        warn!("Failed to get tx {} from primary: {}", txid, e);
                        if let Some(fallback) = &self.fallback_blockchain {
                            match timeout(timeout_duration, fallback.get_tx(&txid)).await {
                                Ok(Ok(status)) => {
                                    utxo.height = status.block_height;
                                    if let Some(height) = utxo.height {
                                        if height > 0 && height <= current_height {
                                            utxo.confirmations = current_height - height + 1;
                                        } else {
                                            utxo.confirmations = 0;
                                        }
                                    } else {
                                        utxo.confirmations = 0;
                                    }
                                    tx_found = true;
                                    updated_count += 1;
                                    break;
                                },
                                Ok(Err(e)) => warn!("Failed to get tx {} from fallback: {}", txid, e),
                                Err(_) => warn!("Fallback get_tx timed out for {}", txid),
                            }
                        }
                    },
                    Err(_) => {
                        warn!("Timeout getting tx {} from primary", txid);
                        if let Some(fallback) = &self.fallback_blockchain {
                            match timeout(timeout_duration, fallback.get_tx(&txid)).await {
                                Ok(Ok(status)) => {
                                    utxo.height = status.block_height;
                                    if let Some(height) = utxo.height {
                                        if height > 0 && height <= current_height {
                                            utxo.confirmations = current_height - height + 1;
                                        } else {
                                            utxo.confirmations = 0;
                                        }
                                    } else {
                                        utxo.confirmations = 0;
                                    }
                                    tx_found = true;
                                    updated_count += 1;
                                    break;
                                },
                                Ok(Err(e)) => warn!("Failed to get tx {} from fallback: {}", txid, e),
                                Err(_) => warn!("Fallback get_tx timed out for {}", txid),
                            }
                        }
                    }
                }
            }
            if !tx_found {
                warn!("Failed to get transaction data for UTXO {}:{} after {} retries", utxo.txid, utxo.vout, RETRY_MAX);
            }
        }
        debug!("Updated confirmations for {}/{} UTXOs", updated_count, utxos.len());
        Ok(())
    }
}
