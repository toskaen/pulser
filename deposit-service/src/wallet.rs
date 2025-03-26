// deposit-service/src/wallet.rs
use std::fs;
use std::str::FromStr;
use std::collections::HashMap;
use bdk_wallet::{Wallet, KeychainKind};
use bdk_esplora::esplora_client;
use bdk_wallet::bitcoin::{Network, ScriptBuf, Address};
use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::bitcoin::bip32::{DerivationPath, Xpub, Fingerprint};
use bdk_wallet::keys::bip39::{Mnemonic, Language, WordCount};
use bdk_wallet::keys::{GeneratableKey, DerivableKey, GeneratedKey, ExtendedKey};
use bdk_wallet::miniscript::descriptor::{DescriptorXKey, Wildcard};
use bdk_wallet::keys::DescriptorPublicKey;
use common::error::PulserError;
use common::types::{USD, Event, PriceInfo, Utxo};
use crate::types::{StableChain, UtxoInfo, Bitcoin, DepositAddressInfo};
use log::{info, warn};
use serde_json;
use chrono::Utc;
use reqwest::Client;
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
    pub async fn from_config(config_path: &str, user_id: &str) -> Result<(Self, DepositAddressInfo, StableChain), PulserError> {
        let config_str = fs::read_to_string(config_path)?;
        let config: Config = toml::from_str(&config_str)?;
        let network = Network::from_str(&config.network)?;
        let blockchain = esplora_client::Builder::new(&config.esplora_url).build_async()?;
        let wallet_path = format!("{}/user_{}/multisig", config.data_dir, user_id);
        let public_path = format!("{}/user_{}/user_{}_public.json", config.data_dir, user_id, user_id);
        let key_path = format!("{}/user_{}/user_{}_recovery.txt", config.data_dir, user_id, user_id);

        fs::create_dir_all(format!("{}/user_{}", config.data_dir, user_id))?;

        let secp = Secp256k1::new();
        let is_preloaded = std::path::Path::new(&public_path).exists();
        let public_data: serde_json::Value = if is_preloaded {
            let public_str = fs::read_to_string(&public_path)?;
            serde_json::from_str(&public_str)?
        } else {
            serde_json::json!({})
        };

        let (external_descriptor, internal_descriptor, user_xpub_str, mnemonic_opt) = if is_preloaded {
            let external_desc = public_data["wallet_descriptor"].as_str()
                .ok_or(PulserError::WalletError("Missing wallet_descriptor".into()))?;
            let internal_desc = public_data["internal_descriptor"].as_str()
                .ok_or(PulserError::WalletError("Missing internal_descriptor".into()))?;
            let user_pubkey = public_data["user_pubkey"].as_str()
                .ok_or(PulserError::WalletError("Missing user_pubkey".into()))?;
            (external_desc.to_string(), internal_desc.to_string(), user_pubkey.to_string(), None)
        } else {
            let lsp_xpub = Xpub::from_str(&config.lsp_pubkey)?;
            let trustee_xpub = Xpub::from_str(&config.trustee_pubkey)?;
            let external_path = DerivationPath::from_str("m/84'/1'/0'/0/0")?;
            let internal_path = DerivationPath::from_str("m/84'/1'/0'/1/0")?;

            let mnemonic = if std::path::Path::new(&key_path).exists() {
                let recovery_txt = fs::read_to_string(&key_path)?;
                let seed_line = recovery_txt.lines()
                    .find(|line| line.starts_with("weather") || line.contains(" "))
                    .ok_or(PulserError::WalletError("No seed found".into()))?;
                Mnemonic::parse_in(Language::English, seed_line.trim())?
            } else {
                let generated: GeneratedKey<Mnemonic, miniscript::Tap> = Mnemonic::generate((WordCount::Words12, Language::English))
                    .map_err(|e| PulserError::WalletError(format!("Mnemonic generation failed: {:?}", e)))?;
                generated.into_key()
            };

            let user_extended_key: ExtendedKey<miniscript::Tap> = mnemonic.clone().into_extended_key()?;
            let user_xpriv = user_extended_key.into_xprv(network)
                .ok_or(PulserError::WalletError("Failed to get xprv".into()))?
                .derive_priv(&secp, &external_path)?;
            let user_xpub = Xpub::from_priv(&secp, &user_xpriv);

            let user_pubkey = DescriptorPublicKey::XPub(DescriptorXKey {
                origin: Some((Fingerprint::default(), external_path.clone())),
                xkey: user_xpub,
                derivation_path: DerivationPath::master(),
                wildcard: Wildcard::Unhardened,
            });
            let lsp_pubkey = DescriptorPublicKey::XPub(DescriptorXKey {
                origin: Some((Fingerprint::default(), external_path.clone())),
                xkey: lsp_xpub,
                derivation_path: DerivationPath::master(),
                wildcard: Wildcard::Unhardened,
            });
            let trustee_pubkey = DescriptorPublicKey::XPub(DescriptorXKey {
                origin: Some((Fingerprint::default(), external_path)),
                xkey: trustee_xpub,
                derivation_path: DerivationPath::master(),
                wildcard: Wildcard::Unhardened,
            });

            let unspendable_key = "4d54bb9928a0683b7e383de72943b214b0716f58aa54c7ba6bcea2328bc9c768";
            let external_descriptor = format!(
                "tr({},multi_a(2,{},{},{}))",
                unspendable_key, lsp_pubkey, trustee_pubkey, user_pubkey
            );
            let internal_descriptor = format!(
                "tr({},multi_a(2,{},{},{}))",
                unspendable_key, lsp_pubkey, trustee_pubkey, user_pubkey
            );

            (external_descriptor, internal_descriptor, user_xpub.to_string(), Some(mnemonic))
        };

        let mut wallet = Wallet::create(external_descriptor.clone(), internal_descriptor.clone())
            .network(network)
            .create_wallet_no_persist()?;

        let initial_addr = wallet.reveal_next_address(KeychainKind::External).address;
        info!("Generated initial deposit address for user {}: {}", user_id, initial_addr);
        let deposit_info = DepositAddressInfo {
            address: initial_addr.to_string(),
            descriptor: external_descriptor.clone(),
            path: "m/84'/1'/0'/0/0".to_string(),
            user_pubkey: user_xpub_str.clone(),
            lsp_pubkey: config.lsp_pubkey.clone(),
            trustee_pubkey: config.trustee_pubkey.clone(),
        };
        let mut logged = LOGGED_ADDRESSES.lock().await;
        logged.insert(user_id.to_string(), initial_addr.clone());

        let stable_chain_path = format!("{}/stable_chain_{}.json", wallet_path, user_id);
        let now = Utc::now();
        let stable_chain = if std::path::Path::new(&stable_chain_path).exists() {
            let sc_json = fs::read_to_string(&stable_chain_path)?;
            serde_json::from_str(&sc_json)?
        } else {
            StableChain {
                user_id: user_id.parse::<u32>().map_err(|e| PulserError::WalletError(format!("Invalid user_id: {}", e)))?,
                is_stable_receiver: false,
                counterparty: "unknown".to_string(),
                accumulated_btc: Bitcoin { sats: 0 },
                stabilized_usd: USD(0.0),
                timestamp: now.timestamp(),
                formatted_datetime: now.to_rfc3339(),
                sc_dir: wallet_path.clone(),
                raw_btc_usd: 0.0,
                prices: HashMap::new(),
                multisig_addr: initial_addr.to_string(),
                utxos: Vec::new(),
                pending_sweep_txid: None,
                events: Vec::new(),
                total_withdrawn_usd: 0.0,
                expected_usd: USD(0.0),
                hedge_position_id: None,
                pending_channel_id: None,
                shorts: Vec::new(),
                hedge_ready: false,
                last_hedge_time: 0,
                short_reduction_amount: None,
            }
        };

        if !is_preloaded {
            if let Some(mnemonic) = mnemonic_opt {
                let public_data = serde_json::json!({
                    "wallet_descriptor": &external_descriptor,
                    "internal_descriptor": &internal_descriptor,
                    "lsp_pubkey": &config.lsp_pubkey,
                    "trustee_pubkey": &config.trustee_pubkey,
                    "user_pubkey": user_xpub_str,
                    "user_id": stable_chain.user_id
                });
                fs::write(&public_path, serde_json::to_string_pretty(&public_data)?)?;
                info!("Public data saved to {}", public_path);

                let recovery_doc = format!(
                    "# PULSER WALLET RECOVERY DOCUMENT\n\n\
                     IMPORTANT: KEEP THIS DOCUMENT SECURE\n\n\
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
                     ## Cloud Backup Status\n\
                     Not backed up to cloud\n\n\
                     ## Recovery Instructions\n\
                     1. Use this descriptor with a compatible wallet (BlueWallet, Sparrow, Electrum)\n\
                     2. Import the descriptor and seed if available\n\
                     3. Requires 2-of-3 signatures to spend\n\
                     4. Contact support if needed",
                    user_id, config.network, Utc::now().timestamp(),
                    external_descriptor, user_xpub_str, config.lsp_pubkey, config.trustee_pubkey,
                    mnemonic.to_string()
                );
                fs::write(&key_path, &recovery_doc)?;
                info!("Recovery document cached locally at {}—back up to cloud optionally!", key_path);
            }
        }

        let wallet_instance = DepositWallet {
            wallet,
            blockchain,
            network,
            wallet_path,
            events: Vec::new(),
            stable_chain: stable_chain.clone(),
        };

        Ok((wallet_instance, deposit_info, stable_chain))
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
            } else if confirmations == 0 {
                info!("0-conf UTXO detected for {}: {} BTC", address, u.amount as f64 / 100_000_000.0);
            }
        }
        Ok(validated_utxos)
    }

    pub async fn update_stable_chain(&mut self, price_info: &PriceInfo) -> Result<Vec<Utxo>, PulserError> {
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
        self.save_stable_chain()?;
        info!("Updated stable chain for user {}: {} BTC", self.stable_chain.user_id, self.stable_chain.accumulated_btc.sats as f64 / 100_000_000.0);
        Ok(utxos)
    }

    pub async fn reveal_new_address(&mut self) -> Result<Address, PulserError> {
        let new_addr = self.wallet.reveal_next_address(KeychainKind::External).address;
        let mut logged = LOGGED_ADDRESSES.lock().await;
        logged.insert(self.stable_chain.user_id.to_string(), new_addr.clone());
        self.stable_chain.multisig_addr = new_addr.to_string();
        self.save_stable_chain()?;
        info!("Revealed new deposit address for user {}: {}", self.stable_chain.user_id, new_addr);
        Ok(new_addr)
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

    pub fn save_stable_chain(&self) -> Result<(), PulserError> {
        let sc_path = format!("{}/stable_chain_{}.json", self.wallet_path, self.stable_chain.user_id);
        let json = serde_json::to_string_pretty(&self.stable_chain)?;
        fs::write(&sc_path, json)?;
        Ok(())
    }

    pub fn get_cached_utxos(&self) -> Vec<Utxo> {
        self.stable_chain.utxos.clone()
    }
}

#[derive(serde::Deserialize)]
struct Config {
    network: String,
    esplora_url: String,
    lsp_pubkey: String,
    trustee_pubkey: String,
    data_dir: String,
    wallet_dir: String,
}
