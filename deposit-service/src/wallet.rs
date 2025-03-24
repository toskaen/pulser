use std::fs;
use std::str::FromStr;
use std::collections::HashMap;
use bdk_wallet::{Wallet, KeychainKind};
use bdk_esplora::esplora_client;
use bdk_chain::spk_client::SyncRequest;
use bdk_wallet::bitcoin::{Network, ScriptBuf};
use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::bitcoin::bip32::{DerivationPath, Xpub, Fingerprint};
use bdk_wallet::keys::bip39::{Mnemonic, Language, WordCount};
use bdk_wallet::keys::{GeneratableKey, DerivableKey, GeneratedKey, ExtendedKey};
use bdk_wallet::miniscript::descriptor::{DescriptorXKey, Wildcard};
use bdk_wallet::keys::DescriptorPublicKey;
use common::error::PulserError;
use common::types::{USD, Event, PriceInfo};
use crate::types::{StableChain, Utxo, Bitcoin, DepositAddressInfo};
use log::{info, warn};
use serde_json;
use chrono::Utc;
use reqwest::Client;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UtxoInfo { // Shared type for deposit_monitor
    pub txid: String,
    pub vout: u32,
    pub amount_sat: u64,
    pub address: String,
    pub keychain: String,
    pub timestamp: u64,
    pub confirmations: u32,
    pub participants: Vec<String>,
    pub stable_value_usd: f64,
    pub spendable: bool,
    pub derivation_path: String,
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
        let wallet_path = format!("{}/{}/multisig_{}", config.data_dir, config.wallet_dir, user_id);
        fs::create_dir_all(&wallet_path)?;

        let secp = Secp256k1::new();
        let key_path = format!("{}/secrets/user_{}_recovery.txt", config.data_dir, user_id);
        fs::create_dir_all(format!("{}/secrets", config.data_dir))?;

let mnemonic = if std::path::Path::new(&key_path).exists() {
    let recovery_txt = fs::read_to_string(&key_path)?;
    let seed_line = recovery_txt.lines()
        .find(|line| line.starts_with("weather") || line.contains(" "))
        .ok_or(PulserError::WalletError("No seed found in recovery.txt".into()))?;
    Mnemonic::parse_in(Language::English, seed_line.trim())?
} else {
    let generated: GeneratedKey<Mnemonic, miniscript::Tap> = Mnemonic::generate((WordCount::Words12, Language::English))
        .map_err(|e| PulserError::WalletError(format!("Mnemonic generation failed: {:?}", e)))?;
    generated.into_key()
};

        let lsp_xpub = Xpub::from_str(&config.lsp_pubkey)?;
        let trustee_xpub = Xpub::from_str(&config.trustee_pubkey)?;
        let external_path = DerivationPath::from_str("m/84'/1'/0'/0/0")?;
        let internal_path = DerivationPath::from_str("m/84'/1'/0'/1/0")?;

let user_extended_key: ExtendedKey<miniscript::Tap> = mnemonic.clone().into_extended_key()?;
        let user_xpriv = user_extended_key
            .into_xprv(network).ok_or(PulserError::WalletError("Failed to get xprv".into()))?
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
            "tr(03a34b99f22c790c4e36b2b3c2c35a36db06226e41c692fc82b8b56ac1c540c5,multi_a(2,{},{},{}))",
            lsp_pubkey, trustee_pubkey, user_pubkey
        );

        let mut wallet = Wallet::create(external_descriptor.clone(), internal_descriptor.clone())
            .network(network)
            .create_wallet_no_persist()?;

        let address_info = wallet.reveal_next_address(KeychainKind::External);
        let deposit_info = DepositAddressInfo {
            address: address_info.address.to_string(),
            descriptor: external_descriptor.clone(),
            path: "m/84'/1'/0'/0/0".to_string(),
            user_pubkey: user_xpub.to_string(),
            lsp_pubkey: config.lsp_pubkey.clone(),
            trustee_pubkey: config.trustee_pubkey.clone(),
        };

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
                multisig_addr: address_info.address.to_string(),
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

        let public_data = serde_json::json!({
            "wallet_descriptor": external_descriptor,
            "internal_descriptor": internal_descriptor,
            "lsp_pubkey": &config.lsp_pubkey,
            "trustee_pubkey": &config.trustee_pubkey,
            "user_pubkey": user_xpub.to_string(),
            "user_id": stable_chain.user_id
        });
        let public_path = format!("{}/user_{}_public.json", config.data_dir, user_id);
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
external_descriptor, user_xpub, config.lsp_pubkey, config.trustee_pubkey,
mnemonic.to_string() // Original still available
        );
        fs::write(&key_path, &recovery_doc)?;
        info!("Recovery document cached locally at {}—back up to cloud optionally!", key_path);

        let client = Client::new();
        client.post("http://localhost:8081/register")
            .json(&public_data)
            .send()
            .await
            .map_err(|e| PulserError::NetworkError(format!("Failed to send public data to LSP: {}", e)))?;

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

pub async fn sync(&mut self) -> Result<(), PulserError> {
    let client = Client::new();
    let url = format!("http://localhost:8081/activity/{}", self.stable_chain.user_id);
    let response = client.get(&url).send().await
        .map_err(|e| PulserError::NetworkError(format!("LSP API fetch failed: {}", e)))?;
    let utxos: Vec<UtxoInfo> = response.json().await
        .map_err(|e| PulserError::ApiError(format!("LSP JSON parse failed: {}", e)))?;
    
    self.stable_chain.utxos = utxos.into_iter().map(|u| Utxo {
        txid: u.txid,
        vout: u.vout,
        amount: u.amount_sat,
        confirmations: u.confirmations,
        script_pubkey: u.address,
        height: None,
        usd_value: Some(USD(u.stable_value_usd)),
    }).collect();
    self.stable_chain.accumulated_btc = Bitcoin { sats: self.stable_chain.utxos.iter().map(|u| u.amount).sum() };
    Ok(())
}

pub async fn update_stable_chain(&mut self) -> Result<(), PulserError> {
    let client = Client::new();
    let utxos = self.list_utxos()?;
    let price_info = common::price_feed::fetch_btc_usd_price(&client).await
        .unwrap_or_else(|e| {
            warn!("Price fetch failed: {}, using 0.0", e);
            PriceInfo { raw_btc_usd: 0.0, timestamp: 0, price_feeds: HashMap::new() }
        });
    let price = price_info.raw_btc_usd;

    for utxo in utxos {
        if !self.stable_chain.utxos.iter().any(|u| u.txid == utxo.txid && u.vout == utxo.vout) {
            self.stable_chain.utxos.push(Utxo {
                txid: utxo.txid,
                vout: utxo.vout,
                amount: utxo.amount,
                confirmations: utxo.confirmations,
                script_pubkey: utxo.script_pubkey,
                height: utxo.height,
                usd_value: Some(USD((utxo.amount as f64 / 100_000_000.0) * price)),
            });
        }
    }
    self.stable_chain.accumulated_btc = Bitcoin { sats: self.stable_chain.utxos.iter().map(|u| u.amount).sum() };
    self.stable_chain.stabilized_usd = USD((self.stable_chain.accumulated_btc.sats as f64 / 100_000_000.0) * price);
    self.stable_chain.raw_btc_usd = price;
    self.stable_chain.timestamp = Utc::now().timestamp();
    self.stable_chain.formatted_datetime = Utc::now().to_rfc3339();
    self.stable_chain.prices = price_info.price_feeds;
    self.save_stable_chain()?;
    Ok(())
}

pub fn list_utxos(&self) -> Result<Vec<Utxo>, PulserError> {
    let utxos: Vec<Utxo> = self.wallet.list_unspent().into_iter().map(|utxo| {
        Utxo {
            txid: utxo.outpoint.txid.to_string(),
            vout: utxo.outpoint.vout,
            amount: utxo.txout.value.to_sat(),
            confirmations: 0, // Updated via LSP sync
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
