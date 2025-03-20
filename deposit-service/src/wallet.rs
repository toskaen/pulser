use std::fs;
use std::str::FromStr;
use bdk_esplora::{esplora_client, EsploraAsyncExt};
use bdk_wallet::{Wallet, KeychainKind};
use bdk_chain::spk_client::SyncRequest;
use bdk_wallet::bitcoin::{Network, ScriptBuf};
use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::bitcoin::bip32::{DerivationPath, Xpub, Fingerprint};
use bdk_wallet::keys::DescriptorPublicKey;
use bdk_wallet::miniscript::Tap;
use miniscript::descriptor::{DescriptorXKey, Wildcard};
use common::error::PulserError;
use crate::types::{DepositAddressInfo, Utxo};
use log::info;

pub struct DepositWallet {
    pub wallet: Wallet,
    pub blockchain: esplora_client::AsyncClient,
    pub network: Network,
    pub wallet_path: String,
    pub events: Vec<common::types::Event>,
}

impl DepositWallet {
    pub fn from_config(config_path: &str, user_id: &str) -> Result<(Self, DepositAddressInfo), PulserError> {
        let config_str = fs::read_to_string(config_path)?;
        let config: Config = toml::from_str(&config_str)?;
        let network = Network::from_str(&config.network)
            .map_err(|e| PulserError::ConfigError(format!("Invalid network: {}", e)))?;
        let blockchain = esplora_client::Builder::new(&config.esplora_url).build_async()?;
        let data_dir = format!("{}/{}", config.data_dir, config.wallet_dir);
        fs::create_dir_all(&data_dir)?;

        let user_xpub_path = format!("{}/secrets/user_{}_xpub.txt", data_dir, user_id);
        let user_xpub = fs::read_to_string(&user_xpub_path)
            .map_err(|e| PulserError::StorageError(format!("User Xpub missing: {}", e)))?;

        let secp = Secp256k1::new();
        let lsp_xpub = Xpub::from_str(&config.lsp_pubkey)?;
        let trustee_xpub = Xpub::from_str(&config.trustee_pubkey)?;

        let external_path = DerivationPath::from_str("m/84'/1'/0'/0/0")?;
        let internal_path = DerivationPath::from_str("m/84'/1'/0'/1/0")?;

        let user_desc_xkey = DescriptorXKey {
            origin: Some((Fingerprint::default(), external_path.clone())),
            xkey: Xpub::from_str(&user_xpub)?,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        };
        let lsp_desc_xkey = DescriptorXKey {
            origin: Some((Fingerprint::default(), external_path.clone())),
            xkey: lsp_xpub,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        };
        let trustee_desc_xkey = DescriptorXKey {
            origin: Some((Fingerprint::default(), external_path)),
            xkey: trustee_xpub,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        };

        let user_pubkey = DescriptorPublicKey::XPub(user_desc_xkey);
        let lsp_pubkey = DescriptorPublicKey::XPub(lsp_desc_xkey);
        let trustee_pubkey = DescriptorPublicKey::XPub(trustee_desc_xkey);

        let unspendable_key = DescriptorPublicKey::from_str(
            "4d54bb9928a0683b7e383de72943b214b0716f58aa54c7ba6bcea2328bc9c768",
        )?;

        let external_descriptor = format!(
            "tr({},multi_a(2,{},{},{}))",
            unspendable_key, lsp_pubkey, trustee_pubkey, user_pubkey
        );

        let user_desc_xkey_internal = DescriptorXKey {
            origin: Some((Fingerprint::default(), internal_path.clone())),
            xkey: Xpub::from_str(&user_xpub)?,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        };
        let lsp_desc_xkey_internal = DescriptorXKey {
            origin: Some((Fingerprint::default(), internal_path.clone())),
            xkey: lsp_xpub,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        };
        let trustee_desc_xkey_internal = DescriptorXKey {
            origin: Some((Fingerprint::default(), internal_path)),
            xkey: trustee_xpub,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        };

        let user_pubkey_internal = DescriptorPublicKey::XPub(user_desc_xkey_internal);
        let lsp_pubkey_internal = DescriptorPublicKey::XPub(lsp_desc_xkey_internal);
        let trustee_pubkey_internal = DescriptorPublicKey::XPub(trustee_desc_xkey_internal);

        let internal_descriptor = format!(
            "tr({},multi_a(2,{},{},{}))",
            unspendable_key, lsp_pubkey_internal, trustee_pubkey_internal, user_pubkey_internal
        );

        let wallet_path = format!("{}/multisig_{}", data_dir, user_id);
        fs::create_dir_all(&wallet_path)?;
        fs::write(format!("{}/external.desc", wallet_path), &external_descriptor)?;
        fs::write(format!("{}/internal.desc", wallet_path), &internal_descriptor)?;

        let mut wallet = Wallet::create(external_descriptor.clone(), internal_descriptor)
            .network(network)
            .create_wallet_no_persist()?;

        let address_info = wallet.reveal_next_address(KeychainKind::External);
        let deposit_info = DepositAddressInfo {
            address: address_info.address.to_string(),
            descriptor: external_descriptor,
            path: "taproot/0".to_string(),
            user_pubkey: user_xpub,
            lsp_pubkey: config.lsp_pubkey,
            trustee_pubkey: config.trustee_pubkey,
        };

        Ok((DepositWallet {
            wallet,
            blockchain,
            network,
            wallet_path,
            events: Vec::new(),
        }, deposit_info))
    }

    pub async fn sync(&mut self) -> Result<(), PulserError> {
        let spks: Vec<ScriptBuf> = self.wallet.all_unbounded_spk_iters()
            .get(&KeychainKind::External).unwrap().clone()
            .map(|(_, spk)| spk).collect();
        let request = SyncRequest::builder().spks(spks).build();
        let response = self.blockchain.sync(request, 1).await?;
        self.wallet.apply_update(response)?;
        Ok(())
    }

    pub fn list_utxos(&self) -> Result<Vec<Utxo>, PulserError> {
        let utxos = self.wallet.list_unspent().into_iter().map(|utxo| {
            info!("UTXO: {}:{} = {} sats", utxo.outpoint.txid, utxo.outpoint.vout, utxo.txout.value.to_sat());
            Utxo {
                txid: utxo.outpoint.txid.to_string(),
                vout: utxo.outpoint.vout,
                amount: utxo.txout.value.to_sat(),
                confirmations: 0, // Mocked
                script_pubkey: utxo.txout.script_pubkey.to_hex_string(),
                height: None,
            }
        }).collect();
        info!("Found {} UTXOs", utxos.len());
        Ok(utxos)
    }

    pub async fn monitor_deposits(&mut self, user_id: &str) -> Result<(), PulserError> {
        loop {
            self.sync().await?;
            let utxos = self.list_utxos()?;
            for utxo in utxos {
                if utxo.amount > 0 {
                    let btc = utxo.amount as f64 / 100_000_000.0;
                    let usd = btc; // Mock: 1 testnet BTC = 1 USD
                    println!("User {}: Deposit detected: {} BTC ({} USD, hedged) at {}", 
                             user_id, btc, usd, utxo.txid);
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
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
