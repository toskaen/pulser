use std::fs;
use std::str::FromStr;
use std::path::Path;
use bdk_esplora::{esplora_client, EsploraAsyncExt};
use bdk_wallet::{Wallet, KeychainKind};
use bdk_chain::{spk_client::SyncRequest, BlockId};
use bdk_wallet::bitcoin::{Amount, Network, ScriptBuf};
use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::bitcoin::bip32::{DerivationPath, Xpub};
use common::error::PulserError;
use crate::types::{DepositAddressInfo, Utxo, StableChain, Bitcoin};
use crate::keys::create_multisig_descriptor;
use log::info;
use common::types::USD;
use std::collections::HashMap;
use common::price_feed::fetch_btc_usd_price;
use bitcoin::Address; // Add this
use serde::Deserialize;





pub struct DepositWallet {
    pub wallet: Wallet,
    pub blockchain: esplora_client::AsyncClient,
    pub network: Network,
    pub wallet_path: String,
    pub events: Vec<common::types::Event>,
}

impl DepositWallet {
pub fn from_config(config_path: &str, user_id: &str) -> Result<(Self, Option<StableChain>), PulserError> {
    let config_str = fs::read_to_string(config_path)?;
    let config: Config = toml::from_str(&config_str)?;
    let network = Network::from_str(&config.network)?;
    let lsp_xpub = Xpub::from_str(&config.lsp_pubkey)?;
    let trustee_xpub = Xpub::from_str(&config.trustee_pubkey)?;

    let secp = Secp256k1::new();
    let key_path = format!("{}/secrets/user_{}_key.json", config.data_dir, user_id);
    let mnemonic = if Path::new(&key_path).exists() {
        let key_json = fs::read_to_string(&key_path)?;
        let key_material: TaprootKeyMaterial = serde_json::from_str(&key_json)?;
        Mnemonic::parse_in(Language::English, key_material.secret_key.ok_or("No secret key")?.trim())?
    } else {
        Mnemonic::generate((WordCount::Words12, Language::English))?.into_key()
    };

    let external_path = DerivationPath::from_str("m/84'/1'/0'/0/0")?;
    let internal_path = DerivationPath::from_str("m/84'/1'/0'/1/0")?;
    let unspendable_key_external = "4d54bb9928a0683b7e383de72943b214b0716f58aa54c7ba6bcea2328bc9c768";
    let unspendable_key_internal = "03a34b99f22c790c4e36b2b3c2c35a36db06226e41c692fc82b8b56ac1c540c5";

    let user_xpriv = mnemonic.into_extended_key()?.into_xprv(network)?.derive_priv(&secp, &external_path)?;
    let user_xpub = Xpub::from_priv(&secp, &user_xpriv);
    let user_xpriv_internal = mnemonic.into_extended_key()?.into_xprv(network)?.derive_priv(&secp, &internal_path)?;
    let user_xpub_internal = Xpub::from_priv(&secp, &user_xpriv_internal);

    let external_descriptor = create_multisig_descriptor(
        &secp, &user_xpub, &lsp_xpub, &trustee_xpub, &external_path, unspendable_key_external, false
    )?;
    let internal_descriptor = create_multisig_descriptor(
        &secp, &user_xpub_internal, &lsp_xpub, &trustee_xpub, &internal_path, unspendable_key_internal, true
    )?;

    let wallet = Wallet::create(external_descriptor, internal_descriptor)
        .network(network)
        .create_wallet_no_persist()?;
    let blockchain = esplora_client::Builder::new(&config.esplora_url).build_async()?;
    let wallet_path = format!("{}/{}", config.data_dir, config.wallet_dir);

    let chain = Some(StableChain::load_or_create(user_id, &wallet.reveal_next_address(KeychainKind::External).address.to_string(), &wallet_path)?);
    Ok((Self { wallet, blockchain, network, wallet_path, events: Vec::new() }, chain))
}

    pub async fn sync(&mut self) -> Result<(), PulserError> {
        let spk_iters = self.wallet.all_unbounded_spk_iters();
        let external_spks: Vec<_> = spk_iters.get(&KeychainKind::External).unwrap().clone()
            .map(|(_, spk)| spk).collect();
        let request = SyncRequest::builder().spks(external_spks).build();
        let response = self.blockchain.sync(request, 1).await?;

        let chain_tip = self.blockchain.get_height().await?;
        let chain_tip_hash = self.blockchain.get_block_hash(chain_tip).await?;
        let mut chain = self.wallet.local_chain().clone();
        for (anchor, txid) in &response.tx_update.anchors {
            chain.insert_block(anchor.block_id)
                .map_err(|e| PulserError::WalletError(format!("Anchor insert failed for {}: {}", txid, e)))?;
        }
        chain.insert_block(BlockId { height: chain_tip, hash: chain_tip_hash })
            .map_err(|e| PulserError::WalletError(format!("Tip insert failed: {}", e)))?;

        let update = bdk_wallet::Update {
            tx_update: response.tx_update,
            chain: Some(chain.tip()),
            last_active_indices: Default::default(),
        };
        self.wallet.apply_update(update)?;
        Ok(())
    }

    pub fn list_utxos(&self) -> Result<Vec<Utxo>, PulserError> {
        let utxos: Vec<Utxo> = self.wallet.list_unspent().into_iter().map(|utxo| {
            info!("UTXO: {}:{} = {} sats", utxo.outpoint.txid, utxo.outpoint.vout, utxo.txout.value.to_sat());
            Utxo {
                txid: utxo.outpoint.txid.to_string(),
                vout: utxo.outpoint.vout,
                amount: utxo.txout.value.to_sat(),
                confirmations: 0, // Mocked, no persistence
                script_pubkey: utxo.txout.script_pubkey.to_hex_string(),
                height: None,
                    usd_value: None, // Add this
            }
        }).collect();
        info!("Found {} UTXOs", utxos.len());
        Ok(utxos)
    }

    pub async fn monitor_deposits(&mut self, user_id: &str, stable_chain: &mut StableChain) -> Result<(), PulserError> {
        println!("Syncing deposits for user {} at address {}", user_id, stable_chain.multisig_addr);
        self.sync().await?;
        let utxos = self.list_utxos()?;
        let client = reqwest::Client::new();
        let price_info = fetch_btc_usd_price(&client).await?;
        stable_chain.raw_btc_usd = price_info.raw_btc_usd;

        for utxo in utxos {
            if !stable_chain.utxos.iter().any(|u| u.txid == utxo.txid && u.vout == utxo.vout) {
                let btc = Bitcoin::from_sats(utxo.amount);
                let usd_at_deposit = USD(btc.sats as f64 / 100_000_000.0 * price_info.raw_btc_usd);
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
                println!("User {}: Deposit detected: {} BTC (${:.2}) at {}", 
                         user_id, btc, usd_at_deposit.0, &utxo.txid);
                stable_chain.log_event("deposit", "new_utxo", &format!("{} sats (${:.2}) at {}", utxo.amount, usd_at_deposit.0, utxo.txid));
            }
        }
        stable_chain.accumulated_btc = Bitcoin::from_sats(stable_chain.utxos.iter().map(|u| u.amount).sum());
        stable_chain.stabilized_usd = USD(stable_chain.utxos.iter().map(|u| u.usd_value.as_ref().unwrap_or(&USD(0.0)).0).sum());

        let sc_path = Path::new(&stable_chain.sc_dir).join(format!("stable_chain_{}.json", user_id));
        fs::create_dir_all(&stable_chain.sc_dir)?;
        let json = serde_json::to_string_pretty(stable_chain)?;
        println!("User {}: StableChain state: {}", user_id, json);
        fs::write(&sc_path, json)?;
        log::info!("Saved StableChain to {:?}", sc_path);
        Ok(())
    }

pub fn create_for_user(
    user_id: &str,
    config_path: &str,
    lsp_pubkey: &str,
    trustee_pubkey: &str,
    user_pubkey: &str,
    network: Network,
) -> Result<(Self, DepositAddressInfo, StableChain), PulserError> {
    let (wallet, deposit_info) = Self::from_config(config_path, user_id)?;
    let stable_chain = StableChain::load_or_create(user_id, &wallet.multisig_addr, &wallet.wallet_path)?;
    Ok((wallet, deposit_info, stable_chain))
}
    }

#[derive(Deserialize)]
pub struct Config {
    pub network: String,
    pub esplora_url: String,
    pub lsp_pubkey: String,
    pub trustee_pubkey: String,
    pub data_dir: String,
    pub wallet_dir: String,
}

