use std::fs;
use std::path::Path;
use std::str::FromStr;

use bdk_esplora::{esplora_client, EsploraAsyncExt}; // For sync
use bdk_esplora::esplora_client::AsyncClient;
use bdk_file_store::Store;
use bdk_wallet::{KeychainKind, Wallet, Update};
use bdk_chain::spk_client::SyncRequest;
use bitcoin::{Network, ScriptBuf};
use common::error::PulserError;

use crate::types::{DepositAddressInfo, Utxo};

pub struct DepositWallet {
    pub wallet: Wallet,
    pub blockchain: AsyncClient,
    pub network: Network,
    pub wallet_path: String,
    pub events: Vec<common::types::Event>,
}

impl DepositWallet {
    pub fn from_config(config_path: &str, user_pubkey: &str) -> Result<(Self, DepositAddressInfo), PulserError> {
        let config_str = fs::read_to_string(config_path)
            .map_err(|e| PulserError::StorageError(format!("Failed to read config: {}", e)))?;
        let config: Config = toml::from_str(&config_str)
            .map_err(|e| PulserError::ConfigError(format!("Failed to parse config: {}", e)))?;

        let network = Network::from_str(&config.network)
            .map_err(|e| PulserError::ConfigError(format!("Invalid network: {}", e)))?;
        let blockchain = esplora_client::Builder::new(&config.esplora_url)
            .build_async()
            .map_err(|e| PulserError::ApiError(format!("Esplora client error: {}", e)))?;

        let data_dir = format!("{}/{}", config.data_dir, config.wallet_dir);
        fs::create_dir_all(&data_dir)
            .map_err(|e| PulserError::StorageError(format!("Failed to create dir: {}", e)))?;

        create_taproot_multisig(
            user_pubkey,
            &config.lsp_pubkey,
            &config.trustee_pubkey,
            network,
            blockchain,
            &data_dir,
        )
    }

    pub async fn sync(&mut self) -> Result<(), PulserError> {
        let spks: Vec<ScriptBuf> = self
            .wallet
            .all_unbounded_spk_iters()
            .get(&KeychainKind::External)
            .unwrap()
            .clone()
            .take(1)
            .map(|(_, spk)| spk)
            .collect();
        let request = SyncRequest::builder()
            .spks(spks)
            .build();
        let response = self
            .blockchain
            .sync(request, 1)
            .await
            .map_err(|e| PulserError::WalletError(format!("Sync failed: {}", e)))?;
        let update = Update {
            tx_update: response.tx_update,
            chain: None,
            last_active_indices: Default::default(),
        };
        self.wallet
            .apply_update(update)
            .map_err(|e| PulserError::WalletError(format!("Apply update failed: {}", e)))?;
        self.persist()?; // Persist changes after sync
        Ok(())
    }

    pub fn list_utxos(&self) -> Result<Vec<Utxo>, PulserError> {
        let unspent = self.wallet.list_unspent();
        let address_info = self.wallet.peek_address(KeychainKind::External, 0);

        let utxos = unspent.map(|output| {
            let amount_sats = output.txout.value.to_sat();
            let (confirmations, height) = match &output.chain_position {
                bdk_chain::ChainPosition::Confirmed { anchor, .. } => (1, Some(anchor.block_id.height)),
                _ => (0, None),
            };
            Utxo {
                txid: output.outpoint.txid.to_string(),
                vout: output.outpoint.vout,
                amount: amount_sats,
                confirmations,
                script_pubkey: hex::encode(output.txout.script_pubkey.as_bytes()),
                height,
                address: address_info.address.to_string(),
            }
        }).collect();
        Ok(utxos)
    }

    pub async fn update_utxo_confirmations(&mut self, utxos: &mut Vec<Utxo>) -> Result<(), PulserError> {
        self.sync().await?;
        let blockchain_utxos = self.list_utxos()?;
        for utxo in utxos.iter_mut() {
            if let Some(bdk_utxo) = blockchain_utxos.iter().find(|u| u.txid == utxo.txid && u.vout == utxo.vout) {
                utxo.confirmations = bdk_utxo.confirmations;
                utxo.height = bdk_utxo.height;
            }
        }
        Ok(())
    }

    pub fn persist(&mut self) -> Result<(), PulserError> {
        let mut db = Store::<bdk_wallet::ChangeSet>::open_or_create_new(b"pulser_wallet", &self.wallet_path)
            .map_err(|e| PulserError::StorageError(format!("Failed to write store: {}", e)))?;
        self.wallet
            .persist(&mut db)
            .map_err(|e| PulserError::WalletError(format!("Persist failed: {}", e)))?;
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

fn create_taproot_multisig(
    user_pubkey: &str,
    lsp_pubkey: &str,
    trustee_pubkey: &str,
    network: Network,
    blockchain: AsyncClient,
    data_dir: &str,
) -> Result<(DepositWallet, DepositAddressInfo), PulserError> {
    let keys = [user_pubkey, lsp_pubkey, trustee_pubkey];
    for key in &keys {
        if key.len() != 64 || !key.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(PulserError::InvalidRequest(
                "All public keys must be 32-byte X-only keys (64 hex characters)".to_string()
            ));
        }
    }

    let external_descriptor = format!(
        "tr({},{})",
        user_pubkey,
        format!("multi_a(2,{},{},{})", user_pubkey, lsp_pubkey, trustee_pubkey)
    );
    let internal_descriptor = format!(
        "tr({},{})",
        lsp_pubkey,
        format!("multi_a(2,{},{},{})", user_pubkey, lsp_pubkey, trustee_pubkey)
    );

    let wallet_path = format!("{}/multisig_{}.store", data_dir, user_pubkey);
    let mut wallet = Wallet::create(external_descriptor.clone(), internal_descriptor.clone())
        .network(network)
        .create_wallet_no_persist()
        .map_err(|e| PulserError::WalletError(format!("Wallet creation error: {}", e)))?;

    let address_info = wallet.reveal_next_address(KeychainKind::External);
    let deposit_info = DepositAddressInfo {
        address: address_info.address.to_string(),
        descriptor: external_descriptor,
        path: "taproot/0".to_string(),
        user_pubkey: user_pubkey.to_string(),
        lsp_pubkey: lsp_pubkey.to_string(),
        trustee_pubkey: trustee_pubkey.to_string(),
    };

    let deposit_wallet = DepositWallet {
        wallet,
        blockchain,
        network,
        wallet_path,
        events: Vec::new(),
    };

    Ok((deposit_wallet, deposit_info))
}
