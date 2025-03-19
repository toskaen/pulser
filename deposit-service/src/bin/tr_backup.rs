use std::fs;
use std::path::Path;
use std::str::FromStr;

use bdk_esplora::{esplora_client, EsploraAsyncExt};
use bdk_esplora::esplora_client::AsyncClient;
use bdk_wallet::{KeychainKind, Update, Wallet};
use bdk_chain::spk_client::SyncRequest;
use bdk_chain::{Anchor, BlockId, ChainPosition, local_chain::LocalChain};
use bdk_chain::bitcoin::BlockHash;
use bitcoin::{Amount, Network, ScriptBuf};
use common::error::PulserError;
use crate::types::{DepositAddressInfo, Utxo};
use common::types::Event;

use aes_gcm::{Aes256Gcm, KeyInit, Nonce, aead::Aead};
use rand::Rng;
use bitcoin::secp256k1::{Secp256k1, SecretKey, XOnlyPublicKey};

pub struct DepositWallet {
    pub wallet: Wallet,
    pub blockchain: AsyncClient,
    pub network: Network,
    pub wallet_path: String,
    pub events: Vec<Event>,
}

pub fn secure_init(user_id: &str, data_dir: &str) -> Result<String, PulserError> {
    let secp = Secp256k1::new();
    let priv_key = SecretKey::new(&mut rand::thread_rng());
    let (xonly_pubkey, _) = XOnlyPublicKey::from_keypair(&priv_key.keypair(&secp));
    let pubkey = xonly_pubkey.to_string();
    println!("Generated pubkey: {} (len: {})", pubkey, pubkey.len());

    let key = Aes256Gcm::generate_key(&mut rand::thread_rng());
    let cipher = Aes256Gcm::new(&key);
    let nonce = rand::thread_rng().gen::<[u8; 12]>();
    let encrypted_seed = cipher.encrypt(&Nonce::from_slice(&nonce), &priv_key[..])
        .map_err(|e| PulserError::StorageError(format!("Encryption failed: {}", e)))?;

    fs::create_dir_all(format!("{}/secrets", data_dir))?;
    let seed_path = format!("{}/secrets/user_{}.enc", data_dir, user_id);
    fs::write(&seed_path, [&nonce[..], &encrypted_seed[..]].concat())?;
    fs::write(format!("{}/secrets/key_{}.bin", data_dir, user_id), key.as_slice())?;

    Ok(pubkey)
}

fn get_seed(user_id: &str, data_dir: &str) -> Result<Vec<u8>, PulserError> {
    let seed_path = format!("{}/secrets/user_{}.enc", data_dir, user_id);
    let key_path = format!("{}/secrets/key_{}.bin", data_dir, user_id);
    let encrypted_data = fs::read(&seed_path)?;
    let key = fs::read(&key_path)?;
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|e| PulserError::StorageError(format!("Key init failed: {}", e)))?;
    let nonce = &encrypted_data[..12];
    let ciphertext = &encrypted_data[12..];
    cipher.decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|e| PulserError::StorageError(format!("Decryption failed: {}", e)))
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

        let user_pubkey = if fs::metadata(format!("{}/secrets/user_{}.enc", data_dir, user_pubkey)).is_ok() {
            user_pubkey.to_string()
        } else {
            secure_init(user_pubkey, &data_dir)?
        };

        Self::create_user_multisig(
            &user_pubkey,
            &config.lsp_pubkey,
            &config.trustee_pubkey,
            network,
            blockchain,
            &data_dir,
        )
    }

    pub fn create_user_multisig(
        user_pubkey: &str,
        lsp_pubkey: &str,
        trustee_pubkey: &str,
        network: Network,
        blockchain: AsyncClient,
        data_dir: &str,
    ) -> Result<(Self, DepositAddressInfo), PulserError> {
        let keys = [user_pubkey, lsp_pubkey, trustee_pubkey];
        
for key in &[user_pubkey, lsp_pubkey, trustee_pubkey] {
    println!("Pubkey: {} (len: {})", key, key.len());
    if key.len() != 64 || !key.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(PulserError::InvalidRequest(format!("Invalid pubkey: {}", key)));
    }
}

let wallet_path = format!("{}/multisig_{}", data_dir, user_pubkey);
fs::create_dir_all(&wallet_path)?;
let external_path = format!("{}/external.desc", wallet_path);
let internal_path = format!("{}/internal.desc", wallet_path);


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
println!("Descriptor: {}", external_descriptor);


if !Path::new(&external_path).exists() {
    fs::write(&external_path, format!("{}\n{}", external_descriptor, network))?;
}
if !Path::new(&internal_path).exists() {
    fs::write(&internal_path, format!("{}\n{}", internal_descriptor, network))?;
}


let mut wallet = Wallet::create(external_descriptor.clone(), internal_descriptor)
    .network(network)
    .create_wallet_no_persist()
    .map_err(|e| PulserError::WalletError(format!("Descriptor error: {}", e)))?;
let address_info = wallet.reveal_next_address(KeychainKind::External);
println!("Address: {}", address_info.address);



let deposit_info = DepositAddressInfo {
    address: address_info.address.to_string(),
    descriptor: external_descriptor,
    path: "taproot/0".to_string(),
    user_pubkey: user_pubkey.to_string(),
    lsp_pubkey: lsp_pubkey.to_string(),
    trustee_pubkey: trustee_pubkey.to_string(),
};
Ok((DepositWallet {
    wallet,
    blockchain,
    network,
    wallet_path,
    events: Vec::new(),
}, deposit_info))
       
    }

    pub fn monitor_taproot_multisig(
        external_desc: String,
        internal_desc: String,
        network: Network,
        blockchain: AsyncClient,
    ) -> Result<Self, PulserError> {
   let mut wallet = Wallet::create(external_desc.clone(), internal_desc)
    .network(network)
    .create_wallet_no_persist()
    .map_err(|e| PulserError::WalletError(format!("Descriptor error: {}", e)))?;
        let genesis_hash = BlockHash::from_str("000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943")
            .map_err(|e| PulserError::WalletError(format!("Invalid genesis hash: {}", e)))?;
        let (chain, _) = LocalChain::from_genesis_hash(genesis_hash);
        wallet.apply_update(Update {
            tx_update: Default::default(),
            chain: Some(chain.tip()),
            last_active_indices: Default::default(),
        })
        .map_err(|e| PulserError::WalletError(format!("Apply update failed: {}", e)))?;

        Ok(DepositWallet {
    wallet,
    blockchain,
    network,
    wallet_path: "".to_string(),
    events: Vec::new(),
})

    }

    pub async fn sync(&mut self) -> Result<(), PulserError> {
        let spks: Vec<ScriptBuf> = self.wallet.all_unbounded_spk_iters()
            .get(&KeychainKind::External).unwrap().clone()
            .map(|(_, spk)| spk).collect();
        println!("Syncing scripts: {:?}", spks);
        let request = SyncRequest::builder().spks(spks).build();
        let response = self.blockchain.sync(request, 1).await?;
        let chain_tip = self.blockchain.get_height().await
            .map_err(|e| PulserError::ApiError(format!("Failed to get chain height: {}", e)))?;
        let chain_tip_hash = self.blockchain.get_block_hash(chain_tip).await
            .map_err(|e| PulserError::ApiError(format!("Failed to get block hash: {}", e)))?;

        let mut chain = self.wallet.local_chain().clone();
        for (anchor, txid) in &response.tx_update.anchors {
            let block_id = anchor.block_id;
            chain.insert_block(block_id)
                .map_err(|e| PulserError::WalletError(format!("Anchor insert failed for {}: {}", txid, e)))?;
        }
        chain.insert_block(BlockId {
            height: chain_tip,
            hash: chain_tip_hash,
        })
        .map_err(|e| PulserError::WalletError(format!("Tip insert failed: {}", e)))?;

        let update = Update {
            tx_update: response.tx_update,
            chain: Some(chain.tip()),
            last_active_indices: Default::default(),
        };
        self.wallet.apply_update(update)
            .map_err(|e| PulserError::WalletError(format!("Apply update failed: {}", e)))?;
        Ok(())
    }

    pub fn list_utxos(&self) -> Result<Vec<Utxo>, PulserError> {
        let tip_height = self.wallet.latest_checkpoint().height();
        let utxos = self.wallet.list_unspent().into_iter().map(|utxo| {
            let (height, confirmations) = match utxo.chain_position {
                ChainPosition::Confirmed { anchor, .. } => {
                    let height = anchor.confirmation_height_upper_bound();
                    (Some(height), tip_height.saturating_sub(height) + 1)
                }
                ChainPosition::Unconfirmed { .. } => (None, 0),
            };
            Utxo {
                txid: utxo.outpoint.txid.to_string(),
                vout: utxo.outpoint.vout,
                amount: utxo.txout.value.to_sat(),
                confirmations,
                script_pubkey: utxo.txout.script_pubkey.to_hex_string(),
                height,
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

    pub fn withdraw(&mut self, user_id: &str, amount: u64, recipient: ScriptBuf) -> Result<(), PulserError> {
        let seed = get_seed(user_id, &self.wallet_path[..self.wallet_path.rfind('/').unwrap()])?;
        let mut tx_builder = self.wallet.build_tx();
tx_builder.add_recipient(recipient, Amount::from_sat(amount));
        let _psbt = tx_builder.finish()
            .map_err(|e| PulserError::WalletError(format!("Failed to build TX: {}", e)))?;
        // TODO: Sign with seed and broadcast (requires secp256k1 integration)
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
