use std::fs;
use bdk_wallet::{Wallet, KeychainKind};
use bdk_wallet::bitcoin::{Network, Address};
use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::bitcoin::bip32::{DerivationPath, Xpub, Fingerprint};
use bdk_wallet::keys::bip39::{Mnemonic, Language, WordCount};
use bdk_wallet::keys::{DerivableKey, GeneratableKey, ExtendedKey, GeneratedKey};
use bdk_wallet::miniscript::descriptor::{DescriptorXKey, Wildcard};
use bdk_wallet::keys::DescriptorPublicKey;
use common::error::PulserError;
use common::types::DepositAddressInfo;
use log::info;
use serde_json;
use chrono::Utc;
use std::str::FromStr;
use bdk_wallet::miniscript::Tap;

#[derive(serde::Deserialize)]
pub struct Config {
    pub network: String,
    pub data_dir: String,
    pub lsp_pubkey: String,
    pub trustee_pubkey: String,
    pub esplora_url: String,
    pub listening_address: String,
    pub webhook_url: String,
    pub fallback_esplora_url: String,
    pub listening_port: u16,
    pub sync_interval_secs: u64,
    pub max_concurrent_users: usize,
    pub min_confirmations: u32, // Added for sync_and_stabilize_utxos

}

impl Config {
    pub fn from_toml(toml: &toml::Value) -> Result<Self, toml::de::Error> {
        toml.clone().try_into()
    }
}

pub struct WalletInitResult {
    pub external_descriptor: String,
    pub internal_descriptor: String,
    pub deposit_info: DepositAddressInfo,
    pub recovery_doc: String,
    pub public_data: serde_json::Value,
}

pub fn init_wallet(config: &Config, user_id: &str) -> Result<WalletInitResult, PulserError> {
    let network = Network::from_str(&config.network)?;
    let wallet_dir = format!("{}/user_{}", config.data_dir, user_id);
    let public_path = format!("{}/user_{}_public.json", wallet_dir, user_id);
    fs::create_dir_all(&wallet_dir)?;

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
        let external_path = DerivationPath::from_str("m/86'/1'/0'/0/0")?;
        let internal_path = DerivationPath::from_str("m/86'/1'/0'/1/0")?;

        let mnemonic: GeneratedKey<Mnemonic, miniscript::Tap> = Mnemonic::generate((WordCount::Words12, Language::English))
            .map_err(|e| PulserError::WalletError(format!("Mnemonic generation failed: {:?}", e)))?;

        let user_extended_key: ExtendedKey<miniscript::Tap> = mnemonic
            .clone()
            .into_extended_key()
            .map_err(|e| PulserError::WalletError(format!("Failed to get extended key: {:?}", e)))?;
        let user_xpriv = user_extended_key
            .into_xprv(network)
            .ok_or(PulserError::WalletError("Failed to get xprv".into()))?
            .derive_priv(&secp, &external_path)
            .map_err(|e| PulserError::WalletError(format!("Failed to derive private key: {}", e)))?;
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
            origin: Some((Fingerprint::default(), external_path.clone())),
            xkey: trustee_xpub,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        });

let unspendable_key = DescriptorPublicKey::from_str(
    "0254bb9928a0683b7e383de72943b214b0716f58aa54c7ba6bcea2328bc9c768",
)?;

        let external_descriptor = format!(
            "tr({},multi_a(2,{},{},{}))",
            unspendable_key, lsp_pubkey, trustee_pubkey, user_pubkey
        );

        let user_extended_key_internal: ExtendedKey<miniscript::Tap> = mnemonic
            .clone()
            .into_extended_key()
            .map_err(|e| PulserError::WalletError(format!("Failed to get extended key: {:?}", e)))?;
        let user_xpriv_internal = user_extended_key_internal
            .into_xprv(network)
            .ok_or(PulserError::WalletError("Failed to get xprv".into()))?
            .derive_priv(&secp, &internal_path)?;
        let user_xpub_internal = Xpub::from_priv(&secp, &user_xpriv_internal);

        let user_pubkey_internal = DescriptorPublicKey::XPub(DescriptorXKey {
            origin: Some((Fingerprint::default(), internal_path.clone())),
            xkey: user_xpub_internal,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        });
        let lsp_pubkey_internal = DescriptorPublicKey::XPub(DescriptorXKey {
            origin: Some((Fingerprint::default(), internal_path.clone())),
            xkey: lsp_xpub,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        });
        let trustee_pubkey_internal = DescriptorPublicKey::XPub(DescriptorXKey {
            origin: Some((Fingerprint::default(), internal_path)),
            xkey: trustee_xpub,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        });

        let internal_descriptor = format!(
            "tr({},multi_a(2,{},{},{}))",
            unspendable_key, lsp_pubkey_internal, trustee_pubkey_internal, user_pubkey_internal
        );

        (external_descriptor, internal_descriptor, user_xpub.to_string(), Some(mnemonic))
    };

    let mut wallet = Wallet::create(external_descriptor.clone(), internal_descriptor.clone())
        .network(network)
        .create_wallet_no_persist()?;
    let initial_addr = wallet.reveal_next_address(KeychainKind::External).address;

let deposit_info = DepositAddressInfo {
    address: initial_addr.to_string(),
    user_id: user_id.parse().unwrap_or(0),
    multisig_type: "2-of-3".to_string(),
    participants: vec![
        user_xpub_str.clone(),
        config.lsp_pubkey.clone(),
        config.trustee_pubkey.clone()
    ],
    descriptor: Some(external_descriptor.clone()),
    path: "m/86'/1'/0'/0/0".to_string(),
    user_pubkey: user_xpub_str.clone(),
    lsp_pubkey: config.lsp_pubkey.clone(),
    trustee_pubkey: config.trustee_pubkey.clone(),
};

    let public_data = if !is_preloaded {
        let public_data = serde_json::json!({
            "wallet_descriptor": &external_descriptor,
            "internal_descriptor": &internal_descriptor,
            "lsp_pubkey": &config.lsp_pubkey,
            "trustee_pubkey": &config.trustee_pubkey,
            "user_pubkey": &user_xpub_str,
            "user_id": user_id
        });
        fs::write(&public_path, serde_json::to_string_pretty(&public_data)?)?;
        info!("Public data saved to {}", public_path);
        public_data
    } else {
        public_data
    };

    let recovery_doc = if let Some(mnemonic) = mnemonic_opt {
        format!(
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
             ## Recovery Instructions\n\
             1. Use this descriptor with a compatible wallet (BlueWallet, Sparrow, Electrum)\n\
             2. Import the descriptor and seed if available\n\
             3. Requires 2-of-3 signatures to spend\n\
             4. Contact support if needed",
            user_id, config.network, Utc::now().timestamp(),
            external_descriptor, user_xpub_str, config.lsp_pubkey, config.trustee_pubkey,
            mnemonic.to_string()
        )
    } else {
        String::new()
    };

    info!("Initialized wallet for user {}: {}", user_id, initial_addr);
    Ok(WalletInitResult {
        external_descriptor,
        internal_descriptor,
        deposit_info,
        recovery_doc,
        public_data,
    })
}
