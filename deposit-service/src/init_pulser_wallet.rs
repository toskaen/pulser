use std::fs;
use bdk_wallet::bitcoin::Network;
use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::bitcoin::bip32::{DerivationPath, Xpub, Fingerprint};
use bdk_wallet::miniscript::descriptor::{DescriptorXKey, Wildcard};
use bdk_wallet::keys::DescriptorPublicKey;
use common::error::PulserError;
use log::{info, debug};
use serde_json;
use std::str::FromStr;
use crate::config::Config;

#[derive(Debug)]
pub struct WalletInitResult {
    pub external_descriptor: String,
    pub internal_descriptor: String,
    pub user_xpub: String,
    pub public_data: serde_json::Value,
}

pub fn init_pulser_wallet(config: &Config, user_id: &str, user_xpub: Option<String>) -> Result<WalletInitResult, PulserError> {
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

    let (external_descriptor, internal_descriptor, user_xpub) = if is_preloaded {
        let external_desc = public_data["wallet_descriptor"]
            .as_str()
            .ok_or(PulserError::WalletError("Missing wallet_descriptor".into()))?;
        let internal_desc = public_data["internal_descriptor"]
            .as_str()
            .ok_or(PulserError::WalletError("Missing internal_descriptor".into()))?;
        let user_pubkey = public_data["user_pubkey"]
            .as_str()
            .ok_or(PulserError::WalletError("Missing user_pubkey".into()))?;
        (
            external_desc.to_string(),
            internal_desc.to_string(),
            user_pubkey.to_string(),
        )
    } else {
        let lsp_xpub = Xpub::from_str(&config.lsp_pubkey)?;
        let trustee_xpub = Xpub::from_str(&config.trustee_pubkey)?;
        let external_path = DerivationPath::from_str("m/0/0")?; // Non-hardened for Xpub
        let internal_path = DerivationPath::from_str("m/1/0")?; // Non-hardened for Xpub

        let user_xpub = user_xpub.ok_or(PulserError::WalletError("No user_xpub provided for new wallet".into()))?;
        let user_xpub_obj = Xpub::from_str(&user_xpub)?;

        // Derive distinct Xpubs for external and internal
        let user_xpub_external = user_xpub_obj.derive_pub(&secp, &external_path)?;
        let user_xpub_internal = user_xpub_obj.derive_pub(&secp, &internal_path)?;

        let user_pubkey = DescriptorPublicKey::XPub(DescriptorXKey {
            origin: None,
            xkey: user_xpub_external,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        });
        let lsp_pubkey = DescriptorPublicKey::XPub(DescriptorXKey {
            origin: None,
            xkey: lsp_xpub,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        });
        let trustee_pubkey = DescriptorPublicKey::XPub(DescriptorXKey {
            origin: None,
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

        let user_pubkey_internal = DescriptorPublicKey::XPub(DescriptorXKey {
            origin: None,
            xkey: user_xpub_internal,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        });
        let lsp_pubkey_internal = DescriptorPublicKey::XPub(DescriptorXKey {
            origin: None,
            xkey: lsp_xpub,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        });
        let trustee_pubkey_internal = DescriptorPublicKey::XPub(DescriptorXKey {
            origin: None,
            xkey: trustee_xpub,
            derivation_path: DerivationPath::master(),
            wildcard: Wildcard::Unhardened,
        });

        let internal_descriptor = format!(
            "tr({},multi_a(2,{},{},{}))",
            unspendable_key,
            lsp_pubkey_internal,
            trustee_pubkey_internal,
            user_pubkey_internal
        );

        (external_descriptor, internal_descriptor, user_xpub)
    };

    let public_data = if !is_preloaded {
        let public_data = serde_json::json!({
            "wallet_descriptor": &external_descriptor,
            "internal_descriptor": &internal_descriptor,
            "lsp_pubkey": &config.lsp_pubkey,
            "trustee_pubkey": &config.trustee_pubkey,
            "user_pubkey": &user_xpub,
            "user_id": user_id
        });
        if let Some(parent) = std::path::Path::new(&public_path).parent() {
            fs::create_dir_all(parent)?;
        }
        let temp_path = format!("{}.tmp", public_path);
        fs::write(&temp_path, serde_json::to_string_pretty(&public_data)?)?;
        fs::rename(&temp_path, &public_path)?;
        info!("Public data saved to {}", public_path);
        public_data
    } else {
        public_data
    };

    Ok(WalletInitResult {
        external_descriptor,
        internal_descriptor,
        user_xpub,
        public_data,
    })
}
