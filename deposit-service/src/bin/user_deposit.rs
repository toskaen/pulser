use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::keys::bip39::{Mnemonic, Language, WordCount};
use bdk_wallet::keys::{GeneratableKey, GeneratedKey, DerivableKey, ExtendedKey};
use bdk_wallet::bitcoin::bip32::{DerivationPath, Xpub, Fingerprint};
use bdk_wallet::{Wallet, KeychainKind};
use bdk_wallet::bitcoin::Network;
use bdk_wallet::keys::DescriptorPublicKey;
use bdk_wallet::miniscript::Tap;
use miniscript::descriptor::{DescriptorXKey, Wildcard};
use std::{fs, env};
use std::path::Path;
use std::str::FromStr;
use deposit_service::keys::store_wallet_recovery_info;
use chrono::Utc;
use common::types::{TaprootKeyMaterial, CloudBackupStatus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let user_id = args.get(1).unwrap_or(&"1".to_string()).clone();
    let secp = Secp256k1::new();
    let config_path = "config/service_config.toml";
    let config_str = fs::read_to_string(config_path)?;
    let config: Config = toml::from_str(&config_str)?;
    let lsp_xpub = Xpub::from_str(&config.lsp_pubkey)?;
    let trustee_xpub = Xpub::from_str(&config.trustee_pubkey)?;

    // Load or generate mnemonic
    let key_path = format!("data/secrets/user_{}_key.json", user_id);
    let mnemonic = if let Ok(key_json) = fs::read_to_string(&key_path) {
        let key_material: TaprootKeyMaterial = serde_json::from_str(&key_json)?;
        let mnemonic_str = key_material.secret_key
            .ok_or("No secret_key found in existing user_1_key.json")?;
        Mnemonic::parse_in(Language::English, mnemonic_str.trim())?
    } else {
        let generated_result = Mnemonic::generate((WordCount::Words12, Language::English));
        let generated: GeneratedKey<Mnemonic, Tap> = generated_result.map_err(|e| {
            e.map_or("Mnemonic generation failed with no specific error".into(), |err| Box::new(err) as Box<dyn std::error::Error>)
        })?;
        generated.into_key()
    };

    // Derive paths and keys
    let external_path = DerivationPath::from_str("m/84'/1'/0'/0/0")?;
    let internal_path = DerivationPath::from_str("m/84'/1'/0'/1/0")?;
    let unspendable_key_external = "4d54bb9928a0683b7e383de72943b214b0716f58aa54c7ba6bcea2328bc9c768";
    let unspendable_key_internal = "03a34b99f22c790c4e36b2b3c2c35a36db06226e41c692fc82b8b56ac1c540c5";

    // External descriptor
    let user_extended_key: ExtendedKey<Tap> = mnemonic.clone().into_extended_key()?;
    let user_xpriv = user_extended_key
        .into_xprv(Network::Testnet)
        .ok_or("Failed to get private key")?
        .derive_priv(&secp, &external_path)?;
    let user_xpub = Xpub::from_priv(&secp, &user_xpriv);
    let external_descriptor = format!(
        "tr({},multi_a(2,[00000000/84'/1'/0'/0/0]{}/*,[00000000/84'/1'/0'/0/0]{}/*,[00000000/84'/1'/0'/0/0]{}/*))",
        unspendable_key_external, lsp_xpub, trustee_xpub, user_xpub
    );

    // Internal descriptor
    let user_extended_key_internal: ExtendedKey<Tap> = mnemonic.clone().into_extended_key()?;
    let user_xpriv_internal = user_extended_key_internal
        .into_xprv(Network::Testnet)
        .ok_or("Failed to get private key")?
        .derive_priv(&secp, &internal_path)?;
    let user_xpub_internal = Xpub::from_priv(&secp, &user_xpriv_internal);
    let user_desc_xkey_internal = DescriptorXKey {
        origin: Some((Fingerprint::default(), internal_path.clone())),
        xkey: user_xpub_internal,
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
        unspendable_key_internal, lsp_pubkey_internal, trustee_pubkey_internal, user_pubkey_internal
    );

    // Create wallet
    let mut wallet = Wallet::create(external_descriptor.clone(), internal_descriptor.clone())
        .network(Network::Testnet)
        .create_wallet_no_persist()?;
    let address = wallet.reveal_next_address(KeychainKind::External);

    // Store key material
    let data_dir = Path::new("data");
    let mut key_material = TaprootKeyMaterial {
        role: "user".to_string(),
        user_id: Some(user_id.parse::<u32>()?),
        secret_key: Some(mnemonic.to_string()),
        public_key: user_xpub.to_string(),
        network: "testnet".to_string(),
        created_at: Utc::now().timestamp(),
        last_accessed: Utc::now().timestamp(),
        is_taproot_internal: false,
        wallet_descriptor: None, // Set by store_wallet_recovery_info
        lsp_pubkey: None, // Set by store_wallet_recovery_info
        trustee_pubkey: None, // Set by store_wallet_recovery_info
        cloud_backup_status: Some(CloudBackupStatus::NotBackedUp),
        internal_descriptor: Some(internal_descriptor.clone()),
    };
    store_wallet_recovery_info(
        &mut key_material,
        &external_descriptor,
        &lsp_xpub.to_string(),
        &trustee_xpub.to_string(),
        data_dir,
    )?;

    println!("Your deposit address: {}", address.address);
    println!("Recovery document saved to user_recovery.txt—back this up securely!");
    println!("Share this Xpub with LSP: {}", user_xpub);
    Ok(())
}

#[derive(serde::Deserialize)]
struct Config {
    lsp_pubkey: String,
    trustee_pubkey: String,
}
