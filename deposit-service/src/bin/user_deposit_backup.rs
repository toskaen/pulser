use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::keys::bip39::{Mnemonic, Language, WordCount, Error as Bip39Error};
use bdk_wallet::keys::{GeneratableKey, GeneratedKey, DerivableKey, ExtendedKey};
use bdk_wallet::bitcoin::bip32::{DerivationPath, ExtendedPrivKey, Xpub, Fingerprint};
use bdk_wallet::Wallet;
use bdk_wallet::bitcoin::Network;
use bdk_wallet::keys::DescriptorPublicKey;
use bdk_wallet::miniscript::Tap;
use miniscript::descriptor::{DescriptorXKey, Wildcard};
use std::fs;
use std::path::Path;
use std::str::FromStr;
use deposit_service::keys::{TaprootKeyMaterial, store_wallet_recovery_info, generate_recovery_document, CloudBackupStatus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let secp = Secp256k1::new();

    // Load LSP/trustee Xpubs from service_config.toml
    let config_path = "config/service_config.toml";
    let config_str = fs::read_to_string(config_path)?;
    let config: Config = toml::from_str(&config_str)?;
    let lsp_xpub = Xpub::from_str(&config.lsp_pubkey)?;
    let trustee_xpub = Xpub::from_str(&config.trustee_pubkey)?;

    // Generate or load mnemonic
    let mnemonic = if let Ok(seed) = fs::read_to_string("user_seed.txt") {
        Mnemonic::parse_in(Language::English, &seed.trim())?
    } else {
        let generated: GeneratedKey<Mnemonic, Tap> = Mnemonic::generate((WordCount::Words12, Language::English))
            .map_err(|e| e.unwrap_or(Bip39Error::BadEntropyBitCount(128)))?;
        generated.into_key()
    };
    let seed = mnemonic.to_string();

    // Derive user's Xpub
    let user_extended_key: ExtendedKey<Tap> = mnemonic.clone().into_extended_key()?;
    let external_path = DerivationPath::from_str("m/84'/1'/0'/0/0")?;
    let internal_path = DerivationPath::from_str("m/84'/1'/0'/1/0")?;
    let user_xpriv = user_extended_key
        .into_xprv(Network::Testnet)
        .ok_or("Failed to get private key")?
        .derive_priv(&secp, &external_path)?;
    let user_xpub = Xpub::from_priv(&secp, &user_xpriv);

    // Build descriptors
    let user_desc_xkey = DescriptorXKey {
        origin: Some((Fingerprint::default(), external_path.clone())),
        xkey: user_xpub,
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

    let user_extended_key_internal: ExtendedKey<Tap> = mnemonic.into_extended_key()?;
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
        unspendable_key, lsp_pubkey_internal, trustee_pubkey_internal, user_pubkey_internal
    );

    // Create wallet and get address
    let mut wallet = Wallet::create(external_descriptor.clone(), internal_descriptor.clone())
        .network(Network::Testnet)
        .create_wallet_no_persist()?;
    let address = wallet.reveal_next_address(bdk_wallet::KeychainKind::External);

    // Integrate keys.rs for recovery
    let mut key_material = TaprootKeyMaterial {
        role: "user".to_string(),
        user_id: Some(1), // Hardcoded for now—could be arg
        secret_key: Some(seed.clone()),
        public_key: user_xpub.to_string(),
        network: "testnet".to_string(),
        created_at: chrono::Utc::now().timestamp(),
        last_accessed: chrono::Utc::now().timestamp(),
        is_taproot_internal: false,
        wallet_descriptor: None,
        lsp_pubkey: None,
        trustee_pubkey: None,
        cloud_backup_status: Some(CloudBackupStatus::NotBackedUp),
    };

    store_wallet_recovery_info(&mut key_material, &external_descriptor, &config.lsp_pubkey, &config.trustee_pubkey, Path::new("data"))?;
    let recovery_doc = generate_recovery_document(&key_material)?;
    fs::write("user_recovery.txt", &recovery_doc)?;

    // Output
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
