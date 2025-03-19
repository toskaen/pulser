use bdk_wallet::{Wallet, KeychainKind};
use bdk_esplora::esplora_client::Builder as EsploraBuilder;
use bdk_esplora::EsploraAsyncExt;
use bdk_wallet::bitcoin::Network;
use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::keys::{DescriptorPublicKey, DerivableKey, ExtendedKey};
use bdk_wallet::keys::bip39::{Mnemonic, Language};
use bdk_file_store::Store;
use deposit_service::wallet::secure_init;
use common::error::PulserError;
use bitcoin::ScriptBuf;
use bitcoin::bip32::{Xpriv, Xpub, Fingerprint};
use miniscript::descriptor::DescriptorXKey;
use miniscript::descriptor::Wildcard;
use miniscript::Tap;
use std::fs;
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), PulserError> {
    let data_dir = "data/wallets".to_string();
    let secp = Secp256k1::new();

    let unspendable_key = DescriptorPublicKey::from_str(
        "4d54bb9928a0683b7e383de72943b214b0716f58aa54c7ba6bcea2328bc9c768",
    )?;

    let user_mnemonic_str = secure_init("user1", &data_dir)?;
    let lsp_mnemonic_str = secure_init("lsp", &data_dir)?;
    let trustee_mnemonic_str = secure_init("trustee", &data_dir)?;

    let user_mnemonic = Mnemonic::parse_in(Language::English, &user_mnemonic_str)?;
    let lsp_mnemonic = Mnemonic::parse_in(Language::English, &lsp_mnemonic_str)?;
    let trustee_mnemonic = Mnemonic::parse_in(Language::English, &trustee_mnemonic_str)?;

    let external_path = bitcoin::bip32::DerivationPath::from_str("m/84'/1'/0'/0/0")?;
    let internal_path = bitcoin::bip32::DerivationPath::from_str("m/84'/1'/0'/1/0")?;

    // External keys
    let user_extended_key: ExtendedKey<Tap> = user_mnemonic.clone().into_extended_key()?;
    let lsp_extended_key: ExtendedKey<Tap> = lsp_mnemonic.clone().into_extended_key()?;
    let trustee_extended_key: ExtendedKey<Tap> = trustee_mnemonic.clone().into_extended_key()?;

    let user_xpriv = user_extended_key
        .into_xprv(Network::Testnet)
        .ok_or(PulserError::WalletError("Failed to get private key".to_string()))?
        .derive_priv(&secp, &external_path)?;
    let lsp_xpriv = lsp_extended_key
        .into_xprv(Network::Testnet)
        .ok_or(PulserError::WalletError("Failed to get private key".to_string()))?
        .derive_priv(&secp, &external_path)?;
    let trustee_xpriv = trustee_extended_key
        .into_xprv(Network::Testnet)
        .ok_or(PulserError::WalletError("Failed to get private key".to_string()))?
        .derive_priv(&secp, &external_path)?;

    let user_xpub = Xpub::from_priv(&secp, &user_xpriv);
    let lsp_xpub = Xpub::from_priv(&secp, &lsp_xpriv);
    let trustee_xpub = Xpub::from_priv(&secp, &trustee_xpriv);

    let user_desc_xkey = DescriptorXKey {
        origin: Some((Fingerprint::default(), external_path.clone())),
        xkey: user_xpub,
        derivation_path: bitcoin::bip32::DerivationPath::master(),
        wildcard: Wildcard::Unhardened,
    };
    let lsp_desc_xkey = DescriptorXKey {
        origin: Some((Fingerprint::default(), external_path.clone())),
        xkey: lsp_xpub,
        derivation_path: bitcoin::bip32::DerivationPath::master(),
        wildcard: Wildcard::Unhardened,
    };
    let trustee_desc_xkey = DescriptorXKey {
        origin: Some((Fingerprint::default(), external_path.clone())),
        xkey: trustee_xpub,
        derivation_path: bitcoin::bip32::DerivationPath::master(),
        wildcard: Wildcard::Unhardened,
    };

    let user_pubkey = DescriptorPublicKey::XPub(user_desc_xkey);
    let lsp_pubkey = DescriptorPublicKey::XPub(lsp_desc_xkey);
    let trustee_pubkey = DescriptorPublicKey::XPub(trustee_desc_xkey);

    let external_descriptor = format!(
        "tr({},multi_a(2,{},{},{}))",
        unspendable_key,
        lsp_pubkey.to_string(),
        trustee_pubkey.to_string(),
        user_pubkey.to_string()
    );

    // Internal keys (derive from cloned mnemonics)
    let user_extended_key_internal: ExtendedKey<Tap> = user_mnemonic.into_extended_key()?;
    let lsp_extended_key_internal: ExtendedKey<Tap> = lsp_mnemonic.into_extended_key()?;
    let trustee_extended_key_internal: ExtendedKey<Tap> = trustee_mnemonic.into_extended_key()?;

    let user_xpriv_internal = user_extended_key_internal
        .into_xprv(Network::Testnet)
        .ok_or(PulserError::WalletError("Failed to get private key".to_string()))?
        .derive_priv(&secp, &internal_path)?;
    let lsp_xpriv_internal = lsp_extended_key_internal
        .into_xprv(Network::Testnet)
        .ok_or(PulserError::WalletError("Failed to get private key".to_string()))?
        .derive_priv(&secp, &internal_path)?;
    let trustee_xpriv_internal = trustee_extended_key_internal
        .into_xprv(Network::Testnet)
        .ok_or(PulserError::WalletError("Failed to get private key".to_string()))?
        .derive_priv(&secp, &internal_path)?;

    let user_xpub_internal = Xpub::from_priv(&secp, &user_xpriv_internal);
    let lsp_xpub_internal = Xpub::from_priv(&secp, &lsp_xpriv_internal);
    let trustee_xpub_internal = Xpub::from_priv(&secp, &trustee_xpriv_internal);

    let user_desc_xkey_internal = DescriptorXKey {
        origin: Some((Fingerprint::default(), internal_path.clone())),
        xkey: user_xpub_internal,
        derivation_path: bitcoin::bip32::DerivationPath::master(),
        wildcard: Wildcard::Unhardened,
    };
    let lsp_desc_xkey_internal = DescriptorXKey {
        origin: Some((Fingerprint::default(), internal_path.clone())),
        xkey: lsp_xpub_internal,
        derivation_path: bitcoin::bip32::DerivationPath::master(),
        wildcard: Wildcard::Unhardened,
    };
    let trustee_desc_xkey_internal = DescriptorXKey {
        origin: Some((Fingerprint::default(), internal_path)),
        xkey: trustee_xpub_internal,
        derivation_path: bitcoin::bip32::DerivationPath::master(),
        wildcard: Wildcard::Unhardened,
    };

    let user_pubkey_internal = DescriptorPublicKey::XPub(user_desc_xkey_internal);
    let lsp_pubkey_internal = DescriptorPublicKey::XPub(lsp_desc_xkey_internal);
    let trustee_pubkey_internal = DescriptorPublicKey::XPub(trustee_desc_xkey_internal);

    let internal_descriptor = format!(
        "tr({},multi_a(2,{},{},{}))",
        unspendable_key,
        lsp_pubkey_internal.to_string(),
        trustee_pubkey_internal.to_string(),
        user_pubkey_internal.to_string()
    );

    let mut wallet = Wallet::create(external_descriptor.clone(), internal_descriptor.clone())
        .network(Network::Testnet)
        .create_wallet_no_persist()?;

    let address = wallet.reveal_next_address(KeychainKind::External);
    println!("Multisig Address: {}", address.address);

    fs::write("data/wallets/active_descriptor.txt", &external_descriptor)?;

    let blockchain = EsploraBuilder::new("https://blockstream.info/testnet/api").build_async()?;
    let spks: Vec<ScriptBuf> = wallet
        .all_unbounded_spk_iters()
        .get(&KeychainKind::External)
        .unwrap()
        .clone()
        .map(|(_, spk)| spk)
        .collect();
    let request = bdk_chain::spk_client::SyncRequest::builder().spks(spks).build();
    let response = blockchain.sync(request, 1).await?;
    wallet.apply_update(response)?;

    let balance = wallet.balance();
    println!("Balance: {} sats", balance.total());
    let utxos = wallet.list_unspent().collect::<Vec<_>>();
    println!("UTXOs: {:?}", utxos);

    let mut db = Store::<bdk_wallet::ChangeSet>::open_or_create_new(b"magic", "data/wallets/wallet.dat")?;
    db.append_changeset(&wallet.take_staged().expect("Wallet should have changes after sync"))?;

    Ok(())
}
