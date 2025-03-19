use bdk_wallet::{Wallet, KeychainKind};
use bdk_esplora::esplora_client::{AsyncClient, Builder as EsploraBuilder};
use bdk_esplora::EsploraAsyncExt;
use bdk_wallet::bitcoin::Network;
use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::descriptor::IntoWalletDescriptor; // Fixed import
use bdk_wallet::keys::{DescriptorPublicKey, GeneratableKey, GeneratedKey};
use bdk_wallet::keys::bip39::{Mnemonic, WordCount, Language};
use bdk_file_store::Store;
use deposit_service::wallet::secure_init;
use common::error::PulserError;
use bitcoin::ScriptBuf;
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

    // Derive public keys with IntoWalletDescriptor
    let (user_pubkey, _) = (user_mnemonic.clone(), None)
        .into_wallet_descriptor(&secp, "m/84'/1'/0'/0/0")?;
    let (lsp_pubkey, _) = (lsp_mnemonic.clone(), None)
        .into_wallet_descriptor(&secp, "m/84'/1'/0'/0/0")?;
    let (trustee_pubkey, _) = (trustee_mnemonic.clone(), None)
        .into_wallet_descriptor(&secp, "m/84'/1'/0'/0/0")?;

    let external_descriptor = format!(
        "tr({},multi_a(2,{},{},{}))",
        unspendable_key,
        lsp_pubkey.to_string(),
        trustee_pubkey.to_string(),
        user_pubkey.to_string()
    );
    let internal_descriptor = external_descriptor.clone();

    // Create wallet without persistence
    let mut wallet = Wallet::create(external_descriptor.clone(), internal_descriptor)
        .network(Network::Testnet)
        .create_wallet_no_persist()?;

    let address = wallet.reveal_next_address(KeychainKind::External);
    println!("Multisig Address: {}", address.address);

    fs::write("data/wallets/active_descriptor.txt", external_descriptor)?;

    let blockchain = EsploraBuilder::new("https://blockstream.info/testnet/api").build_async()?;
    let spks: Vec<ScriptBuf> = wallet.all_unbounded_spk_iters()
        .get(&KeychainKind::External).unwrap().clone()
        .map(|(_, spk)| spk).collect();
    let request = bdk_chain::spk_client::SyncRequest::builder().spks(spks).build();
    let response = blockchain.sync(request, 1).await?;
    wallet.apply_update(response.into())?;
a
    let balance = wallet.balance();
    println!("Balance: {} sats", balance.total());
    let utxos = wallet.list_unspent().collect::<Vec<_>>();
    println!("UTXOs: {:?}", utxos);

    // Optional persistence
    let mut db = Store::<bdk_wallet::ChangeSet>::open_or_create_new(b"magic", "data/wallets/wallet.dat")?;
    db.persist(&mut wallet)?;

    Ok(())
}
