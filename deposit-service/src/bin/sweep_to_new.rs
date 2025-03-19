// deposit-service/src/bin/sweep_to_new.rs
use bdk_wallet::{Wallet, KeychainKind};
use bdk_esplora::{esplora_client, EsploraBlockchain};
use bdk_wallet::bitcoin::{Network, Amount, Address};
use bdk_wallet::SignOptions;
use bdk_wallet::bitcoin::secp256k1::{Secp256k1, SecretKey};
use serde_json;
use std::fs;
use hex;

#[derive(serde::Deserialize)]
struct KeyData {
    private_key: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Sweeping User1/User2 to New Multisig");
    println!("====================================");

    let network = Network::Testnet;
    let secp = Secp256k1::new();

    // Load LSP and Trustee keys (assuming JSON format)
    let lsp_json = fs::read_to_string("test_data/lsp_key.json")?;
    let lsp_data: KeyData = serde_json::from_str(&lsp_json)?;
    let lsp_privkey = SecretKey::from_slice(&hex::decode(&lsp_data.private_key)?)?;
    let lsp_pubkey = lsp_privkey.public_key(&secp).x_only_public_key().0;

    let trustee_json = fs::read_to_string("test_data/trustee_key.json")?;
    let trustee_data: KeyData = serde_json::from_str(&trustee_json)?;
    let trustee_privkey = SecretKey::from_slice(&hex::decode(&trustee_data.private_key)?)?;
    let trustee_pubkey = trustee_privkey.public_key(&secp).x_only_public_key().0;

    // User1 wallet (assuming static key for simplicity)
    let user1_privkey = SecretKey::from_slice(&hex::decode("201d682188d35d8cd6f0cf059d1b9625dc594a844295c2f61884ee809a6af3a3")?)?;
    let user1_pubkey = "78ea8d868b3e0a4c027a6f65b8c653eb98abc2e87493252a2ea3f633d0044bcf";
    let user1_descriptor = format!("tr({})", user1_pubkey);
    let mut user1_wallet = Wallet::create(user1_descriptor.clone(), user1_descriptor)
        .network(network)
        .create_wallet_no_persist()?;

    // User2 wallet (assuming multisig from test_data/user2_desc.txt)
    let user2_pubkey = "5c1e6c90af592b28ed2ce1ce50663be9ba9deea56601cdaf8ee55cbdbbec3d91";
    let user2_descriptor = format!(
        "tr({},multi_a(2,{},{},{}))",
        user2_pubkey, user2_pubkey, lsp_pubkey, trustee_pubkey
    );
    let mut user2_wallet = Wallet::create(user2_descriptor.clone(), user2_descriptor)
        .network(network)
        .create_wallet_no_persist()?;

    let blockchain = EsploraBlockchain::new(
        esplora_client::Builder::new("https://blockstream.info/testnet/api").build_async()?,
        20,
    );

    // Sync User1
    user1_wallet.sync(&blockchain, Default::default()).await?;
    let user1_balance = user1_wallet.balance();
    println!("User1 Balance: {} sats", user1_balance.total());

    // Sync User2
    user2_wallet.sync(&blockchain, Default::default()).await?;
    let user2_balance = user2_wallet.balance();
    println!("User2 Balance: {} sats", user2_balance.total());

    // New multisig address (replace with output from create_multisig.rs)
    let new_address = "tb1pnewmultisig..."; // Replace with actual address

    // Spend User1
    if user1_balance.total() > 0 {
        let mut tx_builder = user1_wallet.build_tx();
        tx_builder
            .add_recipient(Address::from_str(new_address)?.require_network(network)?, user1_balance.total() - 1000)
            .enable_rbf();
        let mut psbt = tx_builder.finish()?;
        user1_wallet.sign(&mut psbt, SignOptions::default(), &secp, |_, _| Some(user1_privkey))?;
        let finalized = user1_wallet.finalize_psbt(&mut psbt, SignOptions::default())?;
        if finalized {
            let tx = psbt.extract_tx()?;
            blockchain.broadcast(&tx).await?;
            println!("User1 Spend TxID: {}", tx.txid());
        }
    }

    // Spend User2 (multisig)
    if user2_balance.total() > 0 {
        let mut tx_builder = user2_wallet.build_tx();
        tx_builder
            .add_recipient(Address::from_str(new_address)?.require_network(network)?, user2_balance.total() - 1000)
            .enable_rbf();
        let mut psbt = tx_builder.finish()?;
        user2_wallet.sign(&mut psbt, SignOptions::default(), &secp, |_, _| Some(lsp_privkey))?;
        user2_wallet.sign(&mut psbt, SignOptions::default(), &secp, |_, _| Some(trustee_privkey))?;
        let finalized = user2_wallet.finalize_psbt(&mut psbt, SignOptions::default())?;
        if finalized {
            let tx = psbt.extract_tx()?;
            blockchain.broadcast(&tx).await?;
            println!("User2 Spend TxID: {}", tx.txid());
        } else {
            println!("User2 failed — check LSP/Trustee keys or descriptor.");
        }
    }

    Ok(())
}
