use bdk_wallet::{Wallet, KeychainKind};
use bdk_esplora::esplora_client::Builder as EsploraBuilder;
use bdk_esplora::EsploraAsyncExt;
use bdk_wallet::bitcoin::{Network, Amount, Address};
use bdk_wallet::SignOptions;
use bdk_wallet::bitcoin::secp256k1::{Secp256k1, SecretKey};
use serde_json;
use std::fs;
use std::str::FromStr;
use hex;
use common::error::PulserError;
use bitcoin::ScriptBuf;

#[derive(serde::Deserialize)]
struct KeyData {
    private_key: String,
}

#[tokio::main]
async fn main() -> Result<(), PulserError> {
    println!("Sweeping User1/User2 to New Multisig");
    println!("====================================");

    let network = Network::Testnet;
    let secp = Secp256k1::new();

    // Load keys from JSON
    let user1_data: KeyData = serde_json::from_str(&fs::read_to_string("test_data/user_1_key.json")?)?;
    let user1_privkey = SecretKey::from_slice(&hex::decode(&user1_data.private_key)?)?;
    let user1_pubkey = hex::encode(user1_privkey.public_key(&secp).x_only_public_key().0.serialize());

    let lsp_data: KeyData = serde_json::from_str(&fs::read_to_string("test_data/lsp_key.json")?)?;
    let lsp_privkey = SecretKey::from_slice(&hex::decode(&lsp_data.private_key)?)?;
    let lsp_pubkey = hex::encode(lsp_privkey.public_key(&secp).x_only_public_key().0.serialize());

    // User1 wallet
    let user1_desc = format!("tr({})", user1_pubkey);
    let mut user1_wallet = Wallet::create(user1_desc.clone(), user1_desc)
        .network(network)
        .create_wallet_no_persist()?;

    // User2 wallet (using LSP key as "User2")
    let user2_desc = format!("tr({})", lsp_pubkey);
    let mut user2_wallet = Wallet::create(user2_desc.clone(), user2_desc)
        .network(network)
        .create_wallet_no_persist()?;

    // Blockchain
    let blockchain = EsploraBuilder::new("https://blockstream.info/testnet/api").build_async()?;

    // Sync User1
    let spks: Vec<ScriptBuf> = user1_wallet.all_unbounded_spk_iters().get(&KeychainKind::External).unwrap().clone().map(|(_, spk)| spk).collect();
    let request = bdk_chain::spk_client::SyncRequest::builder().spks(spks).build();
    user1_wallet.apply_update(blockchain.sync(request, 1).await?)?;
    let user1_balance = user1_wallet.balance();
    println!("User1 Balance: {} sats", user1_balance.total());

    // Sync User2
    let spks: Vec<ScriptBuf> = user2_wallet.all_unbounded_spk_iters().get(&KeychainKind::External).unwrap().clone().map(|(_, spk)| spk).collect();
    let request = bdk_chain::spk_client::SyncRequest::builder().spks(spks).build();
    user2_wallet.apply_update(blockchain.sync(request, 1).await?)?;
    let user2_balance = user2_wallet.balance();
    println!("User2 Balance: {} sats", user2_balance.total());

    // New multisig address
    let new_address = Address::from_str("tb1pr6czzxwuhf0sx53cr838t94gsrzg60c9fw39xvgsh8hcqk87ethqxp40jh")?.require_network(network)?;

    // Spend User1
    if user1_balance.total() > Amount::from_sat(0) {
        let mut tx_builder = user1_wallet.build_tx();
        tx_builder.add_recipient(new_address.clone(), user1_balance.total() - Amount::from_sat(1000));
        let mut psbt = tx_builder.finish()?;
        psbt.inputs_mut().iter_mut().for_each(|input| {
            let sighash = psbt.unsigned_tx.input[input.index].witness_script[..].to_vec();
            input.tap_key_sig = Some(bdk_wallet::bitcoin::taproot::Signature {
                sig: secp.sign_schnorr_no_aux_rand(&sighash, user1_privkey),
                hash_ty: bdk_wallet::bitcoin::sighash::TapSighashType::Default,
            });
        });
        let finalized = user1_wallet.finalize_psbt(&mut psbt, SignOptions::default())?;
        if finalized {
            let tx = psbt.extract_tx()?;
            blockchain.broadcast(&tx).await?;
            println!("User1 Spend TxID: {}", tx.compute_txid());
        } else {
            println!("User1 failed to finalize PSBT");
        }
    } else {
        println!("User1 has no funds to sweep");
    }

    // Spend User2
    if user2_balance.total() > Amount::from_sat(0) {
        let mut tx_builder = user2_wallet.build_tx();
        tx_builder.add_recipient(new_address, user2_balance.total() - Amount::from_sat(1000));
        let mut psbt = tx_builder.finish()?;
        psbt.inputs_mut().iter_mut().for_each(|input| {
            let sighash = psbt.unsigned_tx.input[input.index].witness_script[..].to_vec();
            input.tap_key_sig = Some(bdk_wallet::bitcoin::taproot::Signature {
                sig: secp.sign_schnorr_no_aux_rand(&sighash, lsp_privkey),
                hash_ty: bdk_wallet::bitcoin::sighash::TapSighashType::Default,
            });
        });
        let finalized = user2_wallet.finalize_psbt(&mut psbt, SignOptions::default())?;
        if finalized {
            let tx = psbt.extract_tx()?;
            blockchain.broadcast(&tx).await?;
            println!("User2 Spend TxID: {}", tx.compute_txid());
        } else {
            println!("User2 failed to finalize PSBT");
        }
    } else {
        println!("User2 has no funds to sweep");
    }

    Ok(())
}
