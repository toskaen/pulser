// deposit-service/src/bin/test_multisig.rs
use bdk_wallet::{Wallet, KeychainKind};
use bdk_esplora::{esplora_client, EsploraBlockchain};
use bdk_wallet::bitcoin::{Network, Amount, Address};
use bdk_wallet::SignOptions;
use bdk_wallet::bitcoin::secp256k1::{Secp256k1, SecretKey};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing Taproot Wallet Type");
    println!("===========================");

    let network = Network::Testnet;
    let user_pubkey = "92e0034c75c2d658220ee4c0064fdfa7f21abf2ec88bedcdfddfe94ad68c3fc2";
    let lsp_pubkey = "8f6a89e914323ee1d8b235e63dd68ac8d79c13bc7c7ff4aa042671f63e8d9bc2";
    let trustee_pubkey = "c64776af3b1164e3dade00d03196a1874a0474f7a9d5cb3599c80836bba8d358";

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

    let mut wallet = Wallet::create(external_descriptor.clone(), internal_descriptor)
        .network(network)
        .create_wallet_no_persist()?;

    let address_info = wallet.reveal_next_address(KeychainKind::External);
    println!("Address: {}", address_info.address);
    assert_eq!(address_info.address.to_string(), "tb1p66vxvmud75sadhmcdd0kdu4e8j3ucycw4j0rzj6gn65vwyax8p3qy4xmxs");

    let blockchain = EsploraBlockchain::new(
        esplora_client::Builder::new("https://blockstream.info/testnet/api").build_async()?,
        20,
    );

    // Sync to see deposit
    let update = blockchain.fetch_full_update(&wallet, 20).await?;
    wallet.apply_update(update)?;
    let balance = wallet.balance();
    println!("Confirmed Balance: {} sats", balance.confirmed);
    if balance.confirmed == 0 {
        println!("No funds detected at this address!");
        return Ok(());
    }

    // Attempt spend with ONE key (keypath test)
    let destination = "tb1qtestdestination"; // Replace with a real Testnet address
    let amount = balance.confirmed / 2; // Spend half
    let user_privkey = "REPLACE_WITH_92e0034c_PRIVATE_KEY"; // Need this!

    let mut tx_builder = wallet.build_tx();
    tx_builder
        .add_recipient(Address::from_str(destination)?.require_network(network)?, Amount::from_sat(amount))
        .enable_rbf();

    let mut psbt = tx_builder.finish()?;
    let secp = Secp256k1::new();
    let user_key = SecretKey::from_str(user_privkey)?;

    // Sign with only user key
    let signed = wallet.sign(&mut psbt, SignOptions::default(), &secp, |_, _| Some(user_key))?;
    println!("Signed with one key: {}", signed);

    let finalized = wallet.finalize_psbt(&mut psbt, SignOptions::default())?;
    println!("Finalized with one key: {}", finalized);

    if finalized {
        println!("Wallet is KEY PATH only — multisig not enforced!");
        let tx = psbt.extract_tx()?;
        blockchain.broadcast(&tx).await?;
        println!("Spend TxID: {}", tx.txid());
    } else {
        println!("Wallet requires MULTISIG — one key isn’t enough.");
    }

    Ok(())
}
