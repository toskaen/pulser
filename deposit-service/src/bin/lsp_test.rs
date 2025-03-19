// deposit-service/src/bin/lsp_test.rs
use bdk_wallet::{Wallet, KeychainKind}; // Correct import
use bdk_wallet::bitcoin::Network;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    println!("LSP Monitor Address: {}", address_info.address);

    Ok(())
}
