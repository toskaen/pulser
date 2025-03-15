// In deposit-service/src/wallet.rs
use bdk_wallet::{Wallet, KeychainKind};
use bitcoin::Network;
use common::error::PulserError;
use crate::types::DepositAddressInfo;
use bdk_esplora::esplora_client;

// Define the DepositWallet struct
pub struct DepositWallet {
    pub wallet: Wallet,
    pub blockchain: esplora_client::AsyncClient,
    pub network: Network,
    pub wallet_path: String,
    pub events: Vec<common::types::Event>,
}

pub fn create_taproot_multisig(
    user_pubkey: &str,
    lsp_pubkey: &str,
    trustee_pubkey: &str,
    network: Network,
    blockchain: esplora_client::AsyncClient,
    data_dir: &str,
) -> Result<(DepositWallet, DepositAddressInfo), PulserError> {
    // Validate that all keys are proper X-only pubkeys (32 bytes)
    let keys = [user_pubkey, lsp_pubkey, trustee_pubkey];
    for key in &keys {
        if key.len() != 64 || !key.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(PulserError::InvalidRequest(
                "All public keys must be 32-byte X-only keys (64 hex characters)".to_string()
            ));
        }
    }
    
    // Create a Taproot multisig descriptor (2-of-3)
    let external_descriptor = format!(
        "tr({},multi_a(2,{},{},{}))",
        user_pubkey, user_pubkey, lsp_pubkey, trustee_pubkey
    );
    
    // Use a different internal key for the change descriptor
    let internal_descriptor = format!(
        "tr({},multi_a(2,{},{},{}))",
        lsp_pubkey, user_pubkey, lsp_pubkey, trustee_pubkey
    );
    
    // Create wallet with these descriptors
    let mut wallet = match Wallet::create(external_descriptor.clone(), internal_descriptor)
        .network(network)
        .create_wallet_no_persist() {
            Ok(w) => w,
            Err(e) => return Err(PulserError::WalletError(format!("Failed to create wallet: {}", e))),
        };
        
    // Get address for deposits
    let address_info = wallet.reveal_next_address(KeychainKind::External);
    
    let deposit_info = DepositAddressInfo {
        address: address_info.address.to_string(),
        descriptor: external_descriptor,
        path: "taproot/0".to_string(),
        user_pubkey: user_pubkey.to_string(),
        lsp_pubkey: lsp_pubkey.to_string(),
        trustee_pubkey: trustee_pubkey.to_string(),
    };
    
    keys::store_wallet_recovery_info(
    user_id,
    &external_descriptor,
    user_pubkey,
    lsp_pubkey,
    trustee_pubkey,
    Path::new(data_dir),
)?;
    
    // Generate a unique wallet path
    let wallet_path = format!("{}/wallets/multisig_{}.store", data_dir, user_pubkey);
    
    // Create our wallet wrapper
    let deposit_wallet = DepositWallet {
        wallet,
        blockchain,
        network,
        wallet_path,
        events: Vec::new(),
    };
    
    Ok((deposit_wallet, deposit_info))
}
