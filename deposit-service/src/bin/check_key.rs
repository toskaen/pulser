// deposit-service/src/bin/check_key.rs
use std::fs;
use bdk_wallet::bitcoin::secp256k1::{Secp256k1, SecretKey};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let privkey_bytes = fs::read("data/wallets/secrets/key_user2.bin")?; // Adjust path if in user1 subfolder
    let privkey = SecretKey::from_slice(&privkey_bytes)?;
    let secp = Secp256k1::new();
    let pubkey = privkey.public_key(&secp).x_only_public_key().0;
    println!("Private Key (hex): {}", privkey.display_secret());
    println!("Derived Pubkey: {}", pubkey);
    Ok(())
}
