use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::keys::bip39::{Mnemonic, Language, WordCount, Error as Bip39Error};
use bdk_wallet::keys::{GeneratableKey, GeneratedKey, DerivableKey, ExtendedKey};
use bdk_wallet::bitcoin::bip32::{DerivationPath, ExtendedPrivKey, Xpub};
use bdk_wallet::miniscript::Tap;
use bitcoin::Network; // Added this
use std::fs;
use std::str::FromStr;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let secp = Secp256k1::new();
    let generated: GeneratedKey<Mnemonic, Tap> = Mnemonic::generate((WordCount::Words12, Language::English))
        .map_err(|e| e.unwrap_or_else(|| Bip39Error::BadEntropyBitCount(128)))?;
    let mnemonic = generated.into_key();
    let seed = mnemonic.to_string();

    let extended_key: ExtendedKey<Tap> = mnemonic.into_extended_key()?;
    let path = DerivationPath::from_str("m/84'/1'/0'/0/0")?;
    let xpriv = extended_key
        .into_xprv(Network::Testnet)
        .ok_or("Failed to get xpriv")?
        .derive_priv(&secp, &path)?;
    let xpub = Xpub::from_priv(&secp, &xpriv);

    println!("Your seed (keep secret, back up to cloud): {}", seed);
    println!("Your Xpub (share with service): {}", xpub);
    fs::write("seed.txt", seed)?;

    Ok(())
}
