use bitcoin::secp256k1::{Secp256k1, SecretKey, XOnlyPublicKey};
use bitcoin::Network;
use common::error::PulserError;
use common::types::{TaprootKeyMaterial, CloudBackupStatus}; // Import both
use serde::{Serialize, Deserialize};
use rand::rngs::OsRng;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::str::FromStr;
use log::info;

// Remove local CloudBackupStatus and TaprootKeyMaterial definitions

pub fn generate_taproot_keypair() -> Result<(SecretKey, XOnlyPublicKey, String), PulserError> {
    let secp = Secp256k1::new();
    let secret_key = SecretKey::new(&mut OsRng);
    let keypair = bitcoin::secp256k1::Keypair::from_secret_key(&secp, &secret_key);
    let (xonly_pubkey, _parity) = XOnlyPublicKey::from_keypair(&keypair);
    let pubkey_hex = hex::encode(xonly_pubkey.serialize());
    Ok((secret_key, xonly_pubkey, pubkey_hex))
}

pub fn store_key_material(key_material: &TaprootKeyMaterial, data_dir: &Path) -> Result<(), PulserError> {
    let secrets_dir = data_dir.join("secrets");
    fs::create_dir_all(&secrets_dir)?;
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&secrets_dir, fs::Permissions::from_mode(0o700))?;
    }

    let filename = match key_material.user_id {
        Some(id) => format!("{}_{}_key.json", key_material.role, id),
        None => format!("{}_key.json", key_material.role),
    };
    let key_path = secrets_dir.join(filename);

    let json = serde_json::to_string_pretty(key_material)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&key_path)?;
    file.write_all(json.as_bytes())?;
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&key_path, fs::Permissions::from_mode(0o600))?;
    }
    info!("Key material stored at {}", key_path.display());
    Ok(())
}

pub fn load_key_material(role: &str, user_id: Option<u32>, data_dir: &Path) -> Result<TaprootKeyMaterial, PulserError> {
    let secrets_dir = data_dir.join("secrets");
    let filename = match user_id {
        Some(id) => format!("{}_{}_key.json", role, id),
        None => format!("{}_key.json", role),
    };
    let key_path = secrets_dir.join(filename);

    if !key_path.exists() {
        return Err(PulserError::UserNotFound(format!("Key file not found for {}", role)));
    }

    let mut file = File::open(&key_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let mut key_material: TaprootKeyMaterial = serde_json::from_str(&contents)?;
    key_material.last_accessed = chrono::Utc::now().timestamp();
    store_key_material(&key_material, data_dir)?;
    Ok(key_material)
}

pub fn store_wallet_recovery_info(
    key_material: &mut TaprootKeyMaterial,
    descriptor: &str,
    lsp_pubkey: &str,
    trustee_pubkey: &str,
    data_dir: &Path,
) -> Result<(), PulserError> {
    key_material.wallet_descriptor = Some(descriptor.to_string());
    key_material.lsp_pubkey = Some(lsp_pubkey.to_string());
    key_material.trustee_pubkey = Some(trustee_pubkey.to_string());
    store_key_material(key_material, data_dir)?;
    Ok(())
}

pub fn generate_recovery_document(key_material: &TaprootKeyMaterial) -> Result<String, PulserError> {
    if key_material.wallet_descriptor.is_none() {
        return Err(PulserError::InvalidRequest("Wallet descriptor not available".to_string()));
    }

    let backup_status_str = match &key_material.cloud_backup_status {
        Some(CloudBackupStatus::NotBackedUp) => "Not backed up to cloud",
        Some(CloudBackupStatus::BackedUp) => "Backed up to cloud",
        Some(CloudBackupStatus::Failed) => "Backup failed",
        None => "Backup status unknown",
    };

    let doc = format!(
        "# PULSER WALLET RECOVERY DOCUMENT\n\n\
        IMPORTANT: KEEP THIS DOCUMENT SECURE\n\n\
        User ID: {}\n\
        Network: {}\n\
        Created: {}\n\n\
        ## Wallet Information\n\
        Wallet Descriptor: {}\n\n\
        ## Participants\n\
        User Public Key: {}\n\
        LSP Public Key: {}\n\
        Trustee Public Key: {}\n\n\
        ## Recovery Seed\n\
        {}\n\n\
        ## Cloud Backup Status\n\
        {}\n\n\
        ## Recovery Instructions\n\
        1. Use this descriptor with a compatible wallet (BlueWallet, Sparrow, Electrum)\n\
        2. Import the descriptor and seed if available\n\
        3. Requires 2-of-3 signatures to spend\n\
        4. Contact support if needed\n",
        key_material.user_id.unwrap_or(0),
        key_material.network,
        key_material.created_at,
        key_material.wallet_descriptor.as_ref().unwrap(),
        key_material.public_key,
        key_material.lsp_pubkey.as_ref().unwrap_or(&"Not Available".to_string()),
        key_material.trustee_pubkey.as_ref().unwrap_or(&"Not Available".to_string()),
        key_material.secret_key.as_ref().unwrap_or(&"Not Available".to_string()),
        backup_status_str,
    );
    Ok(doc)
}
