// In deposit-service/src/keys.rs
use bitcoin::secp256k1::{Secp256k1, SecretKey, PublicKey, XOnlyPublicKey};
use bitcoin::Network;
use common::error::PulserError;
use serde::{Serialize, Deserialize};
use rand::rngs::OsRng;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use log::{info, warn};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum CloudBackupStatus {
    NotBackedUp,
    BackedUpToApple,
    BackedUpToGoogle,
    BackedUpToCustom(String),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TaprootKeyMaterial {
    pub role: String,
    pub user_id: Option<u32>,
    pub secret_key: Option<String>,       // Store as hex string, only present if we own this key
    pub public_key: String,               // X-only public key as hex
    pub network: String,
    pub created_at: i64,
    pub last_accessed: i64,
    pub is_taproot_internal: bool,        // Whether this is a key in the key path (internal)
    pub wallet_descriptor: Option<String>, // Complete wallet descriptor for recovery
    pub lsp_pubkey: Option<String>,       // LSP's public key
    pub trustee_pubkey: Option<String>,   // Trustee's public key
    pub cloud_backup_status: Option<CloudBackupStatus>, // Track backup status
}

/// Generate a new keypair for Taproot operations
pub fn generate_taproot_keypair() -> Result<(SecretKey, XOnlyPublicKey, String), PulserError> {
    let secp = Secp256k1::new();
    
    // Generate a new private key
    let secret_key = SecretKey::new(&mut OsRng);
    
    // Create a keypair
    let keypair = bitcoin::secp256k1::Keypair::from_secret_key(&secp, &secret_key);
    
    // Convert to X-only for Taproot
    let (xonly_pubkey, _parity) = XOnlyPublicKey::from_keypair(&keypair);
    
    // Get hex format for storage
    let pubkey_hex = hex::encode(xonly_pubkey.serialize());
    
    Ok((secret_key, xonly_pubkey, pubkey_hex))
}

/// Securely store key material
pub fn store_key_material(
    key_material: &TaprootKeyMaterial,
    data_dir: &Path,
) -> Result<(), PulserError> {
    // Create secrets directory with appropriate permissions
    let secrets_dir = data_dir.join("secrets");
    fs::create_dir_all(&secrets_dir)
        .map_err(|e| PulserError::StorageError(format!("Failed to create secrets directory: {}", e)))?;
    
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o700); // Owner read/write/execute only
        fs::set_permissions(&secrets_dir, perms)
            .map_err(|e| PulserError::StorageError(format!("Failed to set directory permissions: {}", e)))?;
    }
    
    // Generate filename based on role
    let filename = match key_material.user_id {
        Some(id) => format!("{}_{}_key.json", key_material.role, id),
        None => format!("{}_key.json", key_material.role),
    };
    
    let key_path = secrets_dir.join(filename);
    
    // Write to file
    let json = serde_json::to_string_pretty(key_material)
        .map_err(|e| PulserError::StorageError(format!("Failed to serialize key material: {}", e)))?;
    
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&key_path)
        .map_err(|e| PulserError::StorageError(format!("Failed to create key file: {}", e)))?;
    
    file.write_all(json.as_bytes())
        .map_err(|e| PulserError::StorageError(format!("Failed to write key file: {}", e)))?;
    
    // Set proper permissions
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o600); // Owner read/write only
        fs::set_permissions(&key_path, perms)
            .map_err(|e| PulserError::StorageError(format!("Failed to set file permissions: {}", e)))?;
    }
    
    info!("Key material stored securely at {}", key_path.display());
    Ok(())
}

/// Load key material from secure storage
pub fn load_key_material(
    role: &str,
    user_id: Option<u32>,
    data_dir: &Path,
) -> Result<TaprootKeyMaterial, PulserError> {
    // Determine the key path
    let secrets_dir = data_dir.join("secrets");
    
    // Generate filename based on role
    let filename = match user_id {
        Some(id) => format!("{}_{}_key.json", role, id),
        None => format!("{}_key.json", role),
    };
    
    let key_path = secrets_dir.join(filename);
    
    // Check if key file exists
    if !key_path.exists() {
        return Err(PulserError::UserNotFound(format!("Key file not found for {}", role)));
    }
    
    // Read the file
    let mut file = File::open(&key_path)
        .map_err(|e| PulserError::StorageError(format!("Failed to open key file: {}", e)))?;
    
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .map_err(|e| PulserError::StorageError(format!("Failed to read key file: {}", e)))?;
    
    // Parse the key material
    let key_material: TaprootKeyMaterial = serde_json::from_str(&contents)
        .map_err(|e| PulserError::StorageError(format!("Failed to parse key file: {}", e)))?;
    
    // Update last accessed time
    let mut updated = key_material.clone();
    updated.last_accessed = common::utils::now_timestamp();
    
    // Write back the updated key material
    store_key_material(&updated, data_dir)?;
    
    Ok(updated)
}

/// Load or generate key material
pub fn load_or_generate_key_material(
    role: &str,
    user_id: Option<u32>,
    network: Network,
    data_dir: &Path,
    is_taproot_internal: bool,
) -> Result<TaprootKeyMaterial, PulserError> {
    // Try to load existing key material
    match load_key_material(role, user_id, data_dir) {
        Ok(key_material) => Ok(key_material),
        Err(PulserError::UserNotFound(_)) => {
            // Generate new key material
            let (secret_key, _xonly_pubkey, pubkey_hex) = generate_taproot_keypair()?;
            
            let key_material = TaprootKeyMaterial {
                role: role.to_string(),
                user_id,
                secret_key: Some(hex::encode(secret_key.secret_bytes())),
                public_key: pubkey_hex,
                network: network.to_string(),
                created_at: common::utils::now_timestamp(),
                last_accessed: common::utils::now_timestamp(),
                is_taproot_internal,
                wallet_descriptor: None,
                lsp_pubkey: None,
                trustee_pubkey: None,
                cloud_backup_status: Some(CloudBackupStatus::NotBackedUp),
            };
            
            // Store the key material
            store_key_material(&key_material, data_dir)?;
            
            Ok(key_material)
        },
        Err(e) => Err(e),
    }
}

pub fn store_wallet_recovery_info(
    key_material: &mut TaprootKeyMaterial,
    descriptor: &str,
    lsp_pubkey: &str,
    trustee_pubkey: &str,
    data_dir: &Path,
) -> Result<(), PulserError> {
    // Update key material with wallet info
    key_material.wallet_descriptor = Some(descriptor.to_string());
    key_material.lsp_pubkey = Some(lsp_pubkey.to_string());
    key_material.trustee_pubkey = Some(trustee_pubkey.to_string());
    
    // Save updated key material
    store_key_material(key_material, data_dir)?;
    
    Ok(())
}

pub fn generate_recovery_document(
    key_material: &TaprootKeyMaterial,
) -> Result<String, PulserError> {
    if key_material.wallet_descriptor.is_none() {
        return Err(PulserError::InvalidRequest("Wallet descriptor not available for recovery".to_string()));
    }
    
    let backup_status_str = match &key_material.cloud_backup_status {
        Some(CloudBackupStatus::NotBackedUp) => "Not backed up to cloud",
        Some(CloudBackupStatus::BackedUpToApple) => "Backed up to Apple iCloud",
        Some(CloudBackupStatus::BackedUpToGoogle) => "Backed up to Google Drive",
        Some(CloudBackupStatus::BackedUpToCustom(s)) => &format!("Backed up to custom provider: {}", s)[..],
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
        ## Recovery Seed (If Generated by Pulser)\n\
        {}\n\n\
        ## Cloud Backup Status\n\
        {}\n\n\
        ## Recovery Instructions\n\
        1. Use this descriptor with a compatible wallet (BlueWallet, Sparrow, Electrum)\n\
        2. Import the descriptor and private key if available\n\
        3. The wallet requires 2-of-3 signatures to spend funds\n\
        4. Contact support if you need assistance\n",
        key_material.user_id.unwrap_or(0),
        key_material.network,
        common::utils::format_timestamp(key_material.created_at),
        key_material.wallet_descriptor.as_ref().unwrap_or(&"Not Available".to_string()),
        key_material.public_key,
        key_material.lsp_pubkey.as_ref().unwrap_or(&"Not Available".to_string()),
        key_material.trustee_pubkey.as_ref().unwrap_or(&"Not Available".to_string()),
        match &key_material.secret_key {
            Some(_) => "PRIVATE KEY AVAILABLE (Contact support for secure recovery)",
            None => "No private key available on this device",
        },
        backup_status_str,
    );
    
    Ok(doc)
}
