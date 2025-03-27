// common/src/types.rs
use serde::{Deserialize, Serialize};
use std::fmt;
use std::collections::HashMap;



/// Generic amount trait - implemented by each service for its own Bitcoin type
pub trait Amount {
    fn to_sats(&self) -> u64;
    fn to_btc(&self) -> f64;
    fn from_sats(sats: u64) -> Self;
    fn from_btc(btc: f64) -> Self;
}

/// Represents a USD amount.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct USD(pub f64);

impl USD {
    pub fn from_f64(value: f64) -> Self { Self(value) }
    
    // Generic conversion - each service implements its own Amount type
    pub fn from_amount<T: Amount>(amount: &T, price: f64) -> Self { 
        Self(amount.to_btc() * price) 
    }
}

impl fmt::Display for USD {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "${:.2}", self.0)
    }
}

/// Event tracking for system activities
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub timestamp: i64,
    pub source: String,
    pub kind: String,
    pub details: String,
}

/// Price information - shared across all services
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceInfo {
    pub raw_btc_usd: f64,
    pub timestamp: i64,
    pub price_feeds: HashMap<String, f64>,
}

/// Deposit address information - serializable contract between services
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DepositAddressInfo {
    pub address: String,
    pub user_id: u32,
    pub multisig_type: String, // "2-of-3" etc.
    pub participants: Vec<String>, // pubkeys as hex strings
    pub descriptor: Option<String>, // Optional descriptor for wallet creation
    pub path: String,
}

/// StableChannel information - serializable contract between services
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelInfo {
    pub user_id: u32,
    pub channel_id: String,
    pub is_stable_receiver: bool,
    pub counterparty: String, // pubkey as hex string
    pub expected_usd: f64,
    pub expected_sats: u64,
    pub stable_receiver_sats: u64,
    pub stable_provider_sats: u64,
    pub timestamp: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaprootKeyMaterial {
    pub role: String,
    pub user_id: Option<u32>,
    pub secret_key: Option<String>,
    pub public_key: String,
    pub network: String,
    pub created_at: i64,
    pub last_accessed: i64,
    pub is_taproot_internal: bool,
    pub wallet_descriptor: Option<String>,
    pub lsp_pubkey: Option<String>,
    pub trustee_pubkey: Option<String>,
    pub cloud_backup_status: Option<CloudBackupStatus>,
    pub internal_descriptor: Option<String>, // Add this
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CloudBackupStatus {
    NotBackedUp,
    BackedUp,
    Failed,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Utxo {
    pub txid: String,
    pub vout: u32,
    pub amount: u64,
    pub confirmations: u32,
    pub script_pubkey: String,
    pub height: Option<u32>,
    pub usd_value: Option<USD>, // Static USD at deposit
}
