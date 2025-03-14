// common/src/lib.rs
pub mod error;
pub mod price_feed;
pub mod utils; // Add this line

// Re-export the minimal set of types needed
pub use error::PulserError;

// Define basic types in this file (instead of in a separate module)
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Event tracking for system activities
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub timestamp: i64,
    pub source: String,
    pub kind: String,
    pub details: String,
}

/// Generic amount trait - implemented by each service for its own Bitcoin type
pub trait Amount {
    fn to_sats(&self) -> u64;
    fn to_btc(&self) -> f64;
    fn from_sats(sats: u64) -> Self;
    fn from_btc(btc: f64) -> Self;
}

/// Represents a USD amount.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct USD(pub f64);

impl USD {
    pub fn from_f64(value: f64) -> Self { Self(value) }
    
    // Generic conversion - each service implements its own Amount type
    pub fn from_amount<T: Amount>(amount: &T, price: f64) -> Self { 
        Self(amount.to_btc() * price) 
    }
}

/// Price information - common across all services
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceInfo {
    pub raw_btc_usd: f64,
    pub synthetic_price: Option<f64>,
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
}

/// UTXO information - serializable between services
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UtxoInfo {
    pub txid: String,
    pub vout: u32,
    pub amount_sats: u64,
    pub confirmations: u32,
    pub script_pubkey: String,
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

/// Hedge position information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HedgePositionInfo {
    pub user_id: u32,
    pub entry_price: f64,
    pub position_btc: f64,
    pub order_id: String,
    pub is_channel: bool,
}

// Helper function to get current timestamp
pub fn now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_secs() as i64
}

// Helper to format timestamps
pub fn format_timestamp(ts: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(ts, 0)
        .unwrap_or_else(|| chrono::Utc::now())
        .format("%Y-%m-%d %H:%M:%S UTC").to_string()
}
