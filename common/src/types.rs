// common/src/types.rs
// Existing imports
use serde::{Deserialize, Serialize};
use std::fmt;
use std::collections::HashMap;
use std::str::FromStr;

/// Generic amount trait - implemented by each service for its own Bitcoin type
pub trait Amount {
    fn to_sats(&self) -> u64;
    fn to_btc(&self) -> f64;
    fn from_sats(sats: u64) -> Self;
    fn from_btc(btc: f64) -> Self;
}

/// Represents a Bitcoin amount.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bitcoin {
    pub sats: u64,
}

impl Bitcoin {
    pub fn from_sats(sats: u64) -> Self { Self { sats } }
    pub fn to_btc(&self) -> f64 { self.sats as f64 / 100_000_000.0 }
}

impl fmt::Display for Bitcoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:10.8}btc", self.to_btc())
    }
}

impl Amount for Bitcoin {
    fn to_sats(&self) -> u64 { self.sats }
    fn to_btc(&self) -> f64 { self.to_btc() }
    fn from_sats(sats: u64) -> Self { Self::from_sats(sats) }
    fn from_btc(btc: f64) -> Self { Self::from_sats((btc * 100_000_000.0) as u64) }
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

/// Represents UTXO information with metadata for tracking
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct UtxoInfo {
    pub txid: String,
    pub vout: u32,
    pub amount_sat: u64,
    pub address: String,
    pub keychain: String,
    pub timestamp: u64,
    pub confirmations: u32,
    pub participants: Vec<String>,
    pub stable_value_usd: f64,
    pub spendable: bool,
    pub derivation_path: String,
    pub spent: bool,
}

/// For webhook retry functionality
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookRetry {
    pub user_id: String,
    pub utxos: Vec<UtxoInfo>,
    pub attempts: u32,
    pub next_attempt: u64,
}

/// Core data structure for tracking stabilized Bitcoin
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StableChain {
    pub user_id: u32,
    pub is_stable_receiver: bool,
    pub counterparty: String,
    pub accumulated_btc: Bitcoin,
    pub stabilized_usd: USD,
    pub timestamp: i64,
    pub formatted_datetime: String,
    pub sc_dir: String,
    pub raw_btc_usd: f64,
    pub prices: HashMap<String, f64>,
    pub multisig_addr: String,
    pub utxos: Vec<Utxo>,
    pub pending_sweep_txid: Option<String>,
    pub events: Vec<Event>,
    pub total_withdrawn_usd: f64,
    pub expected_usd: USD,
    pub hedge_position_id: Option<String>,
    pub pending_channel_id: Option<String>,
    pub shorts: Vec<(f64, f64, String, String)>,
    pub hedge_ready: bool,
    pub last_hedge_time: u64,
    pub short_reduction_amount: Option<f64>,
    pub old_addresses: Vec<String>,
    pub history: Vec<UtxoInfo>,
}

impl StableChain {
    pub fn load_or_create(user_id: &str, address: &str, sc_dir: &str) -> Result<Self, crate::error::PulserError> {
        let sc_path = format!("{}/stable_chain_{}.json", sc_dir, user_id);
        if std::path::Path::new(&sc_path).exists() {
            return Ok(serde_json::from_str(&std::fs::read_to_string(&sc_path)?)?);
        }
        let now = crate::utils::now_timestamp();
        Ok(StableChain {
            user_id: user_id.parse()?,
            is_stable_receiver: false,
            counterparty: String::new(),
            accumulated_btc: Bitcoin::from_sats(0),
            stabilized_usd: USD(0.0),
            timestamp: now,
            formatted_datetime: chrono::Utc::now().to_string(),
            sc_dir: sc_dir.to_string(),
            raw_btc_usd: 0.0,
            prices: HashMap::new(),
            multisig_addr: address.to_string(),
            utxos: Vec::new(),
            pending_sweep_txid: None,
            events: Vec::new(),
            total_withdrawn_usd: 0.0,
            expected_usd: USD(0.0),
            hedge_position_id: None,
            pending_channel_id: None,
            shorts: Vec::new(),
            hedge_ready: false,
            last_hedge_time: 0,
            short_reduction_amount: None,
            old_addresses: Vec::new(),
            history: Vec::new(),
        })
    }

    pub fn is_ready_for_channel(&self, min_confirmations: u32, channel_threshold_usd: f64) -> bool {
        self.utxos.iter().all(|utxo| utxo.confirmations >= min_confirmations) &&
        self.stabilized_usd.0 >= channel_threshold_usd
    }

    pub fn log_event(&mut self, source: &str, kind: &str, details: &str) {
        self.events.push(Event {
            timestamp: crate::utils::now_timestamp(),
            source: source.to_string(),
            kind: kind.to_string(),
            details: details.to_string(),
        });
    }
}

// In common/src/types.rs
impl UtxoInfo {
    // Add a conversion method
    pub fn from_utxo(utxo: &Utxo, address: &str, keychain: &str) -> Self {
        Self {
            txid: utxo.txid.clone(),
            vout: utxo.vout,
            amount_sat: utxo.amount,
            address: address.to_string(),
            keychain: keychain.to_string(),
            timestamp: chrono::Utc::now().timestamp() as u64,
            confirmations: utxo.confirmations,
            participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
            stable_value_usd: utxo.usd_value.as_ref().unwrap_or(&USD(0.0)).0,
            spendable: utxo.confirmations >= 1,
            derivation_path: "".to_string(), // Set default or pass as parameter
            spent: false,
        }
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
    pub user_pubkey: String,
    pub lsp_pubkey: String,
    pub trustee_pubkey: String,
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
    pub internal_descriptor: Option<String>,
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

/// User status information - for tracking API and service state
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserStatus {
    pub user_id: String,
    pub last_sync: u64,
    pub sync_status: String,
    pub utxo_count: u32,
    pub total_value_btc: f64,
    pub total_value_usd: f64,
    pub confirmations_pending: bool,
    pub last_update_message: String,
    pub sync_duration_ms: u64,
    pub last_error: Option<String>,
    pub last_success: u64,
    pub pruned_utxo_count: u32,
    pub current_deposit_address: String,
    pub last_deposit_time: Option<u64>,
}

impl UserStatus {
    pub fn new(user_id: &str) -> Self {
        Self {
            user_id: user_id.to_string(),
            last_sync: 0,
            sync_status: "initialized".to_string(),
            utxo_count: 0,
            total_value_btc: 0.0,
            total_value_usd: 0.0,
            confirmations_pending: false,
            last_update_message: "User initialized".to_string(),
            sync_duration_ms: 0,
            last_error: None,
            last_success: 0,
            pruned_utxo_count: 0,
            current_deposit_address: "Not yet determined".to_string(),
            last_deposit_time: None,
        }
    }
}

/// Service status information - for monitoring and health checks
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceStatus {
    pub up_since: u64,
    pub last_update: u64,
    pub version: String,
    pub users_monitored: u32,
    pub total_utxos: u32,
    pub total_value_btc: f64,
    pub total_value_usd: f64,
    pub health: String,
    pub api_status: HashMap<String, bool>,
    pub last_price: f64,
    pub price_update_count: u32,
    pub active_syncs: u32,
    pub price_cache_staleness_secs: u64,
    pub silent_failures: u32,
    pub api_calls: u32,
    pub error_rate: f64,
    pub users: HashMap<String, UserStatus>, // Add this

}

/// HedgePosition for tracking futures positions
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HedgePosition {
    pub entry_price: f64,
    pub amount_btc: f64,
    pub order_id: String,
    pub user_id: String,
    pub timestamp: u64,
}


/// Notification from deposit-service to hedging-service for hedge actions
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HedgeNotification {
    pub user_id: String,          // Matches StableChain.user_id
    pub action: String,           // "hedge", "adjust", etc.
    pub btc_amount: f64,          // StableChain.accumulated_btc.to_btc()
    pub usd_amount: f64,          // StableChain.stabilized_usd.0
    pub current_price: f64,       // StableChain.raw_btc_usd
    pub timestamp: i64,           // StableChain.timestamp
    pub transaction_id: Option<String>, // StableChain.pending_sweep_txid
}
