// deposit-service/src/types.rs
use common::types::{Amount, USD, Event, Utxo};
use std::collections::HashMap;
use std::fmt;
use serde::{Serialize, Deserialize};

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
    pub spent: bool, // Added for sync_user compatibility
}

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
}

impl StableChain {
    pub fn load_or_create(user_id: &str, address: &str, sc_dir: &str) -> Result<Self, common::error::PulserError> {
        let sc_path = format!("{}/stable_chain_{}.json", sc_dir, user_id);
        if std::path::Path::new(&sc_path).exists() {
            return Ok(serde_json::from_str(&std::fs::read_to_string(&sc_path)?)?);
        }
        let now = common::utils::now_timestamp();
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
        })
    }

    pub fn is_ready_for_channel(&self, min_confirmations: u32, channel_threshold_usd: f64) -> bool {
        self.utxos.iter().all(|utxo| utxo.confirmations >= min_confirmations) &&
        self.stabilized_usd.0 >= channel_threshold_usd
    }

    pub fn log_event(&mut self, source: &str, kind: &str, details: &str) {
        self.events.push(Event {
            timestamp: common::utils::now_timestamp(),
            source: source.to_string(),
            kind: kind.to_string(),
            details: details.to_string(),
        });
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DepositAddressInfo {
    pub address: String,
    pub descriptor: String,
    pub path: String,
    pub user_pubkey: String,
    pub lsp_pubkey: String,
    pub trustee_pubkey: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateDepositRequest {
    pub user_id: u32,
    pub lsp_pubkey: Option<String>,
    pub trustee_pubkey: Option<String>,
    pub user_pubkey: Option<String>,
    pub expected_amount_usd: Option<f64>,
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WithdrawalRequest {
    pub user_id: u32,
    pub amount_usd: f64,
    pub destination_type: String,
    pub destination_address: Option<String>,
    pub urgent: Option<bool>,
    pub max_fee_sats: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WithdrawalResponse {
    pub request_id: String,
    pub txid: Option<String>,
    pub status: String,
    pub amount_usd: f64,
    pub amount_btc: f64,
    pub fee_sats: u64,
    pub fee_usd: f64,
    pub estimated_completion_time: chrono::DateTime<chrono::Utc>,
    pub confirmation_url: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HedgeNotification {
    pub user_id: u32,
    pub action: String,
    pub btc_amount: f64,
    pub usd_amount: f64,
    pub current_price: f64,
    pub timestamp: i64,
    pub transaction_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PsbtSignRequest {
    pub user_id: u32,
    pub psbt: String,
    pub purpose: String,
    pub manual_review: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PsbtStatusRequest {
    pub user_id: u32,
    pub txid: String,
    pub purpose: String,
    pub amount_usd: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelOpenRequest {
    pub user_id: u32,
    pub lsp_pubkey: String,
    pub amount_sats: u64,
    pub expected_usd: f64,
    pub current_price: f64,
}

// For deposit_monitor.rs compatibility
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
}

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
