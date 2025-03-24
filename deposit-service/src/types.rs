// deposit-service/src/types.rs
use common::types::{Amount, USD, Event};
use std::collections::HashMap;
use std::fmt;
use std::fs; // Add this




/// Represents a Bitcoin amount in satoshis.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Bitcoin {
    pub sats: u64,
}

impl Bitcoin {
    pub fn from_sats(sats: u64) -> Self { Self { sats } }
    
    pub fn to_btc(&self) -> f64 { 
        self.sats as f64 / 100_000_000.0 
    }
}

impl fmt::Display for Bitcoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:10.8}btc", self.to_btc())
    }
}

// Implement common Amount trait for our Bitcoin type
impl Amount for Bitcoin {
    fn to_sats(&self) -> u64 { self.sats }
    fn to_btc(&self) -> f64 { self.to_btc() }
    fn from_sats(sats: u64) -> Self { Self::from_sats(sats) }
    fn from_btc(btc: f64) -> Self { Self::from_sats((btc * 100_000_000.0) as u64) }
}

/// UTXO information
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Utxo {
    pub txid: String,
    pub vout: u32,
    pub amount: u64,
    pub confirmations: u32,
    pub script_pubkey: String,
    pub height: Option<u32>,
    pub usd_value: Option<USD>, // Add this

}

/// Represents a multisig deposit pool for a USER.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StableChain {
    pub user_id: u32,
    pub is_stable_receiver: bool,
    pub counterparty: String, // Store as string to avoid version conflicts
    pub accumulated_btc: Bitcoin,
    pub stabilized_usd: USD,
    pub timestamp: i64,
    pub formatted_datetime: String,
    pub sc_dir: String,
    pub raw_btc_usd: f64,
    pub prices: HashMap<String, f64>,
    pub multisig_addr: String, // Store as string to avoid version conflicts
    pub utxos: Vec<Utxo>,
    pub pending_sweep_txid: Option<String>,
    pub events: Vec<Event>,
    pub total_withdrawn_usd: f64,
    pub expected_usd: USD,
    pub hedge_position_id: Option<String>,
    pub pending_channel_id: Option<String>,
     pub shorts: Vec<(f64, f64, String, String)>, // (entry_price, position_btc, order_id, user_id)
    pub hedge_ready: bool,
    pub last_hedge_time: u64, // New: Tracks last hedge time
        pub short_reduction_amount: Option<f64>, // New: Signals hedge-service

}

impl StableChain {
    pub fn load_or_create(user_id: &str, address: &str, sc_dir: &str) -> Result<Self, common::error::PulserError> {
        let sc_path = format!("{}/stable_chain_{}.json", sc_dir, user_id);
        if std::path::Path::new(&sc_path).exists() {
            return Ok(serde_json::from_str(&fs::read_to_string(&sc_path)?)?);
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

/// Deposit address information
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DepositAddressInfo {
    pub address: String,
    pub descriptor: String,
    pub path: String,
    pub user_pubkey: String,
    pub lsp_pubkey: String,
    pub trustee_pubkey: String,
}

/// Create deposit request
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CreateDepositRequest {
    pub user_id: u32,
    pub lsp_pubkey: Option<String>,
    pub trustee_pubkey: Option<String>,
    pub user_pubkey: Option<String>,
    pub expected_amount_usd: Option<f64>,
    pub metadata: Option<HashMap<String, String>>,
}

/// Withdrawal request
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WithdrawalRequest {
    pub user_id: u32,
    pub amount_usd: f64,
    pub destination_type: String,
    pub destination_address: Option<String>,
    pub urgent: Option<bool>,
    pub max_fee_sats: Option<u64>,
}

/// Response for withdrawal
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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

/// Notification to hedge service
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HedgeNotification {
    pub user_id: u32,
    pub action: String,
    pub btc_amount: f64,
    pub usd_amount: f64,
    pub current_price: f64,
    pub timestamp: i64,
    pub transaction_id: Option<String>,
}

/// PSBT signing request
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PsbtSignRequest {
    pub user_id: u32,
    pub psbt: String,
    pub purpose: String,
    pub manual_review: Option<bool>,
}

/// Request struct for PSBT status check
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PsbtStatusRequest {
    pub user_id: u32,
    pub txid: String,
    pub purpose: String,
    pub amount_usd: Option<f64>,
}

/// Channel opening request to channel service
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChannelOpenRequest {
    pub user_id: u32,
    pub lsp_pubkey: String,
    pub amount_sats: u64,
    pub expected_usd: f64,
    pub current_price: f64,
}
