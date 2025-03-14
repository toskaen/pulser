// deposit-service/src/types.rs
use common::types::{Amount, USD, Event};
use std::collections::HashMap;
use std::fmt;

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
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Utxo {
    pub txid: String,
    pub vout: u32,
    pub amount: u64,
    pub confirmations: u32,
    pub script_pubkey: String,
    pub height: Option<u32>,
    pub address: String,
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
    pub synthetic_price: Option<f64>,
    pub prices: HashMap<String, f64>,
    pub multisig_addr: String, // Store as string to avoid version conflicts
    pub utxos: Vec<Utxo>,
    pub pending_sweep_txid: Option<String>,
    pub events: Vec<Event>,
    pub total_withdrawn_usd: f64,
    pub expected_usd: USD,
    pub hedge_position_id: Option<String>,
    pub pending_channel_id: Option<String>,
}

impl StableChain {
    // Helper to check if a chain is ready for channel opening
    pub fn is_ready_for_channel(&self, min_confirmations: u32, channel_threshold_usd: f64) -> bool {
        // Check if all UTXOs have enough confirmations
        let all_confirmed = self.utxos.iter().all(|u| u.confirmations >= min_confirmations);
        
        // Check if we have enough confirmed funds
        let total_confirmed_sats = self.utxos.iter()
            .filter(|u| u.confirmations >= min_confirmations)
            .map(|u| u.amount)
            .sum::<u64>();
            
        // Convert to USD
        let total_confirmed_usd = (total_confirmed_sats as f64 / 100_000_000.0) * self.raw_btc_usd;
        
        all_confirmed && total_confirmed_usd >= channel_threshold_usd && self.pending_sweep_txid.is_none()
    }
    
    // Helper to log events
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
    pub synthetic_price: f64,
}
