// common/src/types.rs
use serde::{Deserialize, Serialize};
use std::fmt;
use std::collections::HashMap;
use std::str::FromStr;
use crate::PulserError;
use crate::StateManager;
use tokio::sync::{RwLock, Mutex, MutexGuard};
use std::sync::Arc;


pub trait Amount {
    fn to_sats(&self) -> u64;
    fn to_btc(&self) -> f64;
    fn from_sats(sats: u64) -> Self;
    fn from_btc(btc: f64) -> Self;
}

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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct USD(pub f64);

impl USD {
    pub fn from_f64(value: f64) -> Self { Self(value) }
    pub fn from_amount<T: Amount>(amount: &T, price: f64) -> Self { 
        Self(amount.to_btc() * price) 
    }
}

impl fmt::Display for USD {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "${:.2}", self.0)
    }
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
    pub spent: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookRetry {
    pub user_id: String,
    pub utxos: Vec<UtxoInfo>,
    pub attempts: u32,
    pub next_attempt: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StableChain {
    pub user_id: u32,
    pub is_stable_receiver: bool,
    pub counterparty: String,
    pub accumulated_btc: Bitcoin, // Option 2: Keep and sync with get_balance()
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
        pub change_log: Vec<ChangeEntry>,

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
            change_log: Vec::new(), // Initialize empty change log
        })
    }
    pub async fn update_with_transaction<F>(&mut self, 
                                           state_manager: &StateManager, 
                                           user_id: &str,
                                           update_fn: F) -> Result<(), PulserError>
    where
        F: FnOnce(&mut StableChain) -> Result<(), PulserError>, // Replace Self with StableChain

    {
        // Apply the update function to this chain
        update_fn(self)?;

        
        // Save the updated chain
        state_manager.save_stable_chain(user_id, self).await
 
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
    
    pub fn log_change(&mut self, change_type: &str, btc_delta: f64, usd_delta: f64, service: &str, details: Option<String>) {
        let entry = ChangeEntry {
            timestamp: chrono::Utc::now().timestamp(),
            change_type: change_type.to_string(),
            btc_delta,
            usd_delta,
            service: service.to_string(),
            details,
        };
        
        self.change_log.push(entry);
        
        // Keep log size reasonable - keep last 100 entries
        if self.change_log.len() > 100 {
            self.change_log = self.change_log.split_off(self.change_log.len() - 100);
        }
    }
}


impl UtxoInfo {
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
            derivation_path: "".to_string(),
            spent: utxo.spent, // Updated to use Utxo.spent
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub timestamp: i64,
    pub source: String,
    pub kind: String,
    pub details: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceInfo {
    pub raw_btc_usd: f64,
    pub timestamp: i64,
    pub price_feeds: HashMap<String, f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DepositAddressInfo {
    pub address: String,
    pub user_id: u32,
    pub multisig_type: String,
    pub participants: Vec<String>,
    pub descriptor: Option<String>,
    pub path: String,
    pub user_pubkey: String,
    pub lsp_pubkey: String,
    pub trustee_pubkey: String,
}

#[derive(Clone, Debug)]
pub struct WalletManager {
    wallets: RwLock<HashMap<String, Arc<Mutex<(wallet::DepositWalletType, StableChain)>>>>,
}

impl WalletManager {
    pub fn new() -> Self {
        Self { wallets: RwLock::new(HashMap::new()) }
    }

    // Get a wallet with exclusive access
    pub async fn get_wallet_mut(&self, user_id: &str) -> Result<tokio::sync::MutexGuard<(wallet::DepositWalletType, StableChain)>, PulserError> {
        let wallets = self.wallets.read().await;
        match wallets.get(user_id) {
            Some(wallet_mutex) => Ok(wallet_mutex.lock().await),
            None => Err(PulserError::UserNotFound(format!("User {} not found", user_id)))
        }
    }

    // Add/update a wallet
    pub async fn store_wallet(&self, user_id: &str, wallet: wallet::DepositWalletType, chain: StableChain) -> Result<(), PulserError> {
        let mut wallets = self.wallets.write().await;
        if let Some(wallet_mutex) = wallets.get(user_id) {
            // Update existing wallet
            let mut guard = wallet_mutex.lock().await;
            *guard = (wallet, chain);
        } else {
            // Create new wallet entry
            wallets.insert(user_id.to_string(), Arc::new(Mutex::new((wallet, chain))));
        }
        Ok(())
    }
}

// Add a simple DepositWalletType definition to avoid circular dependencies
pub mod wallet {
    pub struct DepositWalletType {
        // Add minimal required fields
        pub wallet_id: String,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelInfo {
    pub user_id: u32,
    pub channel_id: String,
    pub is_stable_receiver: bool,
    pub counterparty: String,
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
    pub usd_value: Option<USD>,
    pub spent: bool, // NEW: Aligns with BDK LocalOutput
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
    pub last_price: f64,
    pub price_update_count: u32,
    pub active_syncs: u32,
    pub websocket_active: bool,
    
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeEntry {
    pub timestamp: i64,
    pub change_type: String, // "deposit", "withdrawal", "stabilization", "hedge", etc.
    pub btc_delta: f64,      // Change in BTC amount (positive or negative)
    pub usd_delta: f64,      // Change in USD amount (positive or negative)
    pub service: String,     // Which service made the change ("deposit", "hedge", "withdraw")
    pub details: Option<String>, // Optional additional details (txid, etc.)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HedgingStatus {
    pub total_btc_value: f64,
    pub total_usd_stabilized: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HedgePosition {
    pub entry_price: f64,
    pub amount_btc: f64,
    pub order_id: String,
    pub user_id: String,
    pub timestamp: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HedgeNotification {
    pub user_id: String,
    pub action: String,
    pub btc_amount: f64,
    pub usd_amount: f64,
    pub current_price: f64,
    pub timestamp: i64,
    pub transaction_id: Option<String>,
}
