// common/src/types.rs
use serde::{Deserialize, Serialize};
use std::fmt;
use std::collections::{HashMap, HashSet};
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum UtxoOrigin {
    ExternalDeposit,           // Normal deposit from user
    WithdrawalChange,          // Change from a withdrawal transaction
    UnexpectedChangeDeposit,   // Will be completely ignored
    ServiceDeposit,            // From service operator
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UtxoInfo {
    // Existing fields
    pub txid: String,
    pub vout: u32,
    pub amount_sat: u64,
    pub address: String,
    pub keychain: String,
    
    // New blockchain tracking fields
    pub first_seen_timestamp: u64,
    pub confirmation_timestamp: Option<u64>,
    pub block_height: Option<u32>,
    
    // Existing fields
    pub confirmations: u32,
    pub stable_value_usd: f64,
    pub spent: bool,
    
     pub stabilization_price: Option<f64>,     // BTC/USD price used for stabilization
    pub stabilization_source: Option<String>, // Source of the price (e.g., "Spot", "Deribit", etc.)
    pub stabilization_time: Option<u64>,      // When stabilization occurred
    
    // New classification fields
    pub origin: UtxoOrigin,
    pub parent_txid: Option<String>,
    pub never_spend: bool,
    
    // Legacy fields with defaults for compatibility
    pub participants: Vec<String>,
    pub spendable: bool,
    pub derivation_path: String,
}

impl UtxoInfo {
    pub fn from_utxo(utxo: &Utxo, address: &str, keychain: &str) -> Self {
        let now = chrono::Utc::now().timestamp() as u64;
        
        Self {
            txid: utxo.txid.clone(),
            vout: utxo.vout,
            amount_sat: utxo.amount,
            address: address.to_string(),
            keychain: keychain.to_string(),
            first_seen_timestamp: now,
            confirmation_timestamp: if utxo.confirmations > 0 { Some(now) } else { None },
            block_height: utxo.height,
            confirmations: utxo.confirmations,
            participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
            stable_value_usd: utxo.usd_value.as_ref().unwrap_or(&USD(0.0)).0,
            spendable: utxo.confirmations >= 1,
            derivation_path: "".to_string(),
            spent: utxo.spent,
            origin: UtxoOrigin::ExternalDeposit, // Default origin
            parent_txid: None,
            never_spend: false,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    WithdrawalChange,
    StabilizationAdjustment,
    ReorgAdjustment,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionRecord {
    pub timestamp: u64,
    pub transaction_type: TransactionType,
    pub txid: String,
    pub amount_btc: f64,
    pub value_usd: f64,
    pub confirmations: u32,
    pub block_height: Option<u32>,
    pub current_btc_price: f64,
    pub details: Option<String>,
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
    pub change_log: Vec<ChangeEntry>,
    
    // New fields that were missing
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub regular_utxos: Vec<Utxo>,
    
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub change_utxos: Vec<Utxo>,
    
    // Track change addresses
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub change_address_mappings: HashMap<String, String>,
    
    // Add accounting by origin
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub stable_value_by_origin: HashMap<String, USD>,
    
    // Add detailed transaction history
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transaction_history: Vec<TransactionRecord>,
    
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub trusted_service_addresses: HashSet<String>,
}

// 2. Update the load_or_create method to initialize all fields correctly
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
            change_log: Vec::new(),
            // Initialize new fields
            regular_utxos: Vec::new(),
            change_utxos: Vec::new(),
            change_address_mappings: HashMap::new(),
            stable_value_by_origin: HashMap::new(),
            transaction_history: Vec::new(),
            trusted_service_addresses: HashSet::new(),
        })
    }

    pub async fn update_with_transaction<F>(&mut self, 
                                           state_manager: &StateManager, 
                                           user_id: &str,
                                           update_fn: F) -> Result<(), PulserError>
    where
        F: FnOnce(&mut StableChain) -> Result<(), PulserError>, 

    {
        // Apply the update function to this chain
        update_fn(self)?;

        
        // Save the updated chain
        state_manager.save_stable_chain(user_id, self).await
 
   }
   
    pub fn usable_stable_value(&self) -> USD {
        // Implementation that uses stable_value_by_origin or falls back
        if !self.stable_value_by_origin.is_empty() {
            let total: f64 = self.stable_value_by_origin
                .values()
                .map(|usd| usd.0)
                .sum();
                
            USD(total)
        } else {
            // Fall back to legacy method
            self.stabilized_usd.clone()
        }
    }
    
    // Get spendable UTXOs (excluding unexpected deposits)
    pub fn get_spendable_utxos(&self) -> Vec<&Utxo> {
        self.utxos.iter()
            .filter(|u| {
                u.confirmations >= 1 && 
                u.usd_value.is_some() && 
                !u.spent
            })
            .collect()
    }
    
    // Enhanced change log method
    pub fn log_utxo_event(&mut self, event_type: &str, utxo: &UtxoInfo, details: Option<String>) {
        let event = Event {
            timestamp: crate::utils::now_timestamp(),
            source: "deposit-service".to_string(),
            kind: event_type.to_string(),
            details: details.unwrap_or_else(|| format!("UTXO {}:{}", utxo.txid, utxo.vout)),
        };
        self.events.push(event);
    }
    
    // Generate detailed transaction history
    pub fn generate_transaction_history(&self) -> Vec<TransactionRecord> {
        let mut records = Vec::new();
        
        // Process history entries
        for utxo in &self.history {
            if utxo.stable_value_usd > 0.0 && !utxo.spent {
                let record = TransactionRecord {
timestamp: utxo.first_seen_timestamp,
                    transaction_type: TransactionType::Deposit,
                    txid: utxo.txid.clone(),
                    amount_btc: utxo.amount_sat as f64 / 100_000_000.0,
                    value_usd: utxo.stable_value_usd,
                    confirmations: utxo.confirmations,
                    block_height: utxo.block_height,
                    current_btc_price: self.raw_btc_usd,
                    details: Some(format!("Deposit to {}", utxo.address)),
                };
                records.push(record);
            }
        }
        
        // Combine with existing transaction history
        records.extend(self.transaction_history.clone());
        
        // Sort by timestamp (newest first)
        records.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        
        records
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

// Add definition for DepositWallet directly in common
pub mod wallet {
    use serde::{Serialize, Deserialize};
    use bitcoin::Network;

    #[derive(Debug)]
    pub struct DepositWallet {
        pub wallet_id: String,
        pub user_id: String,
        pub network: Network,
        pub descriptor: String,
    }

    impl DepositWallet {
        pub fn new(wallet_id: &str, user_id: &str, network: Network, descriptor: &str) -> Self {
            Self {
                wallet_id: wallet_id.to_string(),
                user_id: user_id.to_string(),
                network,
                descriptor: descriptor.to_string(),
            }
        }
    }

    // Define DepositWalletType for backward compatibility
    #[derive(Debug)]
    pub struct DepositWalletType {
        pub wallet_id: String,
    }
}

// Update the WalletManager implementation to use our local DepositWallet type
#[derive(Debug)]
pub struct WalletManager {
    // Using self::wallet namespace instead of deposit_service
    wallets: RwLock<HashMap<String, Arc<Mutex<(self::wallet::DepositWalletType, StableChain)>>>>,
}

// Keep the manual Clone implementation
impl Clone for WalletManager {
    fn clone(&self) -> Self {
        Self {
            wallets: RwLock::new(HashMap::new()), // Create new empty RwLock instead of cloning
        }
    }
}

impl WalletManager {
    pub fn new() -> Self {
        Self { wallets: RwLock::new(HashMap::new()) }
    }

    // Update the function to use our local DepositWallet type  
    pub async fn get_wallet(wallets: Arc<HashMap<String, Arc<Mutex<self::wallet::DepositWallet>>>>, user_id: &str) -> Result<Arc<Mutex<self::wallet::DepositWallet>>, PulserError> {
        match wallets.get(user_id) {
            Some(wallet_mutex) => Ok(wallet_mutex.clone()), // Clone the Arc, not the DepositWallet
            None => Err(PulserError::NotFound("Wallet not found".to_string())),
        }
    }

    // Store wallet method remains the same with local wallet type
    pub async fn store_wallet(&self, user_id: &str, wallet: self::wallet::DepositWalletType, chain: StableChain) -> Result<(), PulserError> {
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
     pub stabilization_price: Option<f64>,     // BTC/USD price used for stabilization
    pub stabilization_source: Option<String>, // Source of the price (e.g., "Spot", "Deribit", etc.)
    pub stabilization_time: Option<u64>,      // When stabilization occurred
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
    #[serde(default)]
    pub utxo_stats: Option<UtxoStatistics>,
    
    #[serde(default)]
    pub unusual_activity_detected: bool,
    
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unusual_activity_details: Vec<String>,
        pub resource_usage: Option<ResourceUsage>,
    pub performance: Option<PerformanceMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub cpu_percent: f64,
    pub memory_mb: f64,
    pub disk_operations_per_min: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub avg_sync_time_ms: u32,
    pub avg_price_fetch_time_ms: u32,
    pub max_sync_time_ms: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UtxoStatistics {
    pub regular_deposits: u32,
    pub withdrawal_change: u32,
    pub ignored_deposits: u32,
    pub total_confirmed_value_btc: f64,
    pub total_stable_value_usd: f64,
        pub price_data: PriceStatistics,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PriceStatistics {
    pub min_stabilization_price: f64,
    pub max_stabilization_price: f64,
    pub avg_stabilization_price: f64,
    pub price_sources: HashMap<String, u32>,  // Count of UTXOs per price source
    pub total_stabilized_utxos: u32,
}

impl ServiceStatus {
pub fn update_with_utxo_stats(&mut self, stats: UtxoStatistics, unusual_activity: Vec<String>) {
    self.utxo_stats = Some(stats.clone());  // Clone stats here
    self.unusual_activity_detected = !unusual_activity.is_empty();
    self.unusual_activity_details = unusual_activity;
    self.total_utxos = stats.regular_deposits + stats.withdrawal_change;
    self.total_value_btc = stats.total_confirmed_value_btc;
    self.total_value_usd = stats.total_stable_value_usd;
}
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
