// common/src/lib.rs
pub mod error;
pub mod price_feed;
pub mod utils;
pub mod types;
pub mod storage;
pub mod wallet_utils;
pub mod task_manager;

pub use error::PulserError;
pub use types::{Amount, Bitcoin, USD, StableChain, UserStatus, ServiceStatus, 
                UtxoInfo, WebhookRetry, Event, PriceInfo, Utxo, HedgePosition};
pub use storage::StateManager;
pub use task_manager::UserTaskLock; // Update to match your struct
