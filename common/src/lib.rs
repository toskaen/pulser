// common/src/lib.rs
pub mod error;
pub mod price_feed;
pub mod utils;
pub mod types;
pub mod storage;

// Re-export common types and utilities
pub use error::PulserError;
pub use types::{Amount, Bitcoin, USD, StableChain, UserStatus, ServiceStatus, 
                UtxoInfo, WebhookRetry, Event, PriceInfo, Utxo, HedgePosition};
pub use storage::{StateManager, UtxoInfo as StorageUtxoInfo};
