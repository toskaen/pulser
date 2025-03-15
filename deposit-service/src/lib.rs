// In deposit-service/src/lib.rs
pub mod types;
pub mod config;
pub mod blockchain;
pub mod wallet;
pub mod keys;
pub mod monitoring;  // Add this line

// Re-export common types
pub use config::Config;
pub use wallet::DepositWallet;
