// common/src/error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PulserError {
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    #[error("Network error: {0}")]
    NetworkError(String),
    
    #[error("API error: {0}")]
    ApiError(String),
    
    #[error("User not found: {0}")]
    UserNotFound(String),
    
    #[error("Transaction error: {0}")]
    TransactionError(String),
    
    #[error("Wallet error: {0}")]
    WalletError(String),
    
    #[error("Authentication error: {0}")]
    AuthError(String),
    
    #[error("Internal error: {0}")]
    InternalError(String),
    
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    
    #[error("Insufficient funds: {0}")]
    InsufficientFunds(String),
    
    #[error("Price feed error: {0}")]
    PriceFeedError(String),
    
    #[error("Storage error: {0}")]
    StorageError(String),
    
    #[error("PSBT error: {0}")]
    PsbtError(String),
    
    #[error("Signing error: {0}")]
    SigningError(String),
    
    #[error("Broadcast error: {0}")]
    BroadcastError(String),
    
    #[error("Channel error: {0}")]
    ChannelError(String),
    
    #[error("Taproot error: {0}")]
    TaprootError(String),
}
