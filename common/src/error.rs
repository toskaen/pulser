use thiserror::Error;
use actix_web::ResponseError;
use actix_web::http::StatusCode;

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

impl ResponseError for PulserError {
    fn status_code(&self) -> StatusCode {
        match self {
            PulserError::ConfigError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PulserError::NetworkError(_) => StatusCode::SERVICE_UNAVAILABLE,
            PulserError::ApiError(_) => StatusCode::BAD_GATEWAY,
            PulserError::UserNotFound(_) => StatusCode::NOT_FOUND,
            PulserError::TransactionError(_) => StatusCode::BAD_REQUEST,
            PulserError::WalletError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PulserError::AuthError(_) => StatusCode::UNAUTHORIZED,
            PulserError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PulserError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            PulserError::InsufficientFunds(_) => StatusCode::PAYMENT_REQUIRED,
            PulserError::PriceFeedError(_) => StatusCode::SERVICE_UNAVAILABLE,
            PulserError::StorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PulserError::PsbtError(_) => StatusCode::BAD_REQUEST,
            PulserError::SigningError(_) => StatusCode::BAD_REQUEST,
            PulserError::BroadcastError(_) => StatusCode::BAD_GATEWAY,
            PulserError::ChannelError(_) => StatusCode::BAD_REQUEST,
            PulserError::TaprootError(_) => StatusCode::BAD_REQUEST,
        }
    }
}
