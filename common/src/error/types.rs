// common/src/error/types.rs
use std::fmt;
use super::context::ErrorContext;
use actix_web::{ResponseError, http::StatusCode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    Configuration,
    Network,
    Database,
    Authentication,
    Validation,
    NotFound,
    Bitcoin,
    PriceFeed,
    Storage,
    External,
    Internal,
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCategory::Configuration => write!(f, "Configuration"),
            ErrorCategory::Network => write!(f, "Network"),
            ErrorCategory::Database => write!(f, "Database"),
            ErrorCategory::Authentication => write!(f, "Authentication"),
            ErrorCategory::Validation => write!(f, "Validation"),
            ErrorCategory::NotFound => write!(f, "NotFound"),
            ErrorCategory::Bitcoin => write!(f, "Bitcoin"),
            ErrorCategory::PriceFeed => write!(f, "PriceFeed"),
            ErrorCategory::Storage => write!(f, "Storage"),
            ErrorCategory::External => write!(f, "External"),
            ErrorCategory::Internal => write!(f, "Internal"),
        }
    }
}

#[derive(Debug)]
pub struct ContextualError {
    pub error: PulserError,
    pub context: ErrorContext,
}

impl fmt::Display for ContextualError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} [{}]", self.error, self.context)
    }
}

#[derive(thiserror::Error, Debug)]
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
    #[error("Invalid input error: {0}")]
    InvalidInput(String),
    #[error("Bitcoin error: {0}")]
    BitcoinError(String),
    #[error("Consensus encoding error: {0}")]
    ConsensusError(String),
    #[error("Webhook error: {0}")]
    WebhookError(String),
    #[error("{0}")]
    WithContext(Box<ContextualError>),  
    #[error("Sync error: {0}")]
    SyncError(String),
    #[error("NotFound error: {0}")]
    NotFound(String),
}

impl PulserError {
    pub fn add_context(self, context: ErrorContext) -> Self {
        match self {
            // Avoid nesting contexts
            Self::WithContext(boxed_error) => {
                let mut error = *boxed_error;
                error.context.merge(context);
                Self::WithContext(Box::new(error))
            }
            _ => Self::WithContext(Box::new(ContextualError {
                error: self,
                context,
            }))
        }
    }

    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::WithContext(boxed_error) => boxed_error.error.category(),
            Self::ConfigError(_) => ErrorCategory::Configuration,
            Self::NetworkError(_) => ErrorCategory::Network,
            Self::ApiError(_) => ErrorCategory::External,
            Self::UserNotFound(_) => ErrorCategory::NotFound,
            Self::TransactionError(_) => ErrorCategory::Bitcoin,
            Self::WalletError(_) => ErrorCategory::Bitcoin,
            Self::AuthError(_) => ErrorCategory::Authentication,
            Self::InternalError(_) => ErrorCategory::Internal,
            Self::InvalidRequest(_) => ErrorCategory::Validation,
            Self::InsufficientFunds(_) => ErrorCategory::Bitcoin,
            Self::PriceFeedError(_) => ErrorCategory::PriceFeed,
            Self::StorageError(_) => ErrorCategory::Storage,
            Self::PsbtError(_) => ErrorCategory::Bitcoin,
            Self::SigningError(_) => ErrorCategory::Bitcoin,
            Self::BroadcastError(_) => ErrorCategory::Network,
            Self::ChannelError(_) => ErrorCategory::Network,
            Self::TaprootError(_) => ErrorCategory::Bitcoin,
            Self::InvalidInput(_) => ErrorCategory::Validation,
            Self::BitcoinError(_) => ErrorCategory::Bitcoin,
            Self::ConsensusError(_) => ErrorCategory::Bitcoin,
            Self::WebhookError(_) => ErrorCategory::External,
            Self::SyncError(_) => ErrorCategory::Bitcoin, // Or Internal, depending on intent
        Self::NotFound(_) => ErrorCategory::NotFound,
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self.category() {
            ErrorCategory::Network |
            ErrorCategory::External |
            ErrorCategory::PriceFeed => true,
            _ => false
        }
    }
}

impl ResponseError for PulserError {
    fn status_code(&self) -> StatusCode {
        match self {
            PulserError::WithContext(boxed_error) => boxed_error.error.status_code(),
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
            PulserError::TaprootError(_) => StatusCode::BAD_REQUEST,
            PulserError::InvalidInput(_) => StatusCode::BAD_REQUEST,
            PulserError::BitcoinError(_) => StatusCode::BAD_REQUEST,
            PulserError::ConsensusError(_) => StatusCode::INTERNAL_SERVER_ERROR, 
            PulserError::WebhookError(_) => StatusCode::BAD_GATEWAY,
                PulserError::ChannelError(_) => StatusCode::SERVICE_UNAVAILABLE,
                            PulserError::SyncError(_) => StatusCode::SERVICE_UNAVAILABLE,
            PulserError::NotFound(_) => StatusCode::NOT_FOUND,


        }
    }
}
