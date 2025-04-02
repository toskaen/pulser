use thiserror::Error;
use actix_web::{ResponseError, http::StatusCode};
use bdk_chain::local_chain::CannotConnectError;
use bdk_esplora::esplora_client::Error as EsploraError;
use bdk_wallet::keys::KeyError as BdkKeyError;
use bdk_wallet::bip39::Error as Bip39Error;
use bdk_wallet::miniscript::descriptor::DescriptorKeyParseError;
use bdk_wallet::descriptor::DescriptorError;
use bdk_file_store::FileError;
use bdk_wallet::ChangeSet;
use bdk_wallet::CreateWithPersistError;
use std::io;
use std::time::SystemTimeError;
use bdk_wallet::error::CreateTxError;
use bitcoin::network::ParseNetworkError;
use bitcoin::address::ParseError; // Keep only if used
use bitcoin::address::FromScriptError; // Keep only if used
use bitcoin::hashes::hex::HexToBytesError; // Keep only if used
use bitcoin::consensus::encode::Error as ConsensusError; // Add to imports
use warp;
use bincode; // Add
use tokio::sync::broadcast::error::{SendError, RecvError};
use std::net::AddrParseError;

impl warp::reject::Reject for PulserError {}

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
    #[error("Invalid input error: {0}")]
    InvalidInput(String),
    #[error("Bitcoin error: {0}")]
    BitcoinError(String),
        #[error("Consensus encoding error: {0}")]
            ConsensusError(#[from] ConsensusError),
            #[error("Webhook error: {0}")]
    WebhookError(String),

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
            PulserError::InvalidInput(_) => StatusCode::BAD_REQUEST,
            PulserError::BitcoinError(_) => StatusCode::BAD_REQUEST,
       PulserError::ConsensusError(_) => StatusCode::INTERNAL_SERVER_ERROR, 
          PulserError::WebhookError(_) => StatusCode::BAD_GATEWAY,

        }
    }
}

// Keep all From impls—remove unused ones only if confirmed
impl From<io::Error> for PulserError {
    fn from(err: io::Error) -> Self { PulserError::StorageError(err.to_string()) }
}

impl From<toml::de::Error> for PulserError {
    fn from(err: toml::de::Error) -> Self { PulserError::ConfigError(err.to_string()) }
}

impl From<EsploraError> for PulserError {
    fn from(err: EsploraError) -> Self { PulserError::ApiError(err.to_string()) }
}

impl From<Box<EsploraError>> for PulserError {
    fn from(err: Box<EsploraError>) -> Self { PulserError::ApiError(err.to_string()) }
}

impl From<CannotConnectError> for PulserError {
    fn from(err: CannotConnectError) -> Self { PulserError::WalletError(err.to_string()) }
}

impl From<bitcoin::consensus::encode::FromHexError> for PulserError {
    fn from(err: bitcoin::consensus::encode::FromHexError) -> Self { PulserError::WalletError(err.to_string()) }
}

impl From<BdkKeyError> for PulserError {
    fn from(err: BdkKeyError) -> Self { PulserError::WalletError(err.to_string()) }
}

impl From<Option<Bip39Error>> for PulserError {
    fn from(err: Option<Bip39Error>) -> Self {
        match err {
            Some(e) => PulserError::WalletError(e.to_string()),
            None => PulserError::WalletError("Unknown BIP39 error".to_string()),
        }
    }
}

impl From<Bip39Error> for PulserError {
    fn from(err: Bip39Error) -> Self { PulserError::WalletError(err.to_string()) }
}

impl From<DescriptorKeyParseError> for PulserError {
    fn from(err: DescriptorKeyParseError) -> Self { PulserError::WalletError(err.to_string()) }
}

impl From<FileError> for PulserError {
    fn from(err: FileError) -> Self { PulserError::StorageError(err.to_string()) }
}

impl From<CreateWithPersistError<ChangeSet>> for PulserError {
    fn from(err: CreateWithPersistError<ChangeSet>) -> Self { PulserError::WalletError(format!("CreateWithPersistError: {:?}", err)) }
}

impl From<DescriptorError> for PulserError {
    fn from(err: DescriptorError) -> Self { PulserError::WalletError(err.to_string()) }
}

impl From<bitcoin::bip32::Error> for PulserError {
    fn from(err: bitcoin::bip32::Error) -> Self { PulserError::WalletError(err.to_string()) }
}

impl From<serde_json::Error> for PulserError {
    fn from(err: serde_json::Error) -> Self { PulserError::StorageError(err.to_string()) }
}

impl From<std::num::ParseIntError> for PulserError {
    fn from(err: std::num::ParseIntError) -> Self { PulserError::InvalidRequest(err.to_string()) }
}

impl From<reqwest::Error> for PulserError {
    fn from(err: reqwest::Error) -> Self { PulserError::ApiError(err.to_string()) }
}

impl From<SystemTimeError> for PulserError {
    fn from(err: SystemTimeError) -> Self { PulserError::NetworkError(err.to_string()) }
}

impl From<CreateTxError> for PulserError {
    fn from(err: CreateTxError) -> Self { PulserError::TransactionError(err.to_string()) }
}

impl From<ParseNetworkError> for PulserError {
    fn from(err: ParseNetworkError) -> Self { PulserError::ConfigError(err.to_string()) }
}

impl From<bitcoin::address::FromScriptError> for PulserError {
    fn from(err: bitcoin::address::FromScriptError) -> Self { PulserError::BitcoinError(err.to_string()) }
}

impl From<bitcoin::hashes::hex::HexToBytesError> for PulserError {
    fn from(err: bitcoin::hashes::hex::HexToBytesError) -> Self { PulserError::BitcoinError(err.to_string()) }
}

impl From<bitcoin::address::ParseError> for PulserError {
    fn from(err: bitcoin::address::ParseError) -> Self { PulserError::BitcoinError(err.to_string()) }
}

impl From<bitcoin::hashes::hex::HexToArrayError> for PulserError {
    fn from(err: bitcoin::hashes::hex::HexToArrayError) -> Self { PulserError::WalletError(err.to_string()) }
}

impl From<tokio_tungstenite::tungstenite::Error> for PulserError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        PulserError::NetworkError(err.to_string())
    }
}

impl From<tokio::sync::mpsc::error::SendError<String>> for PulserError {
    fn from(err: tokio::sync::mpsc::error::SendError<String>) -> Self {
        PulserError::ChannelError(err.to_string())
    }
}

impl From<bincode::Error> for PulserError {
    fn from(err: bincode::Error) -> Self {
        PulserError::StorageError(err.to_string())
    }
}

impl From<std::num::ParseFloatError> for PulserError {
    fn from(err: std::num::ParseFloatError) -> Self { PulserError::ApiError(err.to_string()) }
}

impl From<tokio::time::error::Elapsed> for PulserError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        PulserError::NetworkError("Operation timed out".to_string())
    }
}

impl From<SendError<()>> for PulserError {
    fn from(err: SendError<()>) -> Self {
        PulserError::InternalError(format!("Broadcast send error: {:?}", err))
    }
}

impl From<RecvError> for PulserError {
    fn from(err: RecvError) -> Self {
        PulserError::InternalError(format!("Broadcast receive error: {}", err))
    }
}

impl From<AddrParseError> for PulserError {
    fn from(err: AddrParseError) -> Self {
        PulserError::ConfigError(format!("Invalid IP address: {}", err))
    }
}


