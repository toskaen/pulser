// common/src/error/convert.rs
use std::fmt;
use super::context::ErrorContext;
use super::types::PulserError;

// Trait for converting any error to PulserError
pub trait FromError<E> {
    fn from_err(error: E, context: Option<ErrorContext>) -> PulserError;
}

// Macro to implement error conversion traits
macro_rules! impl_from_error {
    ($trait_name:ident, $error_type:ty, $pulser_variant:ident) => {
        pub trait $trait_name {
            fn into_pulser_err(self) -> PulserError;
            fn into_pulser_err_with_context(self, context: ErrorContext) -> PulserError;
        }

        impl $trait_name for $error_type {
            fn into_pulser_err(self) -> PulserError {
                PulserError::$pulser_variant(self.to_string())
            }

            fn into_pulser_err_with_context(self, context: ErrorContext) -> PulserError {
                PulserError::$pulser_variant(self.to_string()).add_context(context)
            }
        }

        impl FromError<$error_type> for PulserError {
            fn from_err(error: $error_type, context: Option<ErrorContext>) -> PulserError {
                let err = PulserError::$pulser_variant(error.to_string());
                if let Some(ctx) = context {
                    err.add_context(ctx)
                } else {
                    err
                }
            }
        }
    };
}

// Common error conversions
impl_from_error!(FromIoError, std::io::Error, StorageError);
impl_from_error!(FromJsonError, serde_json::Error, StorageError);
impl_from_error!(FromReqwestError, reqwest::Error, NetworkError);
impl_from_error!(FromTomlError, toml::de::Error, ConfigError);
impl_from_error!(FromRedisError, redis::RedisError, StorageError);
impl_from_error!(FromBincodeError, bincode::Error, StorageError);
impl_from_error!(FromSystemTimeError, std::time::SystemTimeError, InternalError);
impl_from_error!(FromEsploraError, Box<bdk_esplora::esplora_client::Error>, ApiError);
impl_from_error!(FromCannotConnectError, bdk_chain::local_chain::CannotConnectError, WalletError);
impl_from_error!(FromFromScriptError, bitcoin::address::FromScriptError, BitcoinError);
impl_from_error!(FromParseIntError, std::num::ParseIntError, InvalidInput);
impl_from_error!(FromMpscSendError, tokio::sync::mpsc::error::SendError<String>, ChannelError);
impl_from_error!(FromBroadcastSendError, tokio::sync::broadcast::error::SendError<()>, ChannelError);
impl_from_error!(FromBroadcastRecvError, tokio::sync::broadcast::error::RecvError, ChannelError);
impl_from_error!(FromAddrParseError, std::net::AddrParseError, ConfigError);
// BDK errors
impl_from_error!(FromBdkDescriptorError, bdk_wallet::descriptor::DescriptorError, WalletError);
impl_from_error!(FromBdkKeyError, bdk_wallet::keys::KeyError, WalletError);

// Specific conversions that need custom handling
impl FromError<bitcoin::consensus::encode::Error> for PulserError {
    fn from_err(error: bitcoin::consensus::encode::Error, context: Option<ErrorContext>) -> PulserError {
        let err = PulserError::BitcoinError(format!("Consensus error: {}", error));
        if let Some(ctx) = context {
            err.add_context(ctx)
        } else {
            err
        }
    }
}

impl FromError<bitcoin::address::ParseError> for PulserError {
    fn from_err(error: bitcoin::address::ParseError, context: Option<ErrorContext>) -> PulserError {
        let err = PulserError::BitcoinError(format!("Address parse error: {}", error));
        if let Some(ctx) = context {
            err.add_context(ctx)
        } else {
            err
        }
    }
}

// IMPORTANT: Remove the generic implementation completely
// DO NOT include this implementation anymore:
// impl<E: fmt::Display> FromError<E> for PulserError where ...

// IO errors
impl From<std::io::Error> for PulserError {
    fn from(err: std::io::Error) -> Self {
        err.into_pulser_err()
    }
}

// JSON errors
impl From<serde_json::Error> for PulserError {
    fn from(err: serde_json::Error) -> Self {
        err.into_pulser_err()
    }
}

// TOML errors
impl From<toml::de::Error> for PulserError {
    fn from(err: toml::de::Error) -> Self {
        err.into_pulser_err()
    }
}

// Redis errors
impl From<redis::RedisError> for PulserError {
    fn from(err: redis::RedisError) -> Self {
        err.into_pulser_err()
    }
}

// Bitcoin errors
impl From<bitcoin::consensus::encode::Error> for PulserError {
    fn from(err: bitcoin::consensus::encode::Error) -> Self {
        PulserError::from_err(err, None)
    }
}

// Bincode errors
impl From<bincode::Error> for PulserError {
    fn from(err: bincode::Error) -> Self {
        PulserError::StorageError(format!("Bincode error: {}", err))
    }
}

// SystemTime errors
impl From<std::time::SystemTimeError> for PulserError {
    fn from(err: std::time::SystemTimeError) -> Self {
        PulserError::InternalError(format!("SystemTime error: {}", err))
    }
}

// Esplora errors
impl From<Box<bdk_esplora::esplora_client::Error>> for PulserError {
    fn from(err: Box<bdk_esplora::esplora_client::Error>) -> Self {
        PulserError::ApiError(format!("Esplora error: {}", err))
    }
}

// CannotConnect errors
impl From<bdk_chain::local_chain::CannotConnectError> for PulserError {
    fn from(err: bdk_chain::local_chain::CannotConnectError) -> Self {
        PulserError::WalletError(format!("Cannot connect: {}", err))
    }
}

// FromScript errors
impl From<bitcoin::address::FromScriptError> for PulserError {
    fn from(err: bitcoin::address::FromScriptError) -> Self {
        PulserError::BitcoinError(format!("FromScript error: {}", err))
    }
}

// ParseInt errors
impl From<std::num::ParseIntError> for PulserError {
    fn from(err: std::num::ParseIntError) -> Self {
        PulserError::InvalidInput(format!("ParseInt error: {}", err))
    }
}

impl From<reqwest::Error> for PulserError {
    fn from(err: reqwest::Error) -> Self {
        PulserError::NetworkError(format!("HTTP request failed: {}", err))
    }
}

impl From<bitcoin::network::ParseNetworkError> for PulserError {
    fn from(err: bitcoin::network::ParseNetworkError) -> Self {
        PulserError::BitcoinError(format!("Network parse error: {}", err))
    }
}

impl From<bitcoin::address::ParseError> for PulserError {
    fn from(err: bitcoin::address::ParseError) -> Self {
        PulserError::BitcoinError(format!("Address parse error: {}", err))
    }
}

impl From<bdk_esplora::esplora_client::Error> for PulserError {
    fn from(err: bdk_esplora::esplora_client::Error) -> Self {
        PulserError::ApiError(format!("Esplora client error: {}", err))
    }
}

impl From<tokio::time::error::Elapsed> for PulserError {
    fn from(err: tokio::time::error::Elapsed) -> Self {
        PulserError::NetworkError(format!("Operation timed out: {}", err))
    }
}

impl From<bitcoin::bip32::Error> for PulserError {
    fn from(err: bitcoin::bip32::Error) -> Self {
        PulserError::BitcoinError(format!("BIP32 error: {}", err))
    }
}

impl From<bdk_wallet::miniscript::descriptor::DescriptorKeyParseError> for PulserError {
    fn from(err: bdk_wallet::miniscript::descriptor::DescriptorKeyParseError) -> Self {
        PulserError::WalletError(format!("Descriptor key parse error: {}", err))
    }
}

impl From<bdk_wallet::descriptor::DescriptorError> for PulserError {
    fn from(err: bdk_wallet::descriptor::DescriptorError) -> Self {
        PulserError::WalletError(format!("Descriptor error: {}", err))
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for PulserError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        PulserError::NetworkError(format!("WebSocket error: {}", err))
    }
}

impl From<tokio::sync::mpsc::error::SendError<String>> for PulserError {
    fn from(err: tokio::sync::mpsc::error::SendError<String>) -> Self {
        PulserError::ChannelError(format!("MPSC send error: {}", err))
    }
}

impl From<tokio::sync::broadcast::error::SendError<()>> for PulserError {
    fn from(err: tokio::sync::broadcast::error::SendError<()>) -> Self {
        PulserError::ChannelError(format!("Broadcast send error: {}", err))
    }
}

impl From<tokio::sync::broadcast::error::RecvError> for PulserError {
    fn from(err: tokio::sync::broadcast::error::RecvError) -> Self {
        PulserError::ChannelError(format!("Broadcast receive error: {}", err))
    }
}

impl From<std::net::AddrParseError> for PulserError {
    fn from(err: std::net::AddrParseError) -> Self {
        PulserError::ConfigError(format!("Address parsing error: {}", err))
    }
}

impl From<tokio::sync::AcquireError> for PulserError {
    fn from(_: tokio::sync::AcquireError) -> Self {
        PulserError::ApiError("Semaphore acquire failed".to_string())
    }
}
