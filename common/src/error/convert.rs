use std::fmt;
use super::types::PulserError;
use super::context::ErrorContext;

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

// BDK errors
impl_from_error!(FromBdkDescriptorError, bdk_wallet::descriptor::DescriptorError, WalletError);
impl_from_error!(FromBdkKeyError, bdk_wallet::keys::KeyError, WalletError);

// Specific conversions that need custom handling
impl FromError<bitcoin::consensus::encode::Error> for PulserError {
    fn from_err(error: bitcoin::consensus::encode::Error, context: Option<ErrorContext>) -> PulserError {
        let err = PulserError::ConsensusError(error.to_string());
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

// Generic error handler for string errors
impl<E: fmt::Display> FromError<E> for PulserError {
    fn from_err(error: E, context: Option<ErrorContext>) -> PulserError {
        let err = PulserError::InternalError(error.to_string());
        if let Some(ctx) = context {
            err.add_context(ctx)
        } else {
            err
        }
    }
}

// Implement From for common error types
// This is where we centralize all the conversion logic

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

// Bitcoin errors - many of these can be moved here
impl From<bitcoin::consensus::encode::Error> for PulserError {
    fn from(err: bitcoin::consensus::encode::Error) -> Self {
        PulserError::from_err(err, None)
    }
}

// Add more conversions for common types here...
