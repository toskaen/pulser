// common/src/error/mod.rs
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
use bitcoin::consensus::encode::Error as ConsensusError;
use warp;
use bincode;
use tokio::sync::broadcast::error::{SendError, RecvError};
use std::net::AddrParseError;

// Import from submodules
pub use self::context::ErrorContext;
pub use self::types::{ErrorCategory, ContextualError};
pub use self::convert::FromError;

// Submodules
mod context;
mod types;
mod convert;

// Re-exports from submodules for backward compatibility
pub use types::PulserError;

// Add the semi-colon here
pub use crate::with_context;

impl warp::reject::Reject for PulserError {}

// Remove all duplicate From implementations from here to avoid conflicts

// In common/src/error/mod.rs - define with_context as a macro at crate level
#[macro_export]
macro_rules! with_context {
    ($result:expr, $context:expr) => {
        $result.map_err(|e| {
            let ctx = $context();
            let error = crate::error::PulserError::from(e);
            error.add_context(ctx)
        })
    };
}
