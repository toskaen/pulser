mod types;
mod convert;
mod context;

pub use types::{PulserError, ErrorCategory};
pub use context::{ErrorContext, with_context};
pub use convert::FromError;

// Re-export traits that are common conversions
pub use convert::{
    FromIoError,
    FromJsonError,
    FromRedisError,
    FromReqwestError,
    FromBdkError,
};
