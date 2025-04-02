mod config;
mod delivery;
mod retry;

pub use config::{WebhookConfig, WebhookEndpoint};
pub use delivery::{deliver_webhook, WebhookManager, WebhookPayload};
pub use retry::{RetryQueue, RetryStrategy, WebhookEntry};

use crate::error::PulserError;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Get current time in seconds since UNIX epoch
pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}
