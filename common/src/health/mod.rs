mod checker;
pub mod providers;

pub use checker::{HealthCheck, HealthChecker, HealthStatus, HealthResult, Component, ComponentType};

use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_secs()
}

// Convenience function to create a standard health check response
pub fn create_health_response(checker: &HealthChecker) -> Value {
    let status = checker.check_all();
    serde_json::json!({
        "status": status.overall_status,
        "timestamp": current_timestamp(),
        "components": status.components,
        "details": status.details
    })
}
