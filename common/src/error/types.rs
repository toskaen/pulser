use std::fmt;
use super::context::ErrorContext;
use super::PulserError;
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
