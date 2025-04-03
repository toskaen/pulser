use std::collections::HashMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct ErrorContext {
    pub code: String,
    pub user_id: Option<String>,
    pub file: String,
    pub line: u32,
    pub timestamp: u64,
    pub details: HashMap<String, String>,
}

impl ErrorContext {
    pub fn new(code: &str, file: &str, line: u32) -> Self {
        Self {
            code: code.to_string(),
            user_id: None,
            file: file.to_string(),
            line,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| std::time::Duration::from_secs(0))
                .as_secs(),
            details: HashMap::new(),
        }
    }

    pub fn with_user_id(mut self, user_id: &str) -> Self {
        self.user_id = Some(user_id.to_string());
        self
    }

    pub fn with_detail(mut self, key: &str, value: &str) -> Self {
        self.details.insert(key.to_string(), value.to_string());
        self
    }

    pub fn merge(&mut self, other: ErrorContext) {
        if self.user_id.is_none() {
            self.user_id = other.user_id;
        }
        for (k, v) in other.details {
            self.details.insert(k, v);
        }
    }
}

impl fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{} [{}]", self.file, self.line, self.code)?;
        
        if let Some(ref user_id) = self.user_id {
            write!(f, " user_id={}", user_id)?;
        }
        
        if !self.details.is_empty() {
            write!(f, " details={{")?;
            let mut first = true;
            for (k, v) in &self.details {
                if !first {
                    write!(f, ", ")?;
                }
                write!(f, "{}={}", k, v)?;
                first = false;
            }
            write!(f, "}}")?;
        }
        
        Ok(())
    }
}
