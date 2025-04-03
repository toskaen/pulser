use crate::error::PulserError;
use async_trait::async_trait;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ComponentType {
    Database,
    PriceFeed,
    Blockchain,
    WebSocket,
    Redis,
    Api,
    Other(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Component {
    pub name: String,
    pub component_type: ComponentType,
    pub description: String,
    pub is_critical: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthResult {
    Healthy,
    Degraded { reason: String },
    Unhealthy { reason: String },
}

impl HealthResult {
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthResult::Healthy)
    }

    pub fn as_str(&self) -> &str {
        match self {
            HealthResult::Healthy => "healthy",
            HealthResult::Degraded { .. } => "degraded",
            HealthResult::Unhealthy { .. } => "unhealthy",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub overall_status: String,
    pub components: HashMap<String, String>,
    pub details: HashMap<String, serde_json::Value>,
    pub timestamp: u64,
}

#[async_trait]
pub trait HealthCheck: Send + Sync {
    async fn check(&self) -> Result<HealthResult, PulserError>;
    fn component(&self) -> &Component;
}

pub struct HealthChecker {
    checks: HashMap<String, Arc<dyn HealthCheck>>,
    critical_components: HashSet<String>,
    cache: Arc<RwLock<Option<(HealthStatus, u64)>>>,
    cache_ttl_secs: u64,
}

impl HealthChecker {
    pub fn new() -> Self {
        Self {
            checks: HashMap::new(),
            critical_components: HashSet::new(),
            cache: Arc::new(RwLock::new(None)),
            cache_ttl_secs: 5, // Cache health results for 5 seconds by default
        }
    }

    pub fn with_cache_ttl(mut self, ttl_secs: u64) -> Self {
        self.cache_ttl_secs = ttl_secs;
        self
    }

    pub fn register<T: HealthCheck + 'static>(&mut self, check: T) {
        let component = check.component();
        let name = component.name.clone();
        
        if component.is_critical {
            self.critical_components.insert(name.clone());
        }
        
        self.checks.insert(name, Arc::new(check));
    }

    pub async fn check_component(&self, name: &str) -> Result<HealthResult, PulserError> {
        match self.checks.get(name) {
            Some(check) => {
                match timeout(Duration::from_secs(5), check.check()).await {
                    Ok(result) => result,
                    Err(_) => {
                        warn!("Health check timed out for component: {}", name);
                        Ok(HealthResult::Degraded { 
                            reason: "Health check timed out".to_string() 
                        })
                    }
                }
            },
            None => Err(PulserError::InternalError(format!("Component not found: {}", name))),
        }
    }

    pub fn check_all(&self) -> HealthStatus {
        // Try to use cached result if available and recent
        if let Ok(cache) = self.cache.try_read() {
            if let Some((cached_status, timestamp)) = &*cache {
                let now = super::current_timestamp();
                if now - timestamp < self.cache_ttl_secs {
                    return cached_status.clone();
                }
            }
        }

        // Perform check if cache is stale or unavailable
        let result = self.check_all_now();
        
        // Update cache with new result
        if let Ok(mut cache) = self.cache.try_write() {
            *cache = Some((result.clone(), super::current_timestamp()));
        }
        
        result
    }

    pub async fn check_all_async(&self) -> HealthStatus {
        // Try to use cached result if available and recent
        let use_cache = {
            let cache = self.cache.read().await;
            if let Some((cached_status, timestamp)) = &*cache {
                let now = super::current_timestamp();
                if now - timestamp < self.cache_ttl_secs {
                    return cached_status.clone();
                }
            }
            false
        };

        if !use_cache {
            // Perform check in parallel
            let mut components = HashMap::new();
            let mut details = HashMap::new();
            let mut critical_failures = 0;
            let mut non_critical_failures = 0;
            
            let mut checks = Vec::new();
            for (name, check) in &self.checks {
                checks.push((name.clone(), check.clone()));
            }
            
            let results = futures::future::join_all(checks.into_iter().map(|(name, check)| {
                async move {
                    let result = timeout(Duration::from_secs(5), check.check()).await;
                    (name, result)
                }
            })).await;
            
            for (name, result) in results {
                match result {
                    Ok(Ok(health_result)) => {
                        let is_critical = self.critical_components.contains(&name);
                        let status_str = health_result.as_str().to_string();
                        
                        components.insert(name.clone(), status_str);
                        
                        match &health_result {
                            HealthResult::Healthy => {
                                details.insert(name, serde_json::json!({ "status": "healthy" }));
                            },
                            HealthResult::Degraded { reason } => {
                                details.insert(name.clone(), serde_json::json!({ 
                                    "status": "degraded", 
                                    "reason": reason 
                                }));
                                non_critical_failures += 1;
                            },
                            HealthResult::Unhealthy { reason } => {
                                details.insert(name.clone(), serde_json::json!({ 
                                    "status": "unhealthy", 
                                    "reason": reason 
                                }));
                                if is_critical {
                                    critical_failures += 1;
                                } else {
                                    non_critical_failures += 1;
                                }
                            },
                        }
                    },
                    Ok(Err(e)) => {
                        let is_critical = self.critical_components.contains(&name);
                        components.insert(name.clone(), "error".to_string());
                        details.insert(name.clone(), serde_json::json!({ 
                            "status": "error", 
                            "error": e.to_string() 
                        }));
                        
                        if is_critical {
                            critical_failures += 1;
                        } else {
                            non_critical_failures += 1;
                        }
                    },
                    Err(_) => {
                        let is_critical = self.critical_components.contains(&name);
                        components.insert(name.clone(), "timeout".to_string());
                        details.insert(name.clone(), serde_json::json!({ 
                            "status": "timeout", 
                            "error": "Health check timed out" 
                        }));
                        
                        if is_critical {
                            critical_failures += 1;
                        } else {
                            non_critical_failures += 1;
                        }
                    }
                }
            }
            
            let overall_status = if critical_failures > 0 {
                "critical"
            } else if non_critical_failures > 0 {
                "degraded"
            } else {
                "healthy"
            };
            
            let status = HealthStatus {
                overall_status: overall_status.to_string(),
                components,
                details,
                timestamp: super::current_timestamp(),
            };
            
            // Update cache
            let mut cache = self.cache.write().await;
            *cache = Some((status.clone(), super::current_timestamp()));
            
            return status;
        }
        
        unreachable!("This should be unreachable due to early return");
    }

    fn check_all_now(&self) -> HealthStatus {
        let mut components = HashMap::new();
        let mut details = HashMap::new();
        let mut critical_failures = 0;
        let mut non_critical_failures = 0;
        
        for (name, check) in &self.checks {
            let is_critical = self.critical_components.contains(name);
            
            let runtime = tokio::runtime::Handle::current();
            let result = runtime.block_on(async {
                timeout(Duration::from_secs(3), check.check()).await
            });
            
            match result {
                Ok(Ok(health_result)) => {
                    let status_str = health_result.as_str().to_string();
                    components.insert(name.clone(), status_str);
                    
                    match &health_result {
                        HealthResult::Healthy => {
                            details.insert(name.clone(), serde_json::json!({ "status": "healthy" }));
                        },
                        HealthResult::Degraded { reason } => {
                            details.insert(name.clone(), serde_json::json!({ 
                                "status": "degraded", 
                                "reason": reason 
                            }));
                            non_critical_failures += 1;
                        },
                        HealthResult::Unhealthy { reason } => {
                            details.insert(name.clone(), serde_json::json!({ 
                                "status": "unhealthy", 
                                "reason": reason 
                            }));
                            if is_critical {
                                critical_failures += 1;
                            } else {
                                non_critical_failures += 1;
                            }
                        },
                    }
                },
                Ok(Err(e)) => {
                    components.insert(name.clone(), "error".to_string());
                    details.insert(name.clone(), serde_json::json!({ 
                        "status": "error", 
                        "error": e.to_string() 
                    }));
                    
                    if is_critical {
                        critical_failures += 1;
                    } else {
                        non_critical_failures += 1;
                    }
                },
                Err(_) => {
                    components.insert(name.clone(), "timeout".to_string());
                    details.insert(name.clone(), serde_json::json!({ 
                        "status": "timeout", 
                        "error": "Health check timed out" 
                    }));
                    
                    if is_critical {
                        critical_failures += 1;
                    } else {
                        non_critical_failures += 1;
                    }
                }
            }
        }
        
        let overall_status = if critical_failures > 0 {
            "critical"
        } else if non_critical_failures > 0 {
            "degraded"
        } else {
            "healthy"
        };
        
        HealthStatus {
            overall_status: overall_status.to_string(),
            components,
            details,
            timestamp: super::current_timestamp(),
        }
    }
}
