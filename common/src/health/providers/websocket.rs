// common/src/health/providers/websocket.rs
use crate::error::PulserError;
use crate::health::{Component, ComponentType, HealthCheck, HealthResult};
use crate::websocket::WebSocketManager;  // Make sure to import this
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::timeout;

// Updated struct definition to support both legacy and new approaches
pub struct WebSocketCheck {
    component: Component,
    // Fields for legacy approach
    last_activity: Option<Arc<RwLock<Instant>>>,
    connected: Option<Arc<RwLock<bool>>>,
    // Fields for WebSocketManager approach
    manager: Option<Arc<WebSocketManager>>,
    endpoint: Option<String>,
    // Common fields
    max_inactivity_secs: u64,
}

impl WebSocketCheck {
    // Legacy constructor
    pub fn new(last_activity: Arc<RwLock<Instant>>, connected: Arc<RwLock<bool>>) -> Self {
        Self {
            component: Component {
                name: "websocket".to_string(),
                component_type: ComponentType::WebSocket,
                description: "WebSocket connection".to_string(),
                is_critical: false,
            },
            last_activity: Some(last_activity),
            connected: Some(connected),
            manager: None,
            endpoint: None,
            max_inactivity_secs: 60,  // 1 minute max inactivity
        }
    }
    
    // New constructor that takes a WebSocketManager and endpoint
    pub fn new_with_manager(manager: Arc<WebSocketManager>, endpoint: String) -> Self {
        Self {
            component: Component {
                name: "websocket".to_string(),
                component_type: ComponentType::WebSocket,
                description: "WebSocket connection".to_string(),
                is_critical: false,
            },
            last_activity: None,
            connected: None,
            manager: Some(manager),
            endpoint: Some(endpoint),
            max_inactivity_secs: 60,  // 1 minute max inactivity
        }
    }
    
    pub fn with_name(mut self, name: &str) -> Self {
        self.component.name = name.to_string();
        self
    }
    
    pub fn with_critical(mut self, is_critical: bool) -> Self {
        self.component.is_critical = is_critical;
        self
    }
    
    pub fn with_max_inactivity(mut self, max_inactivity_secs: u64) -> Self {
        self.max_inactivity_secs = max_inactivity_secs;
        self
    }
}

#[async_trait]
impl HealthCheck for WebSocketCheck {
    async fn check(&self) -> Result<HealthResult, PulserError> {
        // Check if using WebSocketManager
        if let Some(manager) = &self.manager {
            if let Some(endpoint) = &self.endpoint {
                if manager.is_connected(endpoint).await {
                    // Check if the connection is active recently
if let Some(stats) = manager.get_connection_stats(endpoint) {
                        if let Some(last_msg_time) = stats.last_message_time {
                            let inactivity_secs = last_msg_time.elapsed().as_secs();
                            if inactivity_secs > self.max_inactivity_secs {
                                return Ok(HealthResult::Degraded { 
                                    reason: format!("WebSocket inactive for {}s", inactivity_secs)
                                });
                            }
                        }
                        Ok(HealthResult::Healthy)
                    } else {
                        Ok(HealthResult::Degraded { 
                            reason: "WebSocket connected but no stats available".to_string()
                        })
                    }
                } else {
                    Ok(HealthResult::Unhealthy { 
                        reason: "WebSocket disconnected".to_string()
                    })
                }
            } else {
                Ok(HealthResult::Unhealthy {
                    reason: "No endpoint specified for WebSocket check".to_string()
                })
            }
        } else if let (Some(connected), Some(last_activity)) = (&self.connected, &self.last_activity) {
            // Legacy check for direct RwLock implementation
            let is_connected = match timeout(Duration::from_secs(3), connected.read()).await {
                Ok(connected) => *connected,
                Err(_) => {
                    return Ok(HealthResult::Degraded { 
                        reason: "Timeout checking WebSocket connection status".to_string()
                    });
                }
            };
            
            if !is_connected {
                return Ok(HealthResult::Unhealthy { 
                    reason: "WebSocket disconnected".to_string()
                });
            }
            
            // Check last activity
            let last_activity_time = match timeout(Duration::from_secs(3), last_activity.read()).await {
                Ok(last_activity) => *last_activity,
                Err(_) => {
                    return Ok(HealthResult::Degraded { 
                        reason: "Timeout checking WebSocket last activity".to_string()
                    });
                }
            };
            
            let inactivity_duration = Instant::now().duration_since(last_activity_time).as_secs();
            
            if inactivity_duration > self.max_inactivity_secs {
                return Ok(HealthResult::Degraded { 
                    reason: format!("WebSocket inactive for {}s", inactivity_duration)
                });
            }
            
            Ok(HealthResult::Healthy)
        } else {
            // Neither approach is configured correctly
            Ok(HealthResult::Unhealthy {
                reason: "WebSocketCheck is not properly configured".to_string()
            })
        }
    }
    
    fn component(&self) -> &Component {
        &self.component
    }
}
