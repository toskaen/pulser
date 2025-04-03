use crate::error::PulserError;
use crate::health::{Component, ComponentType, HealthCheck, HealthResult};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::timeout;

pub struct WebSocketCheck {
    component: Component,
    last_activity: Arc<RwLock<Instant>>,
    connected: Arc<RwLock<bool>>,
    max_inactivity_secs: u64,
}

impl WebSocketCheck {
    pub fn new(last_activity: Arc<RwLock<Instant>>, connected: Arc<RwLock<bool>>) -> Self {
        Self {
            component: Component {
                name: "websocket".to_string(),
                component_type: ComponentType::WebSocket,
                description: "WebSocket connection".to_string(),
                is_critical: false,
            },
            last_activity,
            connected,
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
        let is_connected = match timeout(Duration::from_secs(3), self.connected.read()).await {
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
        let last_activity = match timeout(Duration::from_secs(3), self.last_activity.read()).await {
            Ok(last_activity) => *last_activity,
            Err(_) => {
                return Ok(HealthResult::Degraded { 
                    reason: "Timeout checking WebSocket last activity".to_string()
                });
            }
        };
        
        let inactivity_duration = Instant::now().duration_since(last_activity).as_secs();
        
        if inactivity_duration > self.max_inactivity_secs {
            return Ok(HealthResult::Degraded { 
                reason: format!("WebSocket inactive for {}s", inactivity_duration)
            });
        }
        
        Ok(HealthResult::Healthy)
    }
    
    fn component(&self) -> &Component {
        &self.component
    }
}
