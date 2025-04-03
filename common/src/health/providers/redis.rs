use crate::error::PulserError;
use crate::health::{Component, ComponentType, HealthCheck, HealthResult};
use async_trait::async_trait;
use redis::AsyncCommands;
use std::time::Duration;
use tokio::time::timeout;

pub struct RedisCheck {
    component: Component,
    redis_url: String,
}

impl RedisCheck {
    pub fn new(redis_url: &str) -> Self {
        Self {
            component: Component {
                name: "redis".to_string(),
                component_type: ComponentType::Redis,
                description: "Redis cache server".to_string(),
                is_critical: false,
            },
            redis_url: redis_url.to_string(),
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
}

#[async_trait]
impl HealthCheck for RedisCheck {
    async fn check(&self) -> Result<HealthResult, PulserError> {
        let client = match redis::Client::open(&self.redis_url) {
            Ok(client) => client,
            Err(e) => {
                return Ok(HealthResult::Unhealthy { 
                    reason: format!("Failed to create Redis client: {}", e)
                });
            }
        };
        
        // Try to connect
        match timeout(Duration::from_secs(3), client.get_async_connection()).await {
            Ok(Ok(mut conn)) => {
                // Ping Redis
match timeout(Duration::from_secs(2), redis::cmd("PING").query_async::<_, String>(&mut conn)).await {
                    Ok(Ok(response)) if response == "PONG" => {
                        Ok(HealthResult::Healthy)
                    },
                    Ok(Ok(_)) => {
                        Ok(HealthResult::Degraded { 
                            reason: "Redis responded with unexpected value to PING".to_string()
                        })
                    },
                    Ok(Err(e)) => {
                        Ok(HealthResult::Degraded { 
                            reason: format!("Redis PING command failed: {}", e)
                        })
                    },
                    Err(_) => {
                        Ok(HealthResult::Degraded { 
                            reason: "Redis PING command timed out".to_string()
                        })
                    }
                }
            },
            Ok(Err(e)) => {
                Ok(HealthResult::Unhealthy { 
                    reason: format!("Failed to connect to Redis: {}", e)
                })
            },
            Err(_) => {
                Ok(HealthResult::Unhealthy { 
                    reason: "Connection to Redis timed out".to_string()
                })
            }
        }
    }
    
    fn component(&self) -> &Component {
        &self.component
    }
}
