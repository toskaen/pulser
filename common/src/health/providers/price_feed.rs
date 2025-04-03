use crate::error::PulserError;
use crate::health::{Component, ComponentType, HealthCheck, HealthResult};
use crate::price_feed::PriceFeed;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct PriceFeedCheck {
    component: Component,
    price_feed: Arc<PriceFeed>,
    min_price: f64,
    max_staleness_secs: u64,
}

impl PriceFeedCheck {
    pub fn new(price_feed: Arc<PriceFeed>) -> Self {
        Self {
            component: Component {
                name: "price_feed".to_string(),
                component_type: ComponentType::PriceFeed,
                description: "Bitcoin price feed service".to_string(),
                is_critical: true,
            },
            price_feed,
            min_price: 2121.0,  // Minimum reasonable BTC price
            max_staleness_secs: 120,  // 2 minutes
        }
    }
    
    pub fn with_min_price(mut self, min_price: f64) -> Self {
        self.min_price = min_price;
        self
    }
    
    pub fn with_max_staleness(mut self, max_staleness_secs: u64) -> Self {
        self.max_staleness_secs = max_staleness_secs;
        self
    }
}

#[async_trait]
impl HealthCheck for PriceFeedCheck {
    async fn check(&self) -> Result<HealthResult, PulserError> {
        // Check WebSocket connection status
        let is_connected = self.price_feed.is_websocket_connected().await;
        
        // Get the latest price
        match self.price_feed.get_deribit_price().await {
            Ok(price) => {
                // Check if price is reasonable
                if price < self.min_price {
                    return Ok(HealthResult::Unhealthy { 
                        reason: format!("Price below minimum threshold: ${:.2}", price)
                    });
                }
                
                // Check if price is stale
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_else(|_| Duration::from_secs(0))
                    .as_secs();
                    
                let timestamp = {
                    let last_update = self.price_feed.last_deribit_update.read().await;
                    *last_update as u64
                };
                
                let staleness = now.saturating_sub(timestamp);
                
                if staleness > self.max_staleness_secs {
                    return Ok(HealthResult::Degraded { 
                        reason: format!("Price is stale: {}s old", staleness)
                    });
                }
                
                // WebSocket connection check
                if !is_connected {
                    return Ok(HealthResult::Degraded { 
                        reason: "WebSocket disconnected, using fallback price sources".to_string()
                    });
                }
                
                Ok(HealthResult::Healthy)
            },
            Err(e) => {
                Ok(HealthResult::Unhealthy { 
                    reason: format!("Failed to fetch price: {}", e)
                })
            }
        }
    }
    
    fn component(&self) -> &Component {
        &self.component
    }
}
