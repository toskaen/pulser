use crate::error::PulserError;
use crate::health::{Component, ComponentType, HealthCheck, HealthResult};
use crate::price_feed::{PriceFeed, PriceFeedExtensions};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;

pub struct PriceFeedCheck {
    component: Component,
    price_feed: Arc<PriceFeed>,
    min_price: f64,
    max_staleness_secs: u64,
    timeout: Duration, // New field for configurable timeout
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
            timeout: Duration::from_secs(5), // Default timeout
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
    
    pub fn with_timeout(mut self, timeout: Duration) -> Self { // New method
        self.timeout = timeout;
        self
    }
}

#[async_trait]
impl HealthCheck for PriceFeedCheck {
    async fn check(&self) -> Result<HealthResult, PulserError> {
        // Check WebSocket connection status
        let is_connected = self.price_feed.is_websocket_connected().await;
        
        // Get the latest price with configurable timeout
        match timeout(self.timeout, self.price_feed.get_price()).await {
            Ok(Ok(price_info)) => {
                // Check if price is reasonable
                let price = price_info.raw_btc_usd;
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
                    
                let timestamp = price_info.timestamp as u64;
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
            Ok(Err(e)) => {
                // Try Deribit price as fallback with half the timeout
                match timeout(self.timeout / 2, self.price_feed.get_deribit_price()).await {
                    Ok(Ok(deribit_price)) => {
                        if deribit_price < self.min_price {
                            return Ok(HealthResult::Unhealthy { 
                                reason: format!("Deribit price below minimum: ${:.2}", deribit_price)
                            });
                        }
                        Ok(HealthResult::Degraded { 
                            reason: format!("Using Deribit fallback price: ${:.2}", deribit_price)
                        })
                    },
                    _ => {
                        Ok(HealthResult::Unhealthy { 
                            reason: format!("Failed to fetch price: {}", e)
                        })
                    }
                }
            },
            Err(_) => {
                Ok(HealthResult::Unhealthy { 
                    reason: "Timeout fetching price".to_string()
                })
            }
        }
    }
    
    fn component(&self) -> &Component {
        &self.component
    }
}
