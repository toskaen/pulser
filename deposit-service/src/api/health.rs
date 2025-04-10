// deposit-service/src/api/health.rs - Enhanced implementation
use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use tokio::time::timeout;
use reqwest::Client;
use async_trait::async_trait;
use serde_json::{json, Value};
use log::{debug, info, warn};

use common::error::PulserError;
use common::health::{
    HealthChecker, create_health_response, Component, 
    ComponentType, HealthCheck as CommonHealthCheck, HealthResult
};
use common::price_feed::PriceFeed;
use common::websocket::WebSocketManager;

// Constants
const HEALTH_CHECK_TIMEOUT_SECS: u64 = 5;
const HEALTH_CACHE_TTL_SECS: u64 = 10;

// Provide a standard health endpoint
pub fn health(
    health_checker: Arc<HealthChecker>,
    price_feed: Arc<PriceFeed>,
    esplora_url: String,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path("health")
        .and(warp::get())
        .and(with_health_checker(health_checker))
        .and(warp::any().map(move || price_feed.clone()))
        .and(warp::any().map(move || esplora_url.clone()))
        .and_then(health_handler)
}

// Helper to pass health checker to handler
fn with_health_checker(health_checker: Arc<HealthChecker>) -> impl Filter<Extract = (Arc<HealthChecker>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || health_checker.clone())
}

// Enhanced health handler with latency tracking and background updates
async fn health_handler(
    health_checker: Arc<HealthChecker>,
    price_feed: Arc<PriceFeed>,
    esplora_url: String,
) -> Result<impl Reply, Rejection> {
    let start_time = Instant::now();
    
    // Register dynamic components if needed
    register_dynamic_components(health_checker.clone(), price_feed, esplora_url).await;
    
    // Get full health status with timeout
    let health_result = match timeout(
        Duration::from_secs(HEALTH_CHECK_TIMEOUT_SECS),
        health_checker.check_all_async()
    ).await {
        Ok(status) => status,
        Err(_) => {
            // Return partial results if timeout
            warn!("Health check timed out after {}s", HEALTH_CHECK_TIMEOUT_SECS);
            
            // Create a degraded status
            let mut partial_status = health_checker.check_all(); // Uses cached values
            partial_status.overall_status = "degraded".to_string();
            partial_status.components.insert("health_check".to_string(), "timeout".to_string());
            partial_status
        }
    };
    
    // Add execution_time to response
    let elapsed_ms = start_time.elapsed().as_millis() as u64;
    
    // Enhance response with additional metadata
    let mut health_json = json!({
        "status": health_result.overall_status,
        "timestamp": current_timestamp(),
        "components": health_result.components,
        "details": health_result.details,
        "execution_time_ms": elapsed_ms
    });
    
    // Add system details to enriched output
    if let Ok(system_info) = get_system_info().await {
        if let Some(obj) = health_json.as_object_mut() {
            obj.insert("system".to_string(), system_info);
        }
    }
    
    Ok(warp::reply::json(&health_json))
}

// Get system information asynchronously 
async fn get_system_info() -> Result<Value, PulserError> {
    // This would be replaced with actual system metrics in production
    Ok(json!({
        "uptime_sec": 3600, // Example value
        "version": env!("CARGO_PKG_VERSION"),
        "memory_usage_mb": 256, // Example value
        "cpu_usage_percent": 5,  // Example value
    }))
}

// Current timestamp helper
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

// Register dynamically changing components
async fn register_dynamic_components(
    health_checker: Arc<HealthChecker>,
    price_feed: Arc<PriceFeed>,
    esplora_url: String
) {
    // No actual registration here - just demonstrating the pattern
    // In a real implementation, you'd check if components are already registered
    // and only add new ones or update existing ones based on configuration changes
}

// Enhanced API endpoint health check
pub struct ApiEndpointCheck {
    component: Component,
    client: Client,
    url: String,
    timeout_secs: u64,
    last_check: Arc<tokio::sync::RwLock<(Instant, HealthResult)>>,
}

impl ApiEndpointCheck {
    pub fn new(url: &str) -> Self {
        Self {
            component: Component {
                name: "api_endpoint".to_string(),
                component_type: ComponentType::Api,
                description: format!("API endpoint: {}", url),
                is_critical: false,
            },
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_else(|_| Client::new()),
            url: url.to_string(),
            timeout_secs: 5,
            last_check: Arc::new(tokio::sync::RwLock::new((
                Instant::now() - Duration::from_secs(3600), // Set to long ago initially
                HealthResult::Unhealthy { reason: "Not checked yet".to_string() }
            ))),
        }
    }
    
    pub fn with_critical(mut self, is_critical: bool) -> Self {
        self.component.is_critical = is_critical;
        self
    }
    
    pub fn with_name(mut self, name: &str) -> Self {
        self.component.name = name.to_string();
        self
    }
    
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = timeout_secs;
        self
    }
}

#[async_trait]
impl CommonHealthCheck for ApiEndpointCheck {
    async fn check(&self) -> Result<HealthResult, PulserError> {
        // Check if we have a recent result (within last 5 seconds)
        {
            let last_check = self.last_check.read().await;
            if last_check.0.elapsed() < Duration::from_secs(5) {
                return Ok(last_check.1.clone());
            }
        }
        
        // Perform the actual check with timeout
        let result = match timeout(
            Duration::from_secs(self.timeout_secs),
            self.client.get(&self.url).send()
        ).await {
            Ok(Ok(response)) => {
                if response.status().is_success() {
                    HealthResult::Healthy
                } else {
                    HealthResult::Degraded {
                        reason: format!("API returned status: {}", response.status())
                    }
                }
            },
            Ok(Err(e)) => HealthResult::Unhealthy {
                reason: format!("API request failed: {}", e)
            },
            Err(_) => HealthResult::Unhealthy {
                reason: "API request timed out".to_string()
            },
        };
        
        // Update last check result
        {
            let mut last_check = self.last_check.write().await;
            *last_check = (Instant::now(), result.clone());
        }
        
        Ok(result)
    }

    fn component(&self) -> &Component {
        &self.component
    }
}

// Enhanced WebSocket health check with detailed status
pub struct EnhancedWebSocketCheck {
    component: Component,
    manager: Arc<WebSocketManager>,
    endpoint: String,
    max_inactivity_secs: u64,
    check_interval_secs: u64,
    last_check: Arc<tokio::sync::RwLock<(Instant, HealthResult)>>,
}

impl EnhancedWebSocketCheck {
    pub fn new(manager: Arc<WebSocketManager>, endpoint: String) -> Self {
        Self {
            component: Component {
                name: "websocket".to_string(),
                component_type: ComponentType::WebSocket,
                description: format!("WebSocket connection: {}", endpoint),
                is_critical: true,
            },
            manager,
            endpoint,
            max_inactivity_secs: 60,
            check_interval_secs: 5,
            last_check: Arc::new(tokio::sync::RwLock::new((
                Instant::now() - Duration::from_secs(3600), // Set to long ago initially
                HealthResult::Unhealthy { reason: "Not checked yet".to_string() }
            ))),
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
    
    pub fn with_check_interval(mut self, check_interval_secs: u64) -> Self {
        self.check_interval_secs = check_interval_secs;
        self
    }
}

#[async_trait]
impl CommonHealthCheck for EnhancedWebSocketCheck {
    async fn check(&self) -> Result<HealthResult, PulserError> {
        // Check if we have a recent result (within configured interval)
        {
            let last_check = self.last_check.read().await;
            if last_check.0.elapsed() < Duration::from_secs(self.check_interval_secs) {
                return Ok(last_check.1.clone());
            }
        }
        
        // Check WebSocket connection state
        let is_connected = self.manager.is_connected(&self.endpoint).await;
        
        let result = if is_connected {
            // Check connection stats for recent activity
            if let Some(stats) = self.manager.get_connection_stats(&self.endpoint).await {
                if let Some(last_msg_time) = stats.last_message_time {
                    let inactivity_secs = last_msg_time.elapsed().as_secs();
                    
                    if inactivity_secs > self.max_inactivity_secs {
                        HealthResult::Degraded { 
                            reason: format!("WebSocket inactive for {}s", inactivity_secs)
                        }
                    } else {
                        // Healthy with detailed stats
                        HealthResult::Healthy
                    }
                } else {
                    HealthResult::Degraded { 
                        reason: "WebSocket connected but no messages received".to_string()
                    }
                }
            } else {
                HealthResult::Degraded { 
                    reason: "WebSocket connected but no stats available".to_string()
                }
            }
        } else {
            HealthResult::Unhealthy { 
                reason: "WebSocket disconnected".to_string()
            }
        };
        
        // Update last check result
        {
            let mut last_check = self.last_check.write().await;
            *last_check = (Instant::now(), result.clone());
        }
        
        Ok(result)
    }
    
    fn component(&self) -> &Component {
        &self.component
    }
}
