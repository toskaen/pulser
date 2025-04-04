// deposit-service/src/api/health.rs
use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use common::error::PulserError;
// In deposit-service/src/api/health.rs
use common::health::{
    HealthChecker, create_health_response, Component, 
    ComponentType, HealthCheck as CommonHealthCheck, HealthResult
};

// Then use CommonHealthCheck everywhere
use tokio::time::timeout;
use std::time::Duration;
use reqwest::Client;
use async_trait::async_trait;


pub fn health(
    health_checker: Arc<HealthChecker>
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path("health")
        .and(warp::get())
        .and(with_health_checker(health_checker))
        .and_then(health_handler)
}

fn with_health_checker(health_checker: Arc<HealthChecker>) -> impl Filter<Extract = (Arc<HealthChecker>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || health_checker.clone())
}

async fn health_handler(
    health_checker: Arc<HealthChecker>
) -> Result<impl Reply, Rejection> {
    // Use the async checker to get up-to-date health status
    let health_status = health_checker.check_all_async().await;
    
    // Return the health status as JSON
    Ok(warp::reply::json(&health_status))
}

// Add a custom API endpoint check
pub struct ApiEndpointCheck {
    component: Component,
    client: Client,
    url: String,
    timeout_secs: u64,
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
            client: Client::new(),
            url: url.to_string(),
            timeout_secs: 5,
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
}

#[async_trait]
pub trait HealthCheck {
    async fn check(&self) -> Result<HealthResult, PulserError>;
    fn component(&self) -> &Component;
}

#[async_trait]
impl HealthCheck for ApiEndpointCheck {
    async fn check(&self) -> Result<HealthResult, PulserError> {
        match timeout(
            Duration::from_secs(self.timeout_secs), 
            self.client.get(&self.url).send()
        ).await {
            Ok(Ok(response)) => {
                if response.status().is_success() {
                    Ok(HealthResult::Healthy)
                } else {
                    Ok(HealthResult::Degraded { 
                        reason: format!("API returned status: {}", response.status()) 
                    })
                }
            },
            Ok(Err(e)) => Ok(HealthResult::Unhealthy { 
                reason: format!("API request failed: {}", e)
            }),
            Err(_) => Ok(HealthResult::Unhealthy { 
                reason: "API request timed out".to_string()
            }),
        }
    }
    
    fn component(&self) -> &Component {
        &self.component
    }
}
