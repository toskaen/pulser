// deposit-service/src/api/health.rs
use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use common::error::PulserError;
use common::health::{HealthChecker, create_health_response};

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
