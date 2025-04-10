// deposit-service/src/api/service.rs
use warp::{Filter, Rejection, Reply};
use serde::{Deserialize, Serialize};
use log::{info, error};
use std::sync::Arc;
use common::error::PulserError;
use common::StateManager;
use serde_json::json;

#[derive(Deserialize)]
pub struct ServiceAddressRequest {
    pub user_id: String,
    pub address: String,
}

pub async fn add_service_address(
    body: ServiceAddressRequest,
    state_manager: Arc<StateManager>,
) -> Result<impl Reply, Rejection> {
    match state_manager.add_trusted_service_address(&body.user_id, &body.address).await {
        Ok(_) => {
            info!("Added trusted service address {} for user {}", body.address, body.user_id);
            Ok(warp::reply::json(&json!({
                "status": "success",
                "user_id": body.user_id,
                "address": body.address
            })))
        },
        Err(e) => {
            error!("Failed to add trusted service address: {}", e);
            Err(warp::reject::custom(e))
        }
    }
}

pub async fn remove_service_address(
    body: ServiceAddressRequest,
    state_manager: Arc<StateManager>,
) -> Result<impl Reply, Rejection> {
    match state_manager.remove_trusted_service_address(&body.user_id, &body.address).await {
        Ok(_) => {
            info!("Removed trusted service address {} for user {}", body.address, body.user_id);
            Ok(warp::reply::json(&json!({
                "status": "success",
                "user_id": body.user_id,
                "address": body.address
            })))
        },
        Err(e) => {
            error!("Failed to remove trusted service address: {}", e);
            Err(warp::reject::custom(e))
        }
    }
}

pub fn service_routes(
    state_manager: Arc<StateManager>
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    let add_route = warp::path!("service_address")
        .and(warp::post())
        .and(warp::body::json())
        .and(super::with_state_manager(state_manager.clone()))
        .and_then(add_service_address);
        
    let remove_route = warp::path!("service_address")
        .and(warp::delete())
        .and(warp::body::json())
        .and(super::with_state_manager(state_manager.clone()))
        .and_then(remove_service_address);
        
    let list_route = warp::path!("service_address" / String)
        .and(warp::get())
        .and(super::with_state_manager(state_manager.clone()))
        .and_then(|user_id: String, state_manager: Arc<StateManager>| async move {
            match state_manager.load_stable_chain(&user_id).await {
                Ok(chain) => {
                    let addresses: Vec<String> = chain.trusted_service_addresses.iter().cloned().collect();
                    Ok(warp::reply::json(&json!({
                        "user_id": user_id,
                        "trusted_addresses": addresses
                    })))
                },
                Err(e) => Err(warp::reject::custom(e))
            }
        });
        
    add_route.or(remove_route).or(list_route)
}
