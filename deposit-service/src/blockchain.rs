use bdk_esplora::esplora_client::{self, AsyncClient};
use common::error::PulserError;

pub fn create_blockchain(url: &str) -> Result<AsyncClient, PulserError> {
    esplora_client::Builder::new(url)
        .build_async()
        .map_err(|e| PulserError::ApiError(format!("Esplora client error: {}", e)))
}
