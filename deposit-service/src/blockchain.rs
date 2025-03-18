// In deposit-service/src/blockchain.rs
use bitcoin::Network;
use bdk_esplora::esplora_client;
use common::error::PulserError;

/// Create an Esplora blockchain client
pub fn create_esplora_client(network: Network) -> Result<esplora_client::AsyncClient, PulserError> {
    let url = match network {
        Network::Bitcoin => "https://blockstream.info/api/",
        Network::Testnet => "https://blockstream.info/testnet/api/",
        Network::Signet => "https://mempool.space/signet/api/",
        Network::Regtest => "http://localhost:3002/",
        _ => return Err(PulserError::ConfigError("Unsupported network".to_string())),
    };
    
    let client = esplora_client::Builder::new(url)
        .build_async()
        .map_err(|e| PulserError::NetworkError(format!("Failed to create Esplora client: {}", e)))?;
    
    Ok(client)
}

/// Fetch fee rate in satoshis per virtual byte
pub async fn fetch_fee_rate(
    client: &esplora_client::AsyncClient,
) -> Result<f32, PulserError> {
    let fee_estimates = client.get_fee_estimates()
        .await
        .map_err(|e| PulserError::ApiError(format!("Failed to fetch fee estimates: {}", e)))?;
    
    // Get fee for 2 blocks confirmation as a common target
    let fee_rate = fee_estimates.get(&2).cloned().unwrap_or(5.0);
    
    // Convert f64 to f32
    Ok(fee_rate as f32)
}
