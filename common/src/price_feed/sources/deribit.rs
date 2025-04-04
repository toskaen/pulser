// common/src/price_feed/sources/deribit.rs
use crate::error::PulserError;
use super::{PriceProvider, PriceSource};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH, Instant, Duration};
use tokio::time::timeout;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct DeribitProvider {
    pub weight: f64,
    pub timeout_secs: u64,
    pub futures_only: bool,
    // Add these fields to support WebSocket functionality
    pub last_ws_activity: Arc<RwLock<Instant>>,
    pub connected: Arc<RwLock<bool>>,
    pub active_ws: Arc<tokio::sync::Mutex<Option<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>>,
    pub is_connecting: Arc<RwLock<bool>>,
    pub last_price_update: Arc<RwLock<Instant>>,
}

// Add to common/src/price_feed/sources/deribit.rs
impl std::fmt::Debug for DeribitProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeribitProvider")
            .field("weight", &self.weight)
            .field("timeout_secs", &self.timeout_secs)
            .field("futures_only", &self.futures_only)
            .finish_non_exhaustive() // Use this to skip fields that don't implement Debug
    }
}

impl DeribitProvider {
    pub fn new() -> Self {
        Self {
            weight: 0.8, // Lower weight as this is futures, not spot
            timeout_secs: 5,
            futures_only: true,
            // Initialize the WebSocket state fields
            last_ws_activity: Arc::new(RwLock::new(Instant::now())),
            connected: Arc::new(RwLock::new(false)),
            active_ws: Arc::new(tokio::sync::Mutex::new(None)),
            is_connecting: Arc::new(RwLock::new(false)),
            last_price_update: Arc::new(RwLock::new(Instant::now())),
        }
    }
    
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }
    
    pub fn with_futures_only(mut self, futures_only: bool) -> Self {
        self.futures_only = futures_only;
        self
    }

    // Add a helper method for connecting to Deribit WebSocket
    pub async fn connect_deribit(&self, api_key: &str, secret: &str) -> Result<(), PulserError> {
        // Implementation would connect to Deribit WebSocket API
        // This is a placeholder that would be replaced with actual implementation
        tokio::time::sleep(Duration::from_millis(100)).await; // Simulate connection
        
        // Update connected state
        let mut connected = self.connected.write().await;
        *connected = true;
        let mut last_activity = self.last_ws_activity.write().await;
        *last_activity = Instant::now();
        
        Ok(())
    }
}

#[async_trait]
impl PriceProvider for DeribitProvider {
    async fn fetch_price(&self, client: &Client) -> Result<PriceSource, PulserError> {
        // Get the perpetual price (BTC-PERPETUAL)
        let url = "https://www.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL";
        
        let response = match timeout(
            Duration::from_secs(self.timeout_secs),
            client.get(url).send()
        ).await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => return Err(PulserError::NetworkError(format!("Deribit request failed: {}", e))),
            Err(_) => return Err(PulserError::NetworkError("Deribit request timed out".to_string())),
        };
        
        if !response.status().is_success() {
            return Err(PulserError::ApiError(format!("Deribit API error: {}", response.status())));
        }
        
        let json: Value = match timeout(
            Duration::from_secs(self.timeout_secs),
            response.json()
        ).await {
            Ok(Ok(json)) => json,
            Ok(Err(e)) => return Err(PulserError::ApiError(format!("Failed to parse Deribit response: {}", e))),
            Err(_) => return Err(PulserError::ApiError("Timeout parsing Deribit response".to_string())),
        };
        
        let price = json["result"]["last_price"].as_f64()
            .ok_or_else(|| PulserError::ApiError("Invalid Deribit response structure".to_string()))?;
            
        // For volume, we'll use the 24h volume from the index (or estimate if not available)
        let volume = json["result"]["stats"]["volume"].as_f64()
            .unwrap_or_else(|| {
                // If volume isn't available, use a reasonable estimate
                json["result"]["open_interest"].as_f64().unwrap_or(1000.0)
            });
            
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();

        // Update last activity timestamp
        let mut last_activity = self.last_ws_activity.write().await;
        *last_activity = Instant::now();
            
        Ok(PriceSource {
            name: "Deribit".to_string(),
            url: url.to_string(),
            weight: self.weight,
            price,
            volume,
            timestamp: now,
            error_count: 0,
            last_success: now,
        })
    }
    
    fn name(&self) -> &str {
        "Deribit"
    }
    
    fn weight(&self) -> f64 {
        self.weight
    }
}
