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
use log::{info, warn};
use crate::price_feed::PriceFeed;
use crate::price_feed::PriceFeedExtensions;


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

impl std::fmt::Debug for DeribitProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeribitProvider")
            .field("weight", &self.weight)
            .field("timeout_secs", &self.timeout_secs)
            .field("futures_only", &self.futures_only)
            .finish_non_exhaustive() // Use this to skip fields that don't implement Debug
    }
}


impl PriceFeed {
    pub async fn ensure_websocket_connection(&self) -> Result<bool, PulserError> {
        // Check current connection state
        {
            let connected = self.connected.read().await;
            if *connected {
                return Ok(true);
            }
        }
        
        // Check if already trying to connect
        {
            let is_connecting = self.is_connecting.read().await;
            if *is_connecting {
                return Ok(false); // Connection attempt in progress
            }
        }
        
        // Mark as connecting
        {
            let mut is_connecting = self.is_connecting.write().await;
            *is_connecting = true;
        }
        
        // Use a deferred cleanup to ensure is_connecting is reset
        struct Defer<F: FnOnce()>(Option<F>);
        impl<F: FnOnce()> Drop for Defer<F> {
            fn drop(&mut self) {
                if let Some(f) = self.0.take() { f() }
            }
        }
        
        let is_connecting_ref = self.is_connecting.clone();
        let defer = Defer(Some(|| {
            tokio::spawn(async move {
                let mut is_connecting = is_connecting_ref.write().await;
                *is_connecting = false;
            });
        }));
        
        // Connect with exponential backoff
        let mut backoff = Duration::from_millis(100);
        let max_backoff = Duration::from_secs(60);
        let mut attempts = 0;
        
        while attempts < 5 {
            info!("Attempting to connect to Deribit WebSocket (attempt {}/5)", attempts + 1);
            
            match self.connect_deribit_websocket().await {
                Ok(_) => {
                    // Successfully connected
                    let mut connected = self.connected.write().await;
                    *connected = true;
                    
                    let mut last_activity = self.last_ws_activity.write().await;
                    *last_activity = Instant::now();
                    
                    info!("Successfully connected to Deribit WebSocket");
                    return Ok(true);
                },
                Err(e) => {
                    warn!("Failed to connect to Deribit WebSocket: {}", e);
                    attempts += 1;
                    
                    if attempts >= 5 {
                        break;
                    }
                    
                    // Exponential backoff with jitter
                    let jitter = rand::random::<u64>() % 100;
                    backoff = std::cmp::min(backoff * 2, max_backoff) + Duration::from_millis(jitter);
                    tokio::time::sleep(backoff).await;
                }
            }
        }
        
        warn!("Failed to connect to Deribit WebSocket after 5 attempts");
        Ok(false)
    }
    
    // Add heartbeat mechanism to maintain connection
    pub async fn start_heartbeat_monitor(&self) {
        let ping_interval = Duration::from_secs(30);
        let mut interval = tokio::time::interval(ping_interval);
        
        loop {
            interval.tick().await;
            
            // Check if connected
            let is_connected = {
                let connected = self.connected.read().await;
                *connected
            };
            
            if !is_connected {
                // Try to reconnect
                if let Err(e) = self.ensure_websocket_connection().await {
                    warn!("Failed to reconnect WebSocket: {}", e);
                }
                continue;
            }
            
            // Send ping to keep connection alive
            if let Err(e) = self.send_ping().await {
                warn!("Failed to send ping: {}", e);
                
                // Mark as disconnected
                let mut connected = self.connected.write().await;
                *connected = false;
            }
            
            // Check for stale connection
            let last_activity = {
                let activity = self.last_ws_activity.read().await;
                *activity
            };
            
            if Instant::now().duration_since(last_activity) > Duration::from_secs(90) {
                warn!("WebSocket connection stale, reconnecting");
                
                // Mark as disconnected
                let mut connected = self.connected.write().await;
                *connected = false;
                
                // Close existing connection
                if let Err(e) = self.shutdown_websocket().await {
                    warn!("Error closing stale WebSocket: {:?}", e);
                }
                
                // Try to reconnect
                if let Err(e) = self.ensure_websocket_connection().await {
                    warn!("Failed to reconnect WebSocket: {}", e);
                }
            }
        }
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
