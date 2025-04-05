// common/src/websocket/mod.rs
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, broadcast, Mutex};
use std::collections::HashMap;
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream, MaybeTlsStream};
use crate::PulserError;
use log::{info, warn, error, debug};
use tokio::time::{interval, sleep, timeout};
use serde_json::Value;
use futures_util::StreamExt;
use futures::SinkExt;

pub type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Clone)]
pub struct WebSocketConfig {
    pub ping_interval_secs: u64,
    pub timeout_secs: u64,
    pub max_reconnect_attempts: u32,
    pub reconnect_base_delay_secs: u64,
}

#[derive(Debug, Clone)]
pub struct ConnectionStats {
    pub last_message_time: Option<Instant>,
    pub messages_received: usize,
    pub messages_sent: usize,
    pub errors: usize,
}


pub struct WebSocketConnection {
    pub stream: Arc<Mutex<WsStream>>,
    pub connected: Arc<RwLock<bool>>,
    pub last_activity: Arc<RwLock<Instant>>,
    pub last_processed: Arc<RwLock<Instant>>, // For throttling
    pub throttle_ms: u64, // Throttle interval per connection
}

pub struct WebSocketManager {
    connections: RwLock<HashMap<String, Arc<WebSocketConnection>>>,
    config: WebSocketConfig,
}

impl WebSocketManager {
    pub fn new(config: WebSocketConfig) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            config,
        }
    }

    pub fn get_exchange_url(exchange: &str) -> &'static str {
        match exchange.to_lowercase().as_str() {
            "deribit" => "wss://test.deribit.com/ws/api/v2", // Use testnet for MVP
            "kraken" => "wss://futures.kraken.com/ws/v1",
            "binance" => "wss://stream.binance.com:9443/ws/btcusdt@ticker",
            "bitfinex" => "wss://api-pub.bitfinex.com/ws/2",
            _ => "wss://test.deribit.com/ws/api/v2", // Default to Deribit testnet
        }
    }

    pub async fn get_connection(&self, endpoint: &str) -> Result<Arc<WebSocketConnection>, PulserError> {
        let mut connections = self.connections.write().await;

        if let Some(conn) = connections.get(endpoint) {
            let is_connected = *conn.connected.read().await;
            if is_connected {
                debug!("Reusing existing WebSocket connection to {}", endpoint);
                return Ok(conn.clone());
            }
            connections.remove(endpoint);
        }

        info!("Creating new WebSocket connection to {}", endpoint);
        let (ws_stream, _) = timeout(Duration::from_secs(10), connect_async(endpoint)).await
            .map_err(|_| PulserError::NetworkError("WebSocket connection timeout".to_string()))?
            .map_err(|e| PulserError::NetworkError(format!("Failed to connect to {}: {}", endpoint, e)))?;
        let conn = Arc::new(WebSocketConnection {
            stream: Arc::new(Mutex::new(ws_stream)),
            connected: Arc::new(RwLock::new(true)),
            last_activity: Arc::new(RwLock::new(Instant::now())),
            last_processed: Arc::new(RwLock::new(Instant::now())),
            throttle_ms: 0, // Default to no throttling
        });
        connections.insert(endpoint.to_string(), conn.clone());
        Ok(conn)
    }

pub async fn subscribe(&self, endpoint: &str, subscription: &str, throttle_ms: u64) -> Result<Arc<WebSocketConnection>, PulserError> {
    let conn = self.get_connection(endpoint).await?;
    
    // Send subscription
    {
        let mut stream = conn.stream.lock().await;
        stream.send(Message::Text(subscription.to_string())).await
            .map_err(|e| PulserError::NetworkError(format!("Failed to send subscription to {}: {}", endpoint, e)))?;
    }
    
    // Instead of modifying the Arc, create a new connection with the throttle
    let new_conn = Arc::new(WebSocketConnection {
        stream: conn.stream.clone(),
        connected: conn.connected.clone(),
        last_activity: conn.last_activity.clone(),
        last_processed: conn.last_processed.clone(),
        throttle_ms,  // Set throttle for the new connection
    });
    
    // Replace the existing connection in the map
    let mut connections = self.connections.write().await;
    connections.insert(endpoint.to_string(), new_conn.clone());
    
    info!("Subscribed to {} with throttle {}ms", endpoint, throttle_ms);
    Ok(new_conn)
}

    pub async fn start_monitoring(&self, mut shutdown: broadcast::Receiver<()>) {
        let mut interval = interval(Duration::from_secs(self.config.ping_interval_secs));
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Shutting down WebSocketManager");
                    let mut connections = self.connections.write().await;
                    for (endpoint, conn) in connections.drain() {
                        let mut stream = conn.stream.lock().await;
                        if let Err(e) = stream.close(None).await {
                            warn!("Error closing connection to {}: {}", endpoint, e);
                        }
                        let mut connected = conn.connected.write().await;
                        *connected = false;
                    }
                    break;
                }
                _ = interval.tick() => {
                    // Handle ping and connection checks
                    let endpoints_to_check = {
                        let connections = self.connections.read().await;
                        connections.keys().cloned().collect::<Vec<String>>()
                    };
                    
                    for endpoint in endpoints_to_check {
                        let should_reconnect = {
                            let connections = self.connections.read().await;
                            if let Some(conn) = connections.get(&endpoint) {
                                let mut should_reconnect = false;
                                // First check if connected
                                let is_connected = *conn.connected.read().await;
                                if !is_connected {
                                    should_reconnect = true;
                                } else {
                                    // Then try to ping
                                    let mut stream = conn.stream.lock().await;
                                    if let Err(e) = stream.send(Message::Ping(vec![1, 2, 3])).await {
                                        warn!("Ping failed for {}: {}", endpoint, e);
                                        let mut connected = conn.connected.write().await;
                                        *connected = false;
                                        should_reconnect = true;
                                    } else {
                                        let mut last_activity = conn.last_activity.write().await;
                                        *last_activity = Instant::now();
                                    }
                                    
                                    // Also check activity timeout
                                    let last_activity = *conn.last_activity.read().await;
                                    if Instant::now().duration_since(last_activity).as_secs() > self.config.timeout_secs * 2 {
                                        warn!("Connection to {} stale, marking as disconnected", endpoint);
                                        let mut connected = conn.connected.write().await;
                                        *connected = false;
                                        should_reconnect = true;
                                    }
                                }
                                should_reconnect
                            } else {
                                false
                            }
                        };
                        
                        if should_reconnect {
                            // Remove old connection
                            {
                                let mut connections = self.connections.write().await;
                                connections.remove(&endpoint);
                            }
                            
                            // Try to reconnect
                            match self.get_connection(&endpoint).await {
                                Ok(_) => info!("Successfully reconnected to {}", endpoint),
                                Err(e) => error!("Failed to reconnect to {}: {}", endpoint, e)
                            }
                        }
                    }
                }
            }
        }
    }

    pub async fn process_messages<F, Fut>(
        &self, 
        endpoint: &str, 
        mut handler: F
    ) -> Result<(), PulserError>
    where 
        F: FnMut(Value) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), PulserError>> + Send + 'static
    {
        let conn = self.get_connection(endpoint).await?;
        let stream_mutex = conn.stream.clone();
        let last_processed = conn.last_processed.clone();
        let throttle_ms = conn.throttle_ms;
        let endpoint_str = endpoint.to_string();
        
        tokio::spawn(async move {
            let mut stream = stream_mutex.lock().await;
            
            while let Some(msg_result) = stream.next().await {
                match msg_result {
                    Ok(Message::Text(text)) => {
                        // Check throttling
                        let now = Instant::now();
                        let should_process = {
                            let mut last = last_processed.write().await;
                            if throttle_ms == 0 || now.duration_since(*last).as_millis() >= throttle_ms as u128 {
                                *last = now;
                                true
                            } else {
                                false
                            }
                        };
                        
                        if should_process {
                            if let Ok(json) = serde_json::from_str::<Value>(&text) {
                                // Drop the lock before calling handler to avoid deadlocks
                                drop(stream);
                                
                                if let Err(e) = handler(json).await {
                                    warn!("Message handler error for {}: {}", endpoint_str, e);
                                }
                                
                                // Re-acquire the lock
                                stream = stream_mutex.lock().await;
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        if let Err(e) = stream.send(Message::Pong(data)).await {
                            warn!("Failed to send pong for {}: {}", endpoint_str, e);
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => {
                        info!("WebSocket connection closed for {}", endpoint_str);
                        break;
                    }
                    Err(e) => {
                        warn!("WebSocket error for {}: {}", endpoint_str, e);
                        break;
                    }
                    _ => {} // Ignore other message types
                }
            }
            
            // Mark as disconnected when the loop exits
            let conn_connected = conn.connected.clone();
            let mut connected = conn_connected.write().await;
            *connected = false;
            info!("WebSocket connection for {} marked as disconnected", endpoint_str);
        });
        
        Ok(())
    }
    
    pub async fn is_connected(&self, endpoint: &str) -> bool {
        let connections = self.connections.read().await;
        if let Some(conn) = connections.get(endpoint) {
            let connected = conn.connected.read().await;
            *connected
        } else {
            false
        }
    }

pub async fn get_connection_stats(&self, endpoint: &str) -> Option<ConnectionStats> {
        if self.is_connected(endpoint).await {
            // Return basic stats if available
            Some(ConnectionStats {
                last_message_time: Some(Instant::now()), // Default to now for initial implementation
                messages_received: 0,
                messages_sent: 0,
                errors: 0,
            })
        } else {
            None
        }
        }

    pub async fn shutdown_connection(&self, endpoint: &str) -> Option<Result<(), PulserError>> {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.remove(endpoint) {
            let mut stream = conn.stream.lock().await;
            let result = stream.close(None).await
                .map_err(|e| PulserError::NetworkError(format!("Failed to close {}: {}", endpoint, e)));
            
            let mut connected = conn.connected.write().await;
            *connected = false;
            
            Some(result)
        } else {
            None
        }
    }
}
