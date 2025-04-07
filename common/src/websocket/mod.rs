// common/src/websocket/mod.rs
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, broadcast, Mutex};
use std::collections::HashMap;
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream, MaybeTlsStream};
use crate::PulserError;
use log::{info, warn, error, debug};
use tokio::time::{interval, timeout};
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
    subscriptions: RwLock<HashMap<String, String>>, // Store subscriptions per endpoint
    config: WebSocketConfig,
}

impl WebSocketManager {
    pub fn new(config: WebSocketConfig) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            subscriptions: RwLock::new(HashMap::new()),
            config,
        }
    }

    pub fn get_exchange_url(exchange: &str) -> &'static str {
        match exchange.to_lowercase().as_str() {
            "deribit" => "wss://test.deribit.com/ws/api/v2",
            "kraken" => "wss://futures.kraken.com/ws/v1",
            "binance" => "wss://stream.binance.com:9443/ws/btcusdt@ticker",
            "bitfinex" => "wss://api-pub.bitfinex.com/ws/2",
            _ => "wss://test.deribit.com/ws/api/v2", // Default
        }
    }

    pub async fn get_connection(&self, endpoint: &str) -> Result<Arc<WebSocketConnection>, PulserError> {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get(endpoint) {
            if *conn.connected.read().await {
                debug!("Reusing existing WebSocket connection to {}", endpoint);
                return Ok(conn.clone());
            }
            connections.remove(endpoint);
        }

        info!("Creating new WebSocket connection to {}", endpoint);
        let (ws_stream, _) = timeout(Duration::from_secs(10), connect_async(endpoint))
            .await
            .map_err(|_| PulserError::NetworkError("WebSocket connection timeout".to_string()))?
            .map_err(|e| PulserError::NetworkError(format!("Failed to connect to {}: {}", endpoint, e)))?;
        let conn = Arc::new(WebSocketConnection {
            stream: Arc::new(Mutex::new(ws_stream)),
            connected: Arc::new(RwLock::new(true)),
            last_activity: Arc::new(RwLock::new(Instant::now())),
            last_processed: Arc::new(RwLock::new(Instant::now())),
            throttle_ms: 0,
        });
        connections.insert(endpoint.to_string(), conn.clone());
        Ok(conn)
    }

    pub async fn subscribe(&self, endpoint: &str, subscription: &str, throttle_ms: u64) -> Result<Arc<WebSocketConnection>, PulserError> {
        let conn = self.get_connection(endpoint).await?;
        let mut stream = conn.stream.lock().await;
        if !subscription.is_empty() { // Only send if subscription is provided
            stream.send(Message::Text(subscription.to_string())).await?;
            if let Some(Ok(Message::Text(text))) = timeout(Duration::from_secs(5), stream.next()).await? {
                debug!("Subscription response for {}: {}", endpoint, text);
            } else {
                warn!("No subscription response from {}", endpoint);
                return Err(PulserError::NetworkError("Subscription failed".into()));
            }
        }
        let new_conn = Arc::new(WebSocketConnection {
            stream: conn.stream.clone(),
            connected: conn.connected.clone(),
            last_activity: conn.last_activity.clone(),
            last_processed: conn.last_processed.clone(),
            throttle_ms,
        });
        let mut connections = self.connections.write().await;
        connections.insert(endpoint.to_string(), new_conn.clone());
        if !subscription.is_empty() {
            self.subscriptions.write().await.insert(endpoint.to_string(), subscription.to_string());
        }
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
                    let endpoints = self.connections.read().await.keys().cloned().collect::<Vec<_>>();
                    for endpoint in endpoints {
                        let should_reconnect = {
                            let connections = self.connections.read().await;
                            if let Some(conn) = connections.get(&endpoint) {
                                let is_connected = *conn.connected.read().await;
                                if !is_connected {
                                    true
                                } else {
                                    let mut stream = conn.stream.lock().await;
                                    if stream.send(Message::Ping(vec![1, 2, 3])).await.is_err() {
                                        warn!("Ping failed for {}", endpoint);
                                        let mut connected = conn.connected.write().await;
                                        *connected = false;
                                        true
                                    } else {
                                        let mut last_activity = conn.last_activity.write().await;
                                        *last_activity = Instant::now();
                                        let last = *conn.last_activity.read().await;
                                        if Instant::now().duration_since(last).as_secs() > self.config.timeout_secs * 2 {
                                            warn!("Connection to {} stale", endpoint);
                                            let mut connected = conn.connected.write().await;
                                            *connected = false;
                                            true
                                        } else {
                                            false
                                        }
                                    }
                                }
                            } else {
                                false
                            }
                        };
if should_reconnect {
    // Get a copy of the subscription string
    let subscription_str = {
        let subscriptions = self.subscriptions.read().await;
        subscriptions.get(&endpoint).cloned() // Clone the String
    };
    
    // Use the cloned subscription
    match self.reconnect(&endpoint, subscription_str.as_deref()).await {
        Ok(_) => info!("Reconnected to {}", endpoint),
        Err(e) => error!("Reconnection failed for {}: {}", endpoint, e),
    }
}
                    }
                }
            }
        }
    }
    
    // Add this method to your WebSocketManager implementation in websocket/mod.rs
pub async fn send_message(&self, endpoint: &str, message: &str) -> Result<(), PulserError> {
    let conn = self.get_connection(endpoint).await?;
    let mut stream = conn.stream.lock().await;
    stream.send(Message::Text(message.to_string())).await
        .map_err(|e| PulserError::NetworkError(format!("Failed to send message to {}: {}", endpoint, e)))
}

    pub async fn process_messages<F, Fut>(
        &self,
        endpoint: &str,
        mut handler: F,
    ) -> Result<(), PulserError>
    where
        F: FnMut(Value) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), PulserError>> + Send + 'static,
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
                                drop(stream);
                                if let Err(e) = handler(json).await {
                                    warn!("Handler error for {}: {}", endpoint_str, e);
                                }
                                stream = stream_mutex.lock().await;
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        if let Err(e) = stream.send(Message::Pong(data)).await {
                            warn!("Pong failed for {}: {}", endpoint_str, e);
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => {
                        info!("Connection closed for {}", endpoint_str);
                        break;
                    }
                    Err(e) => {
                        warn!("WebSocket error for {}: {}", endpoint_str, e);
                        break;
                    }
                    _ => {}
                }
            }
            let mut connected = conn.connected.write().await;
            *connected = false;
            info!("Connection for {} marked disconnected", endpoint_str);
        });
        Ok(())
    }

// Replace the line "is_connected" with the actual implementation:
pub async fn is_connected(&self, endpoint: &str) -> bool {
    let connections = self.connections.read().await;
    if let Some(conn) = connections.get(endpoint) {
        let connected = conn.connected.read().await;
        *connected
    } else {
        false
    }
}

// Replace the reconnect method with this simplified version
pub async fn reconnect(&self, endpoint: &str, subscription: Option<&str>) -> Result<Arc<WebSocketConnection>, PulserError> {
    debug!("Reconnecting to {}", endpoint);
    
    // Simple reconnection strategy
    let mut delay_ms = 100;
    let max_delay_ms = 30000; // 30 seconds
    let mut attempts = 0;
    let max_attempts = 5;
    
    while attempts < max_attempts {
        attempts += 1;
        
        // Try to connect
        match timeout(Duration::from_secs(10), connect_async(endpoint)).await {
            Ok(Ok((ws_stream, _))) => {
                let mut connections = self.connections.write().await;
                
                // Create new connection
                let conn = Arc::new(WebSocketConnection {
                    stream: Arc::new(Mutex::new(ws_stream)),
                    connected: Arc::new(RwLock::new(true)),
                    last_activity: Arc::new(RwLock::new(Instant::now())),
                    last_processed: Arc::new(RwLock::new(Instant::now())),
                    throttle_ms: 0,
                });
                
                // Handle subscription if needed
                if let Some(sub) = subscription {
                    // Get stream from the connection
                    let mut stream_guard = conn.stream.lock().await;
                    
                    // Send subscription
                    if let Err(e) = stream_guard.send(Message::Text(sub.to_string())).await {
                        warn!("Failed to send subscription to {}: {}", endpoint, e);
                        // Drop the guard and proceed to next attempt
                        drop(stream_guard);
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        delay_ms = (delay_ms * 2).min(max_delay_ms);
                        continue;
                    }
                    
                    // Wait for subscription response
                    match timeout(Duration::from_secs(5), stream_guard.next()).await {
                        Ok(Some(Ok(_))) => {
                            // Success - drop the guard first to avoid borrow issues
                            drop(stream_guard);
                            
                            // Update subscriptions
                            self.subscriptions.write().await.insert(endpoint.to_string(), sub.to_string());
                            
                            // Store the connection and return it
                            connections.insert(endpoint.to_string(), conn.clone());
                            return Ok(conn);
                        },
                        _ => {
                            // Failed to get proper response - try again
                            warn!("Failed to get subscription response from {}", endpoint);
                            // Drop the guard and proceed to next attempt
                            drop(stream_guard);
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            delay_ms = (delay_ms * 2).min(max_delay_ms);
                            continue;
                        }
                    }
                } else {
                    // No subscription needed - just store the connection and return
                    connections.insert(endpoint.to_string(), conn.clone());
                    return Ok(conn);
                }
            },
            _ => {
                warn!("Reconnection attempt {}/{} to {} failed, retrying in {}ms", 
                      attempts, max_attempts, endpoint, delay_ms);
            }
        }
        
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        delay_ms = (delay_ms * 2).min(max_delay_ms); // Exponential backoff with cap
    }
    
    Err(PulserError::NetworkError(format!("Failed to reconnect to {} after {} attempts", endpoint, max_attempts)))
}
    pub async fn get_connection_stats(&self, endpoint: &str) -> Option<ConnectionStats> {
        if self.is_connected(endpoint).await {
            Some(ConnectionStats {
                last_message_time: Some(Instant::now()),
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
            let result = stream
                .close(None)
                .await
                .map_err(|e| PulserError::NetworkError(format!("Failed to close {}: {}", endpoint, e)));
            let mut connected = conn.connected.write().await;
            *connected = false;
            Some(result)
        } else {
            None
        }
    }

pub async fn force_close_connection(&self, endpoint: &str) -> bool {
    let mut connections = self.connections.write().await;
    if let Some(conn) = connections.remove(endpoint) {
        // Just drop the connection without waiting for graceful close
        debug!("Forcefully closed connection to {}", endpoint);
        true
    } else {
        false
    }
}

pub async fn force_close_all_with_timeout(&self, timeout_duration: Duration) -> bool {
    let endpoints = {
        let connections = self.connections.read().await;
        connections.keys().cloned().collect::<Vec<_>>()
    };
    
    let mut all_closed = true;
    
    for endpoint in endpoints {
        match tokio::time::timeout(
            timeout_duration,
            self.shutdown_connection(&endpoint)
        ).await {
            Ok(Some(Ok(_))) => {
                debug!("Successfully closed connection to {}", endpoint);
            },
            _ => {
                warn!("Failed to gracefully close connection to {}, forcing close", endpoint);
                if !self.force_close_connection(&endpoint).await {
                    all_closed = false;
                }
            }
        }
    }
    
    all_closed
}

pub async fn shutdown_connection_with_timeout(&self, endpoint: &str, timeout: Duration) 
-> Option<Result<(), PulserError>> {
    match tokio::time::timeout(timeout, self.shutdown_connection(endpoint)).await {
        Ok(result) => result,
        Err(_) => {
            warn!("Timeout shutting down connection to {}", endpoint);
            // Force-close when timeout occurs
            if self.force_close_connection(endpoint).await {
                Some(Ok(()))
            } else {
                None
            }
        }
    }
}
    
  pub async fn force_close_all(&self) {
    let endpoints = {
        let connections = self.connections.read().await;
        connections.keys().cloned().collect::<Vec<_>>()
    };
    
    for endpoint in endpoints {
        if let Some(Err(e)) = self.shutdown_connection(&endpoint).await {
            warn!("Error force closing {}: {}", endpoint, e);
        }
    }
}
}
