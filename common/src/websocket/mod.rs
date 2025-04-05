use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, broadcast, Mutex};
use std::collections::HashMap;
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream, MaybeTlsStream};
use crate::PulserError;
use log::{info, warn, error};
use tokio::time::{interval, sleep, timeout};
use serde_json::Value;

pub type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Clone)]
pub struct WebSocketConfig {
    pub ping_interval_secs: u64,
    pub timeout_secs: u64,
    pub max_reconnect_attempts: u32,
    pub reconnect_base_delay_secs: u64,
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
            if *conn.connected.read().await {
                info!("Reusing existing WebSocket connection to {}", endpoint);
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
        {
            let mut stream = conn.stream.lock().await;
            stream.send(Message::Text(subscription.to_string())).await
                .map_err(|e| PulserError::NetworkError(format!("Failed to send subscription to {}: {}", endpoint, e)))?;
        }
        conn.throttle_ms = throttle_ms; // Set throttle for this connection
        info!("Subscribed to {} with throttle {}ms", endpoint, throttle_ms);
        Ok(conn)
    }

    pub async fn start_monitoring(&self, mut shutdown: broadcast::Receiver<()>) {
        let mut interval = interval(Duration::from_secs(self.config.ping_interval_secs));
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Shutting down WebSocketManager");
                    let mut connections = self.connections.write().await;
                    for (_, conn) in connections.drain() {
                        let mut stream = conn.stream.lock().await;
                        stream.close(None).await.ok();
                        *conn.connected.write().await = false;
                    }
                    break;
                }
                _ = interval.tick() => {
                    let connections = self.connections.read().await;
                    for (endpoint, conn) in connections.iter() {
                        let mut stream = conn.stream.lock().await;
                        if let Err(e) = stream.send(Message::Ping(vec![1, 2, 3])).await {
                            warn!("Ping failed for {}: {}", endpoint, e);
                            *conn.connected.write().await = false;
                            continue;
                        }
                        *conn.last_activity.write().await = Instant::now();

                        let last_activity = *conn.last_activity.read().await;
                        if Instant::now().duration_since(last_activity).as_secs() > self.config.timeout_secs * 2 {
                            warn!("Connection to {} stale, marking as disconnected", endpoint);
                            *conn.connected.write().await = false;
                        }
                    }

                    let mut connections = self.connections.write().await;
let endpoints: Vec<String> = {
    let connections = self.connections.read().await;
    connections.iter()
        .filter_map(|(endpoint, conn)| async move {
            if !*conn.connected.read().await {
                Some(endpoint.clone())
            } else {
                None
            }
        }.await)
        .collect()
};
                    for endpoint in endpoints {
                        connections.remove(&endpoint);
                        if let Err(e) = self.get_connection(&endpoint).await {
                            error!("Failed to reconnect to {}: {}", endpoint, e);
                        }
                    }
                }
            }
        }
    }

    pub async fn process_messages<F>(&self, endpoint: &str, mut handler: F) -> Result<(), PulserError>
    where F: FnMut(Value) -> Result<(), PulserError> + Send + 'static {
        let conn = self.get_connection(endpoint).await?;
        let stream = conn.stream.clone();
        tokio::spawn(async move {
            let mut stream = stream.lock().await;
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        let now = Instant::now();
                        let mut last_processed = conn.last_processed.write().await;
                        if conn.throttle_ms == 0 || now.duration_since(*last_processed).as_millis() >= conn.throttle_ms as u128 {
                            if let Ok(json) = serde_json::from_str(&text) {
                                if let Err(e) = handler(json) {
                                    warn!("Message handler error for {}: {}", endpoint, e);
                                }
                            }
                            *last_processed = now;
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        stream.send(Message::Pong(data)).await.ok();
                        *conn.last_activity.write().await = Instant::now();
                    }
                    Ok(Message::Close(_)) => break,
                    Err(e) => {
                        warn!("WebSocket error for {}: {}", endpoint, e);
                        *conn.connected.write().await = false;
                        break;
                    }
                    _ => {}
                }
            }
        });
        Ok(())
    }
    
pub async fn is_connected(&self, endpoint: &str) -> bool {
    let connections = self.connections.read().await;
    connections.get(endpoint).map_or(false, |conn| *conn.connected.read().await)
}

    pub async fn shutdown_connection(&self, endpoint: &str) -> Option<Result<(), PulserError>> {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.remove(endpoint) {
            let mut stream = conn.stream.lock().await;
            stream.close(None).await
                .map_err(|e| PulserError::NetworkError(format!("Failed to close {}: {}", endpoint, e)))?;
            *conn.connected.write().await = false;
            Some(Ok(()))
        } else {
            None
        }
    }
}
