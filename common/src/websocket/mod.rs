// common/src/websocket/mod.rs
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
use rand::Rng;

pub type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

// Configurable via environment variables
#[derive(Clone)]
pub struct WebSocketConfig {
    pub ping_interval_secs: u64,
    pub timeout_secs: u64,
    pub max_reconnect_attempts: u32,
    pub reconnect_base_delay_secs: u64,
    pub endpoints: HashMap<String, String>, // Exchange-specific endpoints
}

impl WebSocketConfig {
    pub fn from_env() -> Self {
        let mut endpoints = HashMap::new();
        endpoints.insert("deribit".to_string(), std::env::var("DERIBIT_WS_URL")
            .unwrap_or("wss://test.deribit.com/ws/api/v2".to_string()));
        endpoints.insert("kraken".to_string(), std::env::var("KRAKEN_WS_URL")
            .unwrap_or("wss://futures.kraken.com/ws/v1".to_string()));
        endpoints.insert("binance".to_string(), std::env::var("BINANCE_WS_URL")
            .unwrap_or("wss://stream.binance.com:9443/ws/btcusdt@ticker".to_string()));
        endpoints.insert("bitfinex".to_string(), std::env::var("BITFINEX_WS_URL")
            .unwrap_or("wss://api-pub.bitfinex.com/ws/2".to_string()));

        Self {
            ping_interval_secs: std::env::var("WS_PING_INTERVAL_SECS")
                .unwrap_or("30".to_string()).parse().unwrap_or(30),
            timeout_secs: std::env::var("WS_TIMEOUT_SECS")
                .unwrap_or("60".to_string()).parse().unwrap_or(60),
            max_reconnect_attempts: std::env::var("WS_MAX_RECONNECT_ATTEMPTS")
                .unwrap_or("10".to_string()).parse().unwrap_or(10),
            reconnect_base_delay_secs: std::env::var("WS_RECONNECT_DELAY_SECS")
                .unwrap_or("1".to_string()).parse().unwrap_or(1),
            endpoints,
        }
    }
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
    pub is_open: AtomicBool, // Added for robust state tracking
    pub last_activity: Arc<RwLock<Instant>>,
    pub last_processed: Arc<RwLock<Instant>>,
    pub throttle_ms: u64,
    stats: Arc<RwLock<ConnectionStats>>, // Added for monitoring
}

pub struct WebSocketManager {
    connections: RwLock<HashMap<String, Arc<WebSocketConnection>>>,
    subscriptions: RwLock<HashMap<String, String>>,
    config: WebSocketConfig,
    tasks: RwLock<Vec<tokio::task::JoinHandle<()>>>,
}

impl WebSocketManager {
    pub fn new(config: WebSocketConfig) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            subscriptions: RwLock::new(HashMap::new()),
            config,
            tasks: RwLock::new(Vec::new()),
        }
    }

    pub fn get_exchange_url(&self, exchange: &str) -> String {
        self.config.endpoints.get(&exchange.to_lowercase())
            .cloned()
            .unwrap_or_else(|| "wss://test.deribit.com/ws/api/v2".to_string())
    }

    pub async fn get_connection(&self, endpoint: &str) -> Result<Arc<WebSocketConnection>, PulserError> {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get(endpoint) {
            if conn.is_open.load(Ordering::SeqCst) {
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
            is_open: AtomicBool::new(true),
            last_activity: Arc::new(RwLock::new(Instant::now())),
            last_processed: Arc::new(RwLock::new(Instant::now())),
            throttle_ms: 0,
            stats: Arc::new(RwLock::new(ConnectionStats {
                last_message_time: None,
                messages_received: 0,
                messages_sent: 0,
                errors: 0,
            })),
        });
        connections.insert(endpoint.to_string(), conn.clone());
        Ok(conn)
    }

    pub async fn subscribe(&self, endpoint: &str, subscription: &str, throttle_ms: u64) -> Result<Arc<WebSocketConnection>, PulserError> {
        let conn = self.get_connection(endpoint).await?;
        let mut stream = conn.stream.lock().await;
        if !subscription.is_empty() {
            stream.send(Message::Text(subscription.to_string())).await
                .map_err(|e| {
                    warn!("Failed to send subscription to {}: {}", endpoint, e);
                    PulserError::NetworkError(format!("Subscription send failed: {}", e))
                })?;
            conn.stats.write().await.messages_sent += 1;
            if let Some(Ok(Message::Text(text))) = timeout(Duration::from_secs(5), stream.next()).await? {
                debug!("Subscription response for {}: {}", endpoint, text);
                conn.stats.write().await.messages_received += 1;
            } else {
                warn!("No subscription response from {}", endpoint);
                conn.stats.write().await.errors += 1;
                return Err(PulserError::NetworkError("Subscription failed".into()));
            }
        }
        let new_conn = Arc::new(WebSocketConnection {
            stream: conn.stream.clone(),
            is_open: AtomicBool::new(true),
            last_activity: conn.last_activity.clone(),
            last_processed: conn.last_processed.clone(),
            throttle_ms,
            stats: conn.stats.clone(),
        });
        let mut connections = self.connections.write().await;
        connections.insert(endpoint.to_string(), new_conn.clone());
        if !subscription.is_empty() {
            self.subscriptions.write().await.insert(endpoint.to_string(), subscription.to_string());
        }
        info!("Subscribed to {} with throttle {}ms", endpoint, throttle_ms);
        Ok(new_conn)
    }

pub async fn send_message(&self, endpoint: &str, message: &str) -> Result<(), PulserError> {
    // Check if connection exists and is open
    let conn = if self.is_connected(endpoint).await {
        self.get_connection(endpoint).await?
    } else {
        // Connection is closed, force reconnect
        let subscription = self.subscriptions.read().await.get(endpoint).cloned();
        self.reconnect(endpoint, subscription.as_deref()).await?
    };
    
    // Now we have a fresh connection, proceed with sending
    let mut stream = conn.stream.lock().await;
    stream.send(Message::Text(message.to_string())).await
        .map_err(|e| {
            conn.is_open.store(false, Ordering::SeqCst); // Mark as closed on error
            PulserError::NetworkError(format!("Failed to send message: {}", e))
        })?;
    conn.stats.write().await.messages_sent += 1;
    Ok(())
}
    pub async fn start_monitoring(&self, mut shutdown: broadcast::Receiver<()>) {
        let mut interval = interval(Duration::from_secs(self.config.ping_interval_secs));
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Shutting down WebSocketManager");
                    self.shutdown().await;
                    break;
                }
                _ = interval.tick() => {
                    let endpoints = self.connections.read().await.keys().cloned().collect::<Vec<_>>();
                    for endpoint in endpoints {
                        let should_reconnect = {
                            let connections = self.connections.read().await;
                            if let Some(conn) = connections.get(&endpoint) {
                                if !conn.is_open.load(Ordering::SeqCst) {
                                    true
                                } else {
                                    let mut stream = conn.stream.lock().await;
                                    if stream.send(Message::Ping(vec![1, 2, 3])).await.is_err() {
                                        warn!("Ping failed for {}", endpoint);
                                        conn.is_open.store(false, Ordering::SeqCst);
                                        conn.stats.write().await.errors += 1;
                                        true
                                    } else {
                                        conn.stats.write().await.messages_sent += 1;
                                        let mut last_activity = conn.last_activity.write().await;
                                        *last_activity = Instant::now();
                                        let last = *conn.last_activity.read().await;
                                        if Instant::now().duration_since(last).as_secs() > self.config.timeout_secs * 2 {
                                            warn!("Connection to {} stale", endpoint);
                                            conn.is_open.store(false, Ordering::SeqCst);
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
                            let subscription = self.subscriptions.read().await.get(&endpoint).cloned();
                            match self.reconnect(&endpoint, subscription.as_deref()).await {
                                Ok(_) => info!("Reconnected to {}", endpoint),
                                Err(e) => error!("Reconnection failed for {}: {}", endpoint, e),
                            }
                        }
                    }
                }
            }
        }
    }

    pub async fn process_messages<F, Fut>(&self, endpoint: &str, mut handler: F) -> Result<(), PulserError>
    where
        F: FnMut(Value) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), PulserError>> + Send + 'static,
    {
        let conn = self.get_connection(endpoint).await?;
        let stream_mutex = conn.stream.clone();
        let last_processed = conn.last_processed.clone();
        let throttle_ms = conn.throttle_ms;
        let stats = conn.stats.clone();
        let endpoint_str = endpoint.to_string();

        let task = tokio::spawn(async move {
            let mut stream = stream_mutex.lock().await;
            while conn.is_open.load(Ordering::SeqCst) {
                if let Some(msg_result) = stream.next().await {
                    match msg_result {
                        Ok(Message::Text(text)) => {
                            stats.write().await.messages_received += 1;
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
                                        stats.write().await.errors += 1;
                                    }
                                    stream = stream_mutex.lock().await;
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
    if let Err(e) = stream.send(Message::Pong(data)).await {
                                warn!("Pong failed for {}: {}", endpoint_str,e);
                                conn.is_open.store(false, Ordering::SeqCst);
                                stats.write().await.errors += 1;
                                break;
                            }
                            stats.write().await.messages_sent += 1;
                        }
                        Ok(Message::Close(frame)) => {
                            info!("Connection closed for {}: {:?}", endpoint_str, frame);
                            conn.is_open.store(false, Ordering::SeqCst);
                            break;
                        }
                        Err(e) => {
                            warn!("WebSocket error for {}: {}", endpoint_str, e);
                            conn.is_open.store(false, Ordering::SeqCst);
                            stats.write().await.errors += 1;
                            break;
                        }
                        _ => {}
                    }
                    stats.write().await.last_message_time = Some(Instant::now());
                } else {
                    break;
                }
            }
            info!("Connection for {} marked disconnected", endpoint_str);
        });
        self.tasks.write().await.push(task);
        Ok(())
    }

    pub async fn reconnect(&self, endpoint: &str, subscription: Option<&str>) -> Result<Arc<WebSocketConnection>, PulserError> {
        debug!("Reconnecting to {}", endpoint);
        let mut delay_ms = self.config.reconnect_base_delay_secs * 1000;
        let max_delay_ms = 30000;
        let mut attempts = 0;

        while attempts < self.config.max_reconnect_attempts {
            attempts += 1;
            match timeout(Duration::from_secs(10), connect_async(endpoint)).await {
                Ok(Ok((ws_stream, _))) => {
                    let mut connections = self.connections.write().await;
                    let conn = Arc::new(WebSocketConnection {
                        stream: Arc::new(Mutex::new(ws_stream)),
                        is_open: AtomicBool::new(true),
                        last_activity: Arc::new(RwLock::new(Instant::now())),
                        last_processed: Arc::new(RwLock::new(Instant::now())),
                        throttle_ms: 0,
                        stats: Arc::new(RwLock::new(ConnectionStats {
                            last_message_time: None,
                            messages_received: 0,
                            messages_sent: 0,
                            errors: 0,
                        })),
                    });
                    if let Some(sub) = subscription {
                        let mut stream = conn.stream.lock().await;
                        stream.send(Message::Text(sub.to_string())).await?;
                        conn.stats.write().await.messages_sent += 1;
                        if let Some(Ok(Message::Text(_))) = timeout(Duration::from_secs(5), stream.next()).await? {
                            self.subscriptions.write().await.insert(endpoint.to_string(), sub.to_string());
                        } else {
                            warn!("Subscription failed during reconnect to {}", endpoint);
                            continue;
                        }
                    }
                    connections.insert(endpoint.to_string(), conn.clone());
                    return Ok(conn);
                }
                _ => {
                    warn!("Reconnection attempt {}/{} to {} failed, retrying in {}ms", attempts, self.config.max_reconnect_attempts, endpoint, delay_ms);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms * 2).min(max_delay_ms);
                    let jitter = rand::thread_rng().gen_range(0..delay_ms / 2);
                    tokio::time::sleep(Duration::from_millis(jitter)).await;
                }
            }
        }
        Err(PulserError::NetworkError(format!("Failed to reconnect to {} after {} attempts", endpoint, self.config.max_reconnect_attempts)))
    }

    pub async fn shutdown(&self) {
        info!("Shutting down WebSocketManager");
        let mut connections = self.connections.write().await;
        for (endpoint, conn) in connections.drain() {
            if conn.is_open.load(Ordering::SeqCst) {
                let mut stream = conn.stream.lock().await;
                if let Err(e) = stream.close(None).await {
                    warn!("Error closing connection to {}: {}", endpoint, e);
                }
                conn.is_open.store(false, Ordering::SeqCst);
            }
        }
        let mut tasks = self.tasks.write().await;
        for task in tasks.drain(..) {
            if let Err(e) = task.await {
                warn!("Task failed during shutdown: {:?}", e);
            }
        }
    }

    pub async fn is_connected(&self, endpoint: &str) -> bool {
        let connections = self.connections.read().await;
        connections.get(endpoint).map_or(false, |conn| conn.is_open.load(Ordering::SeqCst))
    }

pub fn get_connection_stats(&self, endpoint: &str) -> Option<ConnectionStats> {
    let connections = self.connections.blocking_read();
    connections.get(endpoint).map(|conn| conn.stats.blocking_read().clone())
}
}
