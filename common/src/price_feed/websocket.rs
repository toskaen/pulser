use crate::error::PulserError;
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, trace, warn};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc};
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message};

// WebSocket handler for price feeds
#[derive(Clone)]
pub struct WebSocketHandler {
    url: String,
    connected: Arc<RwLock<bool>>,
    last_activity: Arc<RwLock<Instant>>,
}

impl WebSocketHandler {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            connected: Arc::new(RwLock::new(false)),
            last_activity: Arc::new(RwLock::new(Instant::now())),
        }
    }
    
    pub async fn connect(&self) -> Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, PulserError> {
        debug!("Connecting to WebSocket at {}", self.url);
        
        match timeout(Duration::from_secs(10), connect_async(&self.url)).await {
            Ok(Ok((ws_stream, _))) => {
                info!("Connected to WebSocket at {}", self.url);
                {
                    let mut connected = self.connected.write().await;
                    *connected = true;
                }
                {
                    let mut last_activity = self.last_activity.write().await;
                    *last_activity = Instant::now();
                }
                Ok(ws_stream)
            },
            Ok(Err(e)) => {
                Err(PulserError::NetworkError(format!("WebSocket connection error: {}", e)))
            },
            Err(_) => {
                Err(PulserError::NetworkError("WebSocket connection timeout".to_string()))
            }
        }
    }
    
    pub async fn send(&self, ws_stream: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, message: Message) -> Result<(), PulserError> {
        match ws_stream.send(message).await {
            Ok(_) => {
                let mut last_activity = self.last_activity.write().await;
                *last_activity = Instant::now();
                Ok(())
            },
            Err(e) => {
                let mut connected = self.connected.write().await;
                *connected = false;
                Err(PulserError::NetworkError(format!("WebSocket send error: {}", e)))
            }
        }
    }
    
    pub async fn is_connected(&self) -> bool {
        *self.connected.read().await
    }
    
    pub async fn disconnect(&self) {
        let mut connected = self.connected.write().await;
        *connected = false;
    }
    
    pub async fn update_activity(&self) {
        let mut last_activity = self.last_activity.write().await;
        *last_activity = Instant::now();
    }
    
    pub async fn get_last_activity(&self) -> Instant {
        *self.last_activity.read().await
    }
}

// Factory function to create exchange-specific WebSocket handlers
pub fn create_exchange_websocket(exchange: &str) -> WebSocketHandler {
    match exchange.to_lowercase().as_str() {
        "deribit" => WebSocketHandler::new("wss://www.deribit.com/ws/api/v2"),
        "binance" => WebSocketHandler::new("wss://stream.binance.com:9443/ws/btcusdt@ticker"),
        "bitfinex" => WebSocketHandler::new("wss://api-pub.bitfinex.com/ws/2"),
        "kraken" => WebSocketHandler::new("wss://ws.kraken.com"),
        _ => WebSocketHandler::new("wss://www.deribit.com/ws/api/v2") // Default to Deribit
    }
}

// WebSocket client with subscription handling
pub struct SubscribedWebSocket {
    handler: WebSocketHandler,
    subscription: String,
    message_tx: mpsc::Sender<Value>,
    shutdown_tx: mpsc::Sender<()>,
}

impl SubscribedWebSocket {
    pub fn new(exchange: &str, subscription: &str) -> (Self, mpsc::Receiver<Value>) {
        let handler = create_exchange_websocket(exchange);
        let (message_tx, message_rx) = mpsc::channel(100);
        let (shutdown_tx, _) = mpsc::channel(1);
        
        (Self {
            handler,
            subscription: subscription.to_string(),
            message_tx,
            shutdown_tx,
        }, message_rx)
    }
    
    pub async fn start(&self) -> Result<(), PulserError> {
        let handler = self.handler.clone();
        let subscription = self.subscription.clone();
        let message_tx = self.message_tx.clone();
        let shutdown_tx = self.shutdown_tx.clone();
        
        tokio::spawn(async move {
            // Connect to WebSocket
            let mut ws_stream = match handler.connect().await {
                Ok(stream) => stream,
                Err(e) => {
                    error!("WebSocket connection failed: {}", e);
                    return;
                }
            };
            
            // Send subscription message
            if !subscription.is_empty() {
                if let Err(e) = ws_stream.send(Message::Text(subscription.clone())).await {
                    error!("Failed to send subscription: {}", e);
                    return;
                }
            }
            
            let (shutdown_sender, mut shutdown_receiver) = mpsc::channel(1);
            
            // Spawn heartbeat task
            tokio::spawn({
                let handler = handler.clone();
                let mut ws_stream_clone = ws_stream.clone();
                let shutdown_sender = shutdown_sender.clone();
                
                async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(30));
                    
                    loop {
                        tokio::select! {
                            _ = interval.tick() => {
                                if !handler.is_connected().await {
                                    break;
                                }
                                
                                // Send ping to keep connection alive
                                match ws_stream_clone.send(Message::Ping(vec![1, 2, 3])).await {
                                    Ok(_) => {
                                        handler.update_activity().await;
                                    },
                                    Err(e) => {
                                        error!("Failed to send WebSocket ping: {}", e);
                                        let _ = shutdown_sender.send(()).await;
                                        break;
                                    }
                                }
                                
                                // Check if connection is stale
                                let last_activity = handler.get_last_activity().await;
                                if Instant::now().duration_since(last_activity).as_secs() > 60 {
                                    error!("WebSocket connection stale, reconnecting");
                                    let _ = shutdown_sender.send(()).await;
                                    break;
                                }
                            }
                        }
                    }
                }
            });
            
            // Process messages
            loop {
                tokio::select! {
                    msg = ws_stream.next() => {
                        match msg {
                            Some(Ok(msg)) => {
                                handler.update_activity().await;
                                
                                match msg {
                                    Message::Text(text) => {
                                        match serde_json::from_str::<Value>(&text) {
                                            Ok(json) => {
                                                // Send message to receiver
                                                if let Err(e) = message_tx.send(json).await {
                                                    warn!("Failed to send message to receiver: {}", e);
                                                }
                                            },
                                            Err(e) => {
                                                warn!("Failed to parse WebSocket message: {}", e);
                                            }
                                        }
                                    },
                                    Message::Ping(data) => {
                                        if let Err(e) = ws_stream.send(Message::Pong(data)).await {
                                            error!("Failed to send pong: {}", e);
                                            break;
                                        }
                                    },
                                    Message::Close(_) => {
                                        info!("WebSocket connection closed by server");
                                        break;
                                    },
                                    _ => {
                                        // Ignore other message types
                                    }
                                }
                            },
                            Some(Err(e)) => {
                                error!("WebSocket error: {}", e);
                                break;
                            },
                            None => {
                                info!("WebSocket connection closed");
                                break;
                            }
                        }
                    },
                    _ = shutdown_receiver.recv() => {
                        debug!("Shutting down WebSocket connection");
                        break;
                    }
                }
            }
            
            // Cleanup
            let _ = ws_stream.close(None).await;
            handler.disconnect().await;
        });
        
        Ok(())
    }
    
    pub async fn stop(&self) -> Result<(), PulserError> {
        if let Err(e) = self.shutdown_tx.send(()).await {
            warn!("Failed to send shutdown signal: {}", e);
        }
        Ok(())
    }
}
