use crate::error::PulserError;
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, trace, warn};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc, Mutex};
use tokio::time::{timeout, interval};

use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

// WebSocket handler for price feeds
#[derive(Clone)]
pub struct WebSocketHandler {
    url: String,
    connected: Arc<RwLock<bool>>,
    last_activity: Arc<RwLock<Instant>>,
}

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

impl WebSocketHandler {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            connected: Arc::new(RwLock::new(false)),
            last_activity: Arc::new(RwLock::new(Instant::now())),
        }
    }

    pub async fn connect(&self) -> Result<WsStream, PulserError> {
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
            }
            Ok(Err(e)) => Err(PulserError::NetworkError(format!(
                "WebSocket connection error: {}",
                e
            ))),
            Err(_) => Err(PulserError::NetworkError(
                "WebSocket connection timeout".to_string(),
            )),
        }
    }

    pub async fn send(
        &self,
        ws_stream: &Arc<Mutex<WsStream>>,
        message: Message,
    ) -> Result<(), PulserError> {
        let mut ws_guard = ws_stream.lock().await;
        match ws_guard.send(message).await {
            Ok(_) => {
                let mut last_activity = self.last_activity.write().await;
                *last_activity = Instant::now();
                Ok(())
            }
            Err(e) => {
                let mut connected = self.connected.write().await;
                *connected = false;
                Err(PulserError::NetworkError(format!(
                    "WebSocket send error: {}",
                    e
                )))
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
        _ => WebSocketHandler::new("wss://www.deribit.com/ws/api/v2"), // Default to Deribit
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

        (
            Self {
                handler,
                subscription: subscription.to_string(),
                message_tx,
                shutdown_tx,
            },
            message_rx,
        )
    }

    pub async fn start(&self) -> Result<(), PulserError> {
        let handler = self.handler.clone();
        let subscription = self.subscription.clone();
        let message_tx = self.message_tx.clone();
        let _shutdown_tx = self.shutdown_tx.clone(); // Mark as unused with underscore

        tokio::spawn(async move {
            // Connect to WebSocket
            let ws_stream = match handler.connect().await {
                Ok(stream) => stream,
                Err(e) => {
                    error!("WebSocket connection failed: {}", e);
                    return;
                }
            };

            // Create a shared WebSocket stream
            let ws_stream = Arc::new(Mutex::new(ws_stream));

            // Send subscription message
            if !subscription.is_empty() {
                let sub_msg = Message::Text(subscription.clone());
                if let Err(e) = handler.send(&ws_stream, sub_msg).await {
                    error!("Failed to send subscription: {}", e);
                    return;
                }
            }

            let (shutdown_sender, mut shutdown_receiver) = mpsc::channel(1);

            // Spawn heartbeat task
            tokio::spawn({
                let handler = handler.clone();
                let ws_stream = ws_stream.clone();
                let shutdown_sender = shutdown_sender.clone();

                async move {
                    let mut interval = interval(Duration::from_secs(30));

                    loop {
                        tokio::select! {
                            _ = interval.tick() => {
                                if !handler.is_connected().await {
                                    break;
                                }

                                // Send ping to keep connection alive
                                let ping_msg = Message::Ping(vec![1, 2, 3]);
                                match handler.send(&ws_stream, ping_msg).await {
                                    Ok(_) => {
                                        handler.update_activity().await;
                                    }
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
                    _ = shutdown_receiver.recv() => {
                        debug!("Shutting down WebSocket connection");
                        break;
                    }

                    msg_opt = async {
                        let mut ws_guard = ws_stream.lock().await;
                        ws_guard.next().await
                    } => {
                        match msg_opt {
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
                                            }
                                            Err(e) => {
                                                warn!("Failed to parse WebSocket message: {}", e);
                                            }
                                        }
                                    }
                                    Message::Ping(data) => {
                                        // No need to drop ws_guard here; send handles locking
                                        let pong_msg = Message::Pong(data);
                                        if let Err(e) = handler.send(&ws_stream, pong_msg).await {
                                            error!("Failed to send pong: {}", e);
                                            break;
                                        }
                                    }
                                    Message::Close(_) => {
                                        info!("WebSocket connection closed by server");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            Some(Err(e)) => {
                                error!("WebSocket error: {}", e);
                                break;
                            }
                            None => {
                                info!("WebSocket connection closed");
                                break;
                            }
                        }
                    }
                }
            }

            // Close the WebSocket connection on exit
            let mut ws_guard = ws_stream.lock().await;
            let _ = ws_guard.close(None).await;
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
