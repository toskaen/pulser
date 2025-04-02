
use crate::error::PulserError;
use crate::types::PriceInfo;
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use log::{debug, error, info, trace, warn};
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use super::{PriceHistory, DEFAULT_CACHE_DURATION_SECS, DEFAULT_TIMEOUT_SECS, PRICE_UPDATE_INTERVAL_MS, WS_CONNECTION_TIMEOUT_SECS, WS_PING_INTERVAL_SECS, PRICE_CACHE};
use super::cache::save_price_history;
use crate::utils::now_timestamp;
use super::http_sources::emergency_fetch_price;


#[derive(Debug, Clone)]
pub struct PriceFeed {
    pub latest_deribit_price: Arc<RwLock<f64>>,
    pub last_deribit_update: Arc<RwLock<i64>>,
    client: reqwest::Client,
    active_ws: Arc<tokio::sync::Mutex<Option<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>>>,
    last_ws_activity: Arc<RwLock<Instant>>,
    is_connecting: Arc<RwLock<bool>>,
    error_counts: Arc<RwLock<HashMap<String, u32>>>,
    connected: Arc<RwLock<bool>>,
    last_price_update: Arc<RwLock<Instant>>,
    price_buffer: Arc<Mutex<Vec<PriceHistory>>>,
}

impl PriceFeed {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
            
        PriceFeed {
            latest_deribit_price: Arc::new(RwLock::new(0.0)),
            last_deribit_update: Arc::new(RwLock::new(0)),
            client,
            active_ws: Arc::new(tokio::sync::Mutex::new(None)),
            last_ws_activity: Arc::new(RwLock::new(Instant::now())),
            is_connecting: Arc::new(RwLock::new(false)),
            error_counts: Arc::new(RwLock::new(HashMap::new())),
            connected: Arc::new(RwLock::new(false)),
            last_price_update: Arc::new(RwLock::new(Instant::now())),
            price_buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }
    
    pub async fn is_websocket_connected(&self) -> bool {
        *self.connected.read().await
    }

    pub async fn get_last_update_timestamp(&self) -> i64 {
        let last_update = self.last_deribit_update.read().await;
        *last_update
    }

    // Implementation of the start_deribit_feed method
    pub async fn start_deribit_feed(&self, shutdown_rx: &mut broadcast::Receiver<()>) -> Result<(), PulserError> {
        // Same implementation as in paste-2.txt...
        let config: toml::Value = match std::fs::read_to_string("config/service_config.toml") {
            Ok(content) => toml::from_str(&content)?,
            Err(e) => {
                warn!("Failed to read config file: {}", e);
                return Err(PulserError::ConfigError(format!("Failed to read config: {}", e)));
            }
        };
        
        let api_key = config.get("deribit_id").and_then(|v| v.as_str()).unwrap_or("your_deribit_id").to_string();
        let secret = config.get("deribit_secret").and_then(|v| v.as_str()).unwrap_or("your_deribit_secret").to_string();

        let mut attempts = 0u32;
        let mut ping_interval = tokio::time::interval(Duration::from_secs(WS_PING_INTERVAL_SECS));
        let max_backoff = 60;
        
        // Buffer for batching price updates
        let price_buffer = Arc::new(Mutex::new(Vec::<PriceHistory>::new()));
        let price_buffer_clone = price_buffer.clone();

        // Initial state
        {
            let mut connected = self.connected.write().await;
            *connected = false;
        }

        // Spawn a task to periodically flush the buffer
        tokio::spawn({
            let buffer = price_buffer_clone;
            let mut flush_interval = tokio::time::interval(Duration::from_secs(30)); // Flush every 30s
            async move {
                loop {
                    flush_interval.tick().await;
                    let entries = {
                        let mut buf = buffer.lock().unwrap();
                        if buf.is_empty() { continue; }
                        std::mem::take(&mut *buf)
                    };
                    trace!("Saved {} batched Deribit prices", entries.len()); // Moved before save
                    if let Err(e) = save_price_history(entries).await {
                        warn!("Failed to save batched Deribit price history: {}", e);
                    }
                }
            }
        });
        
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Deribit feed shutting down");
                let mut ws_guard = self.active_ws.lock().await;
                if let Some(mut ws) = ws_guard.take() {
                    info!("Closing Deribit WebSocket connection");
                    {
                        let mut connected = self.connected.write().await;
                        *connected = false;
                    }
                    match tokio::time::timeout(Duration::from_secs(5), ws.close(None)).await {
                        Ok(result) => { if let Err(e) = result { warn!("Error closing Deribit WebSocket: {}", e); } },
                        Err(_) => { warn!("Timeout closing Deribit WebSocket, abandoning connection"); }
                    }
                }
                // Flush remaining buffer on shutdown
                let remaining = {
                    let mut buf = price_buffer.lock().unwrap(); // Changed from .lock().await
                    std::mem::take(&mut *buf)
                };
                if !remaining.is_empty() {
                    if let Err(e) = save_price_history(remaining).await {
                        warn!("Failed to save final Deribit price history: {}", e);
                    }
                }
                break;
            }
            
            _ = ping_interval.tick() => {
                let is_connected = {
                    let connected = self.connected.read().await;
                    *connected
                };
                
                if is_connected {
                    let mut ws_guard = self.active_ws.lock().await;
                    if let Some(ws) = ws_guard.as_mut() {
                        match ws.send(Message::Ping(vec![1, 2, 3, 4])).await {
                            Ok(_) => { 
                                let mut last_activity = self.last_ws_activity.write().await;
                                *last_activity = Instant::now(); 
                                trace!("Sent WebSocket ping to Deribit");
                            },
                            Err(e) => {
                                warn!("Failed to send ping to Deribit: {}", e);
                                *ws_guard = None;
                                {
                                    let mut is_connecting = self.is_connecting.write().await;
                                    *is_connecting = false;
                                }
                                {
                                    let mut connected = self.connected.write().await;
                                    *connected = false;
                                }
                            }
                        }
                    }
                }
                
                let now = Instant::now();
                let last_activity = {
                    let last_ws = self.last_ws_activity.read().await;
                    *last_ws
                };
                
                if is_connected && now.duration_since(last_activity).as_secs() > WS_PING_INTERVAL_SECS * 2 {
                    warn!("Deribit WebSocket connection stale, closing connection");
                    let mut ws_guard = self.active_ws.lock().await;
                    if let Some(mut ws) = ws_guard.take() {
                        let _ = ws.close(None).await;
                    }
                    {
                        let mut connected = self.connected.write().await;
                        *connected = false;
                    }
                    {
                        let mut is_connecting = self.is_connecting.write().await;
                        *is_connecting = false;
                    }
                }
            }
            
            _ = async {
                let should_reconnect = {
                    let is_connecting = self.is_connecting.read().await;
                    let connected = self.connected.read().await;
                    let has_active_ws = self.active_ws.lock().await.is_some();
                    !*is_connecting && !*connected && !has_active_ws
                };
                
                if should_reconnect {
                    {
                        let mut is_connecting = self.is_connecting.write().await;
                        *is_connecting = true;
                    }
                    
                    let backoff = std::cmp::min(2u64.pow(attempts), max_backoff);
                    let jitter = rand::random::<u64>() % 1000;
                    let backoff_ms = backoff * 1000 + jitter;
                    
                    if attempts > 0 {
                        info!("Waiting {}ms before reconnecting to Deribit", backoff_ms);
                        sleep(Duration::from_millis(backoff_ms)).await;
                    }
                    
                    info!("Connecting to Deribit (attempt {})", attempts + 1);
                    match self.connect_deribit(&api_key, &secret).await {
                        Ok(_) => {
                            info!("Successfully connected to Deribit");
                            attempts = 0;
                            {
                                let mut connected = self.connected.write().await;
                                *connected = true;
                            }
                            let buffer_clone = price_buffer.clone();
                            let ws_clone = self.active_ws.clone();
                            let price_clone = self.latest_deribit_price.clone();
                            let update_clone = self.last_deribit_update.clone();
                            let activity_clone = self.last_ws_activity.clone();
                            let last_update_clone = self.last_price_update.clone();
                            let connected_clone = self.connected.clone();

                            tokio::spawn(async move {
                                let mut ws_lock = ws_clone.lock().await;
                                if let Some(mut ws) = ws_lock.take() {
                                    while let Some(msg_result) = ws.next().await {
                                        match msg_result {
                                            Ok(Message::Text(text)) => {
                                                if let Ok(data) = serde_json::from_str::<Value>(&text) {
                                                    if let Some(price) = data.get("params")
                                                        .and_then(|p| p.get("data"))
                                                        .and_then(|d| d.get("last_price"))
                                                        .and_then(|p| p.as_f64())
                                                    {
                                                        let now = Instant::now();
                                                        let last_update = {
                                                            let last = last_update_clone.read().await;
                                                            *last
                                                        };
                                                        if now.duration_since(last_update).as_millis() >= PRICE_UPDATE_INTERVAL_MS as u128 {
                                                            {
                                                                let mut price_guard = price_clone.write().await;
                                                                *price_guard = price;
                                                            }
                                                            {
                                                                let mut time_guard = update_clone.write().await;
                                                                *time_guard = now_timestamp();
                                                            }
                                                            {
                                                                let mut update_guard = last_update_clone.write().await;
                                                                *update_guard = now;
                                                            }
                                                            {
                                                                let mut buffer = buffer_clone.lock().unwrap(); // Changed from .lock().await
                                                                buffer.push(PriceHistory {
                                                                    timestamp: now_timestamp() as u64,
                                                                    btc_usd: price,
                                                                    source: "Deribit".to_string(),
                                                                });
                                                            }
                                                            trace!("Deribit BTC/USD: ${:.2} (throttled)", price);
                                                        }
                                                    }
                                                }
                                            }
                                            Ok(Message::Ping(data)) => {
                                                if let Err(e) = ws.send(Message::Pong(data)).await {
                                                    warn!("Failed to send pong: {}", e);
                                                    break;
                                                }
                                                let mut last_activity = activity_clone.write().await;
                                                *last_activity = Instant::now();
                                            }
                                            Ok(Message::Close(_)) => {
                                                debug!("WebSocket close frame received");
                                                break;
                                            }
                                            Err(e) => {
                                                warn!("WebSocket read error: {}", e);
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                    let mut connected = connected_clone.write().await;
                                    *connected = false;
                                    *ws_lock = None;
                                }
                            });
                        },
                        Err(e) => {
                            warn!("Deribit WebSocket connection failed: {}", e);
                            let mut ws_guard = self.active_ws.lock().await;
                            if let Some(mut ws) = ws_guard.take() {
                                let _ = ws.close(None).await;
                            }
                            attempts = attempts.saturating_add(1);
                            {
                                let mut connected = self.connected.write().await;
                                *connected = false;
                            }
                            if attempts >= 10 {
                                error!("Max reconnection attempts reached for Deribit");
                                sleep(Duration::from_secs(60)).await;
                                attempts = 5;
                            }
                        }
                    }
                    
                    {
                        let mut is_connecting = self.is_connecting.write().await;
                        *is_connecting = false;
                    }
                }
                
                sleep(Duration::from_millis(1000)).await;
            } => {}
        }
    }
    Ok(())
}
    // Continue updating all methods to use async RwLock operations
    async fn connect_deribit(&self, api_key: &str, secret: &str) -> Result<(), PulserError> {
        debug!("Attempting to connect to Deribit WebSocket");
        
        // Use timeout for connection
        let connect_result = match timeout(
            Duration::from_secs(WS_CONNECTION_TIMEOUT_SECS),
            connect_async("wss://test.deribit.com/ws/api/v2")
        ).await {
            Ok(Ok((ws_conn, _))) => {
                // Successfully connected to WebSocket
                debug!("WebSocket connection established");
                Ok(ws_conn)
            },
            Ok(Err(e)) => {
                {
                    let mut connected = self.connected.write().await;
                    *connected = false;
                }
                Err(PulserError::NetworkError(format!("WebSocket connection error: {}", e)))
            },
            Err(_) => {
                {
                    let mut connected = self.connected.write().await;
                    *connected = false;
                }
                Err(PulserError::NetworkError("WebSocket connection timeout".to_string()))
            },
        }?;
        
        let mut ws_conn = connect_result;
        
        // Reset last activity timer
        {
            let mut last_activity = self.last_ws_activity.write().await;
            *last_activity = Instant::now();
        }
        
        // Send auth message
        let auth_msg = json!({"jsonrpc": "2.0", "id": 1, "method": "public/auth", "params": {"grant_type": "client_credentials", "client_id": api_key, "client_secret": secret}});
        if let Err(e) = ws_conn.send(Message::Text(auth_msg.to_string())).await {
            // Auth message failed, close connection
            ws_conn.close(None).await.ok();
            {
                let mut connected = self.connected.write().await;
                *connected = false;
            }
            return Err(PulserError::ApiError(format!("Failed to send auth message: {}", e)));
        }
        
        // Wait for auth response with timeout
        let token_msg = match timeout(Duration::from_secs(10), ws_conn.next()).await {
            Ok(Some(Ok(msg))) => msg,
            Ok(Some(Err(e))) => {
                {
                    let mut connected = self.connected.write().await;
                    *connected = false;
                }
                return Err(PulserError::ApiError(format!("WebSocket error: {}", e)));
            },
            Ok(None) => {
                {
                    let mut connected = self.connected.write().await;
                    *connected = false;
                }
                return Err(PulserError::ApiError("WebSocket closed".to_string()));
            },
            Err(_) => {
                return Err(PulserError::ApiError("Auth response timeout".to_string()));
            }
        };
        
        // Update activity time
        {
            let mut last_activity = self.last_ws_activity.write().await;
            *last_activity = Instant::now();
        }
        
        // Parse auth response
        let token_text = match token_msg.into_text() {
            Ok(text) => text,
            Err(e) => {
                return Err(PulserError::ApiError(format!("Failed to parse auth response: {}", e)));
            }
        };
        
        let token_json: Value = match serde_json::from_str(&token_text) {
            Ok(json) => json,
            Err(e) => {
                return Err(PulserError::ApiError(format!("Invalid auth response JSON: {}", e)));
            }
        };
        
        trace!("Deribit auth response: {:?}", token_json);
        
        // Verify we got an access token
        let _access_token = match token_json["result"]["access_token"].as_str() {
            Some(token) => token.to_string(),
            None => {
                return Err(PulserError::ApiError("Auth failed: no access token".to_string()));
            }
        };

        // Subscribe to ticker
        let sub_msg = json!({"jsonrpc": "2.0", "id": 2, "method": "public/subscribe", "params": {"channels": ["ticker.BTC-PERPETUAL.raw"]}});
        if let Err(e) = ws_conn.send(Message::Text(sub_msg.to_string())).await {
            return Err(PulserError::ApiError(format!("Failed to send subscribe message: {}", e)));
        }
        
        info!("Subscribed to Deribit ticker.BTC-PERPETUAL.raw");
        
        // Store the connection
        {
            let mut ws_guard = self.active_ws.lock().await;
            *ws_guard = Some(ws_conn);
        }
        
        // Start a separate task to process messages
        let active_ws_clone = self.active_ws.clone();
        let latest_price_clone = self.latest_deribit_price.clone();
        let last_update_clone = self.last_deribit_update.clone();
        let last_activity_clone = self.last_ws_activity.clone();
        let last_price_update_clone = self.last_price_update.clone();
        let connected_clone = self.connected.clone();

        // Create a bounded channel with limited buffer to avoid memory issues
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(10); // Smaller buffer (10 instead of 100)

        // Spawn a task to read from WebSocket and send to the channel
        let ws_clone = self.active_ws.clone();
        let connected_reader_clone = self.connected.clone();
        
        tokio::spawn(async move {
            let mut ws_lock = ws_clone.lock().await;
            if let Some(mut ws) = ws_lock.take() {
                while let Some(msg_result) = ws.next().await {
                    match msg_result {
                        Ok(msg) => {
                            // Only forward price updates and control messages, drop other messages
                            match &msg {
                                Message::Text(text) => {
                                    if text.contains("ticker.BTC-PERPETUAL.raw") {
                                        // For price messages, only send if buffer isn't full
                                        if tx.try_send(msg).is_err() {
                                            trace!("Deribit WSS buffer full, dropping price message");
                                        }
                                    }
                                },
                                Message::Ping(_) | Message::Close(_) => {
                                    // Always send control messages
                                    if tx.send(msg).await.is_err() {
                                        break;
                                    }
                                },
                                _ => { /* Drop other messages */ }
                            }
                        },
                        Err(e) => {
                            warn!("WebSocket read error: {}", e);
                            {
                                let mut connected = connected_reader_clone.write().await;
                                *connected = false;
                            }
                            break;
                        }
                    }
                }
                debug!("Deribit WebSocket reader task ended");
            }
            // WebSocket is implicitly closed when ws goes out of scope
        });

        // Spawn the processing task using the channel receiver
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                // Update last activity time
                {
                    let mut last_activity = last_activity_clone.write().await;
                    *last_activity = Instant::now();
                }

                match msg {
                    Message::Text(text) => {
                        if let Ok(data) = serde_json::from_str::<Value>(&text) {
                            if let Some(price) = data.get("params")
                                .and_then(|p| p.get("data"))
                                .and_then(|d| d.get("last_price"))
                                .and_then(|p| p.as_f64())
                            {
                                let now = Instant::now();
                                let last_update = {
                                    let last_update = last_price_update_clone.read().await;
                                    *last_update
                                };
                                
                                // Rate limit price updates to avoid too frequent updates
                                if now.duration_since(last_update).as_millis() >= PRICE_UPDATE_INTERVAL_MS as u128 {
                                    // Only update price and timestamp if enough time has passed
                                    {
                                        let mut price_guard = latest_price_clone.write().await;
                                        *price_guard = price;
                                    }
                                    {
                                        let mut time_guard = last_update_clone.write().await;
                                        *time_guard = now_timestamp();
                                    }
                                    {
                                        let mut update_guard = last_price_update_clone.write().await;
                                        *update_guard = now;
                                    }
                                    trace!("Deribit BTC/USD: ${:.2} (throttled)", price);
                                    
                                    tokio::spawn(async move {
    let history = vec![PriceHistory {
        timestamp: now_timestamp() as u64,
        btc_usd: price,
        source: "Deribit".to_string(),
    }];
    if let Err(e) = save_price_history(history).await {
        warn!("Failed to save Deribit price history: {}", e);
    }
});
                                }
                            }
                        }
                    }
                    Message::Ping(data) => {
                        // Respond to ping with pong
                        let mut ws_lock = active_ws_clone.lock().await;
                        if let Some(ws) = ws_lock.as_mut() {
                            if let Err(e) = ws.send(Message::Pong(data)).await {
                                warn!("Failed to send pong: {}", e);
                                {
                                    let mut connected = connected_clone.write().await;
                                    *connected = false;
                                }
                                break;
                            }
                        } else {
                            {
                                let mut connected = connected_clone.write().await;
                                *connected = false;
                            }
                            break; // WebSocket gone
                        }
                    }
                    Message::Close(_) => {
                        debug!("WebSocket close frame received");
                        {
                            let mut connected = connected_clone.write().await;
                            *connected = false;
                        }
                        break;
                    }
                    _ => {}
                }
            }
            
            // Clear the WebSocket connection when the task ends
            let mut ws_lock = active_ws_clone.lock().await;
            *ws_lock = None;
            {
                let mut connected = connected_clone.write().await;
                *connected = false;
            }
            debug!("Deribit price feed processor task ended");
        });
            
        // Set connected state
        {
            let mut connected = self.connected.write().await;
            *connected = true;
        }
        
        Ok(())
    }

pub async fn get_deribit_price(&self) -> Result<f64, PulserError> {
    let price = {
        let price_guard = self.latest_deribit_price.read().await;
        *price_guard
    };
    let last = {
        let last_guard = self.last_deribit_update.read().await;
        *last_guard
    };
    let now = crate::utils::now_timestamp();
    
    // Calculate time difference properly with absolute value to avoid huge negative numbers
    let time_diff = if now > last { 
        now - last 
    } else { 
        // This should never happen with proper initialization, but handle it gracefully
        warn!("Clock skew detected: now ({}) earlier than last_update ({})", now, last);
        last - now 
    };
    
    // If we have a recent Deribit price, use it
    if price > 0.0 && time_diff < 60_i64 {
        trace!("Using cached Deribit price: ${:.2}", price);
        return Ok(price);
    }

    // If Deribit price is stale, try to use cached median
    let (cached_median, cached_time) = {
        let cache = super::PRICE_CACHE.read().await;
        (*cache).clone()
    };
    
    // Calculate time difference for median cache
    let cache_time_diff = if now > cached_time { 
        now - cached_time 
    } else { 
        warn!("Clock skew detected in PRICE_CACHE: now ({}) earlier than cached_time ({})", now, cached_time);
        cached_time - now 
    };
    
    if cached_median > 0.0 && cache_time_diff < super::DEFAULT_CACHE_DURATION_SECS as i64 {
        info!("Deribit price stale ({}s), using median cache: ${:.2}", time_diff, cached_median);
        return Ok(cached_median);
    }

    // If both Deribit and median cache are stale, try a direct fetch from Deribit
    info!("Deribit price stale ({}s) and no fresh median, fetching from Deribit", time_diff);
        match self.fetch_deribit_price().await {
            Ok(fresh_price) => {
                if fresh_price > 0.0 {
                    // Update the cached Deribit price
                    {
                        let mut price_guard = self.latest_deribit_price.write().await;
                        *price_guard = fresh_price;
                    }
                    {
                        let mut time_guard = self.last_deribit_update.write().await;
                        *time_guard = now;
                    }
                    info!("Updated Deribit price: ${:.2}", fresh_price);
                    Ok(fresh_price)
                } else {
                    warn!("Invalid price from Deribit direct fetch: ${:.2}", fresh_price);
                    if cached_median > 0.0 {
                        warn!("Falling back to stale median: ${:.2}", cached_median);
                        Ok(cached_median)
                    } else {
                        return Err(PulserError::PriceFeedError("No valid price available".to_string()));
                    }
                }
            }
            Err(e) => {
                if cached_median > 0.0 {
                    warn!("Failed to fetch Deribit price: {}, falling back to stale median: ${:.2}", e, cached_median);
                    Ok(cached_median)
                } else {
                    // No prices available, try fallback sources directly
                    warn!("No valid prices available from primary sources, trying emergency fallback");
                    match emergency_fetch_price(&self.client).await {
                        Ok(emergency_price) => {
                            info!("Using emergency fallback price: ${:.2}", emergency_price);
                            Ok(emergency_price)
                        }
                        Err(e2) => {
                            error!("Emergency price fetch failed after all primary sources failed: {}, original error: {}", e2, e);
                            return Err(PulserError::PriceFeedError("All price sources failed including emergency sources".to_string()));
                        }
                    }
                }
            }
        }
    }
    
    pub async fn shutdown_websocket(&self) -> Option<()> {
        // First update connection state safely
        {
            let mut connected = self.connected.write().await;
            *connected = false;
        }
        
        // Then handle the WebSocket connection
        let mut ws_guard = self.active_ws.lock().await;
        if let Some(mut ws) = ws_guard.take() {
            info!("Shutting down Deribit WebSocket");
            match ws.close(None).await {
                Ok(()) => {
                    info!("WebSocket closed successfully");
                }
                Err(e) => warn!("Error closing WebSocket: {}", e),
            }
            
            // Also update connecting state flag
            {
                let mut is_connecting = self.is_connecting.write().await;
                *is_connecting = false;
            }
            
            Some(())
        } else {
            debug!("No active WebSocket to shut down");
            None
        }
    }
    
    // Add the fetch_deribit_price method
    async fn fetch_deribit_price(&self) -> Result<f64, PulserError> {
        let url = "https://test.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL";
        
        match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), self.client.get(url).send()).await {
            Ok(Ok(response)) => {
                if !response.status().is_success() {
                    return Err(PulserError::ApiError(format!("Deribit API error: {}", response.status())));
                }
                
                match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), response.json::<Value>()).await {
                    Ok(Ok(json)) => {
                        match json["result"]["last_price"].as_f64() {
                            Some(price) if price > 0.0 => Ok(price),
                            Some(price) => Err(PulserError::PriceFeedError(format!("Invalid Deribit price: ${:.2}", price))),
                            None => Err(PulserError::PriceFeedError("Missing price in Deribit response".to_string())),
                        }
                    }
                    Ok(Err(e)) => Err(PulserError::ApiError(format!("Failed to parse Deribit response: {}", e))),
                    Err(_) => Err(PulserError::ApiError("Timeout parsing Deribit response".to_string())),
                }
            }
            Ok(Err(e)) => Err(PulserError::NetworkError(format!("Deribit request failed: {}", e))),
            Err(_) => Err(PulserError::NetworkError("Deribit request timed out".to_string())),
        }
    }

// In common/src/deribit.rs, add to PriceFeed impl
pub async fn get_option_price(&self, strike: f64, option_type: &str) -> Result<f64, PulserError> {
    let instrument = format!("BTC-{}-PUT", strike.round() as i64); // Adjust for Deribit format
    let url = format!("https://test.deribit.com/api/v2/public/ticker?instrument_name={}", instrument);
    
    match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), self.client.get(&url).send()).await {
        Ok(Ok(response)) => {
            if !response.status().is_success() {
                return Err(PulserError::ApiError(format!("Deribit API error: {}", response.status())));
            }
            match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), response.json::<Value>()).await {
                Ok(Ok(json)) => {
                    json["result"]["last_price"].as_f64()
                        .filter(|&price| price > 0.0)
                        .ok_or_else(|| PulserError::PriceFeedError("Missing or invalid option price".to_string()))
                }
                Ok(Err(e)) => Err(PulserError::ApiError(format!("Failed to parse Deribit option response: {}", e))),
                Err(_) => Err(PulserError::ApiError("Timeout parsing Deribit option response".to_string())),
            }
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Deribit option request failed: {}", e))),
        Err(_) => Err(PulserError::NetworkError("Deribit option request timed out".to_string())),
    }
}

// Add to PriceFeed in common/src/deribit.rs
pub async fn get_combo_price(&self, leg1: &str, leg2: &str) -> Result<f64, PulserError> {
    let instrument = format!("{}_{}", leg1, leg2);
    let url = format!("https://test.deribit.com/api/v2/public/ticker?instrument_name={}", instrument);
   match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), self.client.get(&url).send()).await {
        Ok(Ok(response)) => {
            if !response.status().is_success() {
                return Err(PulserError::ApiError(format!("Deribit API error: {}", response.status())));
            }
            match timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS), response.json::<Value>()).await {
                Ok(Ok(json)) => {
                    json["result"]["last_price"].as_f64()
                        .filter(|&price| price > 0.0)
                        .ok_or_else(|| PulserError::PriceFeedError("Missing or invalid option price".to_string()))
                }
                Ok(Err(e)) => Err(PulserError::ApiError(format!("Failed to parse Deribit option response: {}", e))),
                Err(_) => Err(PulserError::ApiError("Timeout parsing Deribit option response".to_string())),
            }
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Deribit option request failed: {}", e))),
        Err(_) => Err(PulserError::NetworkError("Deribit option request timed out".to_string())),
    }
}

}
