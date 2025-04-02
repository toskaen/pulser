use crate::error::PulserError;
use crate::types::PriceInfo;
use log::{debug, info, trace, warn, error};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;

use super::{PriceFeed, PriceHistory, DEFAULT_TIMEOUT_SECS, FALLBACK_RETRY_ATTEMPTS, PRICE_CACHE};
use super::cache::{save_price_history, load_price_history};
use crate::utils::now_timestamp;

// Emergency alternative price sources
pub async fn emergency_fetch_price(client: &Client) -> Result<f64, PulserError> {
    let mut prices = Vec::new();
    let mut sources = Vec::new();
    
    // Try Bitfinex
    match fetch_bitfinex_price(client).await {
        Ok(price) => {
            if price > 0.0 { 
                info!("Fetched Bitfinex price: ${:.2}", price);
                prices.push(price);
                sources.push("Bitfinex");
            } else {
                warn!("Ignoring invalid Bitfinex price: ${:.2}", price);
            }
        },
        Err(e) => warn!("Failed to fetch from Bitfinex: {}", e)
    }
    
    // Try Binance
    match fetch_binance_price(client).await {
        Ok(price) => {
            if price > 0.0 { 
                info!("Fetched Binance price: ${:.2}", price);
                prices.push(price);
                sources.push("Binance");
            } else {
                warn!("Ignoring invalid Binance price: ${:.2}", price);
            }
        },
        Err(e) => warn!("Failed to fetch from Binance: {}", e)
    }
    
    // Try Kraken
    match fetch_kraken_price(client).await {
        Ok(price) => {
            if price > 0.0 { 
                info!("Fetched Kraken price: ${:.2}", price);
                prices.push(price);
                sources.push("Kraken");
            } else {
                warn!("Ignoring invalid Kraken price: ${:.2}", price);
            }
        },
        Err(e) => warn!("Failed to fetch from Kraken: {}", e)
    }
    
    if prices.is_empty() {
        return Err(PulserError::PriceFeedError("All emergency price sources failed".to_string()));
    }
    
    // Sort and get median
    prices.sort_by(|a: &f64, b: &f64| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = if prices.len() % 2 == 0 {
        (prices[prices.len() / 2 - 1] + prices[prices.len() / 2]) / 2.0
    } else {
        prices[prices.len() / 2]
    };
    
    info!("Calculated emergency median price: ${:.2} from sources: {}", median, sources.join(", "));
    Ok(median)
}

async fn fetch_bitfinex_price(client: &Client) -> Result<f64, PulserError> {
    match timeout(
        Duration::from_secs(5),
        client.get("https://api-pub.bitfinex.com/v2/ticker/tBTCUSD").send()
    ).await {
        Ok(Ok(response)) => {
            if !response.status().is_success() {
                return Err(PulserError::ApiError(format!("Bitfinex API error: {}", response.status())));
            }
            
            let json: Value = response.json().await?;
            let price = json.as_array()
                .ok_or_else(|| PulserError::ApiError("Invalid Bitfinex response".to_string()))?[6]
                .as_f64()
                .ok_or_else(|| PulserError::ApiError("Failed to parse Bitfinex price".to_string()))?;
            
            Ok(price)
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Bitfinex request failed: {}", e))),
        Err(_) => Err(PulserError::NetworkError("Bitfinex request timed out".to_string())),
    }
}

async fn fetch_binance_price(client: &Client) -> Result<f64, PulserError> {
    match timeout(
        Duration::from_secs(5), 
        client.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT").send()
    ).await {
        Ok(Ok(response)) => {
            if !response.status().is_success() {
                return Err(PulserError::ApiError(format!("Binance API error: {}", response.status())));
            }
            
            let json: Value = response.json().await?;
            let price = json["price"].as_str()
                .ok_or_else(|| PulserError::ApiError("Invalid Binance response".to_string()))?
                .parse::<f64>()
                .map_err(|_| PulserError::ApiError("Failed to parse Binance price".to_string()))?;
            
            Ok(price)
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Binance request failed: {}", e))),
        Err(_) => Err(PulserError::NetworkError("Binance request timed out".to_string())),
    }
}

async fn fetch_kraken_price(client: &Client) -> Result<f64, PulserError> {
    match timeout(
        Duration::from_secs(5), 
        client.get("https://api.kraken.com/0/public/Ticker?pair=XBTUSD").send()
    ).await {
        Ok(Ok(response)) => {
            if !response.status().is_success() {
                return Err(PulserError::ApiError(format!("Kraken API error: {}", response.status())));
            }
            
            let json: Value = response.json().await?;
            let price = json["result"]["XXBTZUSD"]["c"][0].as_str()
                .ok_or_else(|| PulserError::ApiError("Invalid Kraken response".to_string()))?
                .parse::<f64>()
                .map_err(|_| PulserError::ApiError("Failed to parse Kraken price".to_string()))?;
            
            Ok(price)
        }
        Ok(Err(e)) => Err(PulserError::NetworkError(format!("Kraken request failed: {}", e))),
        Err(_) => Err(PulserError::NetworkError("Kraken request timed out".to_string())),
    }
}

// Main function for fetching price from multiple sources
pub async fn fetch_btc_usd_price(client: &Client, price_feed: &PriceFeed) -> Result<PriceInfo, PulserError> {
    let now = now_timestamp();
    const STALE_THRESHOLD_SECS: i64 = 600; // 10 minutes, matches PRICE_UPDATE_INTERVAL_SECS

    // Try Deribit price first
    match price_feed.get_deribit_price().await {
        Ok(price) if price > 0.0 => {
            let timestamp = price_feed.get_last_update_timestamp().await;
            
            // Check staleness
            let time_diff = now - timestamp;
            if time_diff <= STALE_THRESHOLD_SECS {
                trace!("Using live Deribit price: ${:.2}", price);
                let mut price_feeds = HashMap::new();
                price_feeds.insert("Deribit".to_string(), price);
                let price_info = PriceInfo {
                    raw_btc_usd: price,
                    timestamp,
                    price_feeds,
                };
                // Update cache
                {
                    let mut cache = PRICE_CACHE.write().await;
                    *cache = (price, now);
                }
                // Spawn history save
                let history = vec![PriceHistory {
                    timestamp: now as u64,
                    btc_usd: price,
                    source: "Deribit".to_string(),
                }];
                tokio::spawn(async move {
                    if let Err(e) = save_price_history(history).await {
                        warn!("Failed to save price history: {}", e);
                    }
                });
                Ok(price_info)
            } else {
                warn!("Deribit price stale ({}s), checking median", time_diff);
                // Median fallback
                match load_price_history().await {
                    Ok(history) => {
                        if let Some(median) = history.iter()
                            .filter(|e| e.source == "Median")
                            .rev() // Newest first
                            .find(|e| (now - e.timestamp as i64) <= STALE_THRESHOLD_SECS) // Recent enough
                            .map(|e| e.btc_usd)
                        {
                            info!("Using median price from history: ${:.2}", median);
                            let mut price_feeds = HashMap::new();
                            price_feeds.insert("Median".to_string(), median);
                            let price_info = PriceInfo {
                                raw_btc_usd: median,
                                timestamp: now,
                                price_feeds,
                            };
                            // Update cache
                            {
                                let mut cache = PRICE_CACHE.write().await;
                                *cache = (median, now);
                            }
                            Ok(price_info)
                        } else {
                            warn!("No recent median price available, falling back to emergency fetch");
                            fetch_emergency_price(client, now).await
                        }
                    }
                    Err(e) => {
                        warn!("Failed to load price history for median: {}, using emergency fetch", e);
                        fetch_emergency_price(client, now).await
                    }
                }
            }
        }
        Ok(price) => {
            warn!("Invalid Deribit price: ${:.2}", price);
            // Fall back to emergency fetch if Deribit is invalid
            fetch_emergency_price(client, now).await
        }
        Err(e) => {
            warn!("Deribit price fetch failed: {}", e);
            // Fall back to median, then emergency fetch
            match load_price_history().await {
                Ok(history) => {
                    if let Some(median) = history.iter()
                        .filter(|e| e.source == "Median")
                        .rev()
                        .find(|e| (now - e.timestamp as i64) <= STALE_THRESHOLD_SECS)
                        .map(|e| e.btc_usd)
                    {
                        info!("Using median price from history after Deribit failure: ${:.2}", median);
                        let mut price_feeds = HashMap::new();
                        price_feeds.insert("Median".to_string(), median);
                        let price_info = PriceInfo {
                            raw_btc_usd: median,
                            timestamp: now,
                            price_feeds,
                        };
                        // Update cache
                        {
                            let mut cache = PRICE_CACHE.write().await;
                            *cache = (median, now);
                        }
                        Ok(price_info)
                    } else {
                        warn!("No recent median price available, falling back to emergency fetch");
                        fetch_emergency_price(client, now).await
                    }
                }
                Err(e2) => {
                    warn!("Failed to load price history for median: {}, using emergency fetch", e2);
                    fetch_emergency_price(client, now).await
                }
            }
        }
    }
}

// Helper function for emergency fallback
async fn fetch_emergency_price(client: &Client, now: i64) -> Result<PriceInfo, PulserError> {
    match emergency_fetch_price(client).await {
        Ok(price) => {
            info!("Using emergency fallback price: ${:.2}", price);
            {
                let mut cache = PRICE_CACHE.write().await;
                *cache = (price, now);
            }
            let mut price_feeds = HashMap::new();
            price_feeds.insert("Emergency".to_string(), price);
            let price_info = PriceInfo {
                raw_btc_usd: price,
                timestamp: now,
                price_feeds,
            };
            let history = vec![PriceHistory {
                timestamp: now as u64,
                btc_usd: price,
                source: "Emergency".to_string(),
            }];
            tokio::spawn(async move {
                if let Err(e) = save_price_history(history).await {
                    warn!("Failed to save price history: {}", e);
                }
            });
            Ok(price_info)
        }
        Err(e) => {
            error!("Emergency price fetch failed: {}", e);
            Err(PulserError::PriceFeedError("All price sources failed".to_string()))
        }
    }
}
