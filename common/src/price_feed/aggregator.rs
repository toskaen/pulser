use super::sources::PriceSource;
use crate::error::PulserError;
use crate::types::PriceInfo;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use log::{info, debug, warn};
use crate::price_feed::http_sources;
use crate::price_feed::PriceFeed;
use crate::price_feed::Arc;
use crate::price_feed::PriceFeedExtensions;

pub struct PriceAggregator {
    min_sources: usize,
    max_price_age_secs: u64,
}

impl PriceAggregator {
    pub fn new() -> Self {
        Self {
            min_sources: 2,
            max_price_age_secs: 60,
        }
    }
    
    pub fn with_min_sources(mut self, min_sources: usize) -> Self {
        self.min_sources = min_sources;
        self
    }
    
    pub fn with_max_price_age(mut self, max_age_secs: u64) -> Self {
        self.max_price_age_secs = max_age_secs;
        self
    }
    
    pub fn calculate_vwap(&self, sources: &HashMap<String, Result<PriceSource, PulserError>>) -> Result<PriceInfo, PulserError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_secs();
            
        // Filter valid sources (successful and recent)
        let mut valid_sources: Vec<&PriceSource> = sources.values()
            .filter_map(|r| r.as_ref().ok())
            .filter(|s| now - s.timestamp <= self.max_price_age_secs)
            .filter(|s| s.price > 0.0) // Only filter clearly invalid prices
            .collect();
            
        if valid_sources.len() < self.min_sources {
            return Err(PulserError::PriceFeedError(format!(
                "Insufficient valid price sources: {}/{} required", 
                valid_sources.len(), 
                self.min_sources
            )));
        }
        
        // Filter outliers if we have enough sources
        if valid_sources.len() >= 3 {
            // Calculate median price
            let mut prices: Vec<f64> = valid_sources.iter().map(|s| s.price).collect();
            prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let median = if prices.len() % 2 == 0 {
                (prices[prices.len() / 2 - 1] + prices[prices.len() / 2]) / 2.0
            } else {
                prices[prices.len() / 2]
            };
            
            // Filter out prices that deviate more than 15% from median
            let outlier_threshold = 0.15; // 15% deviation from median
            let pre_filter_count = valid_sources.len();
            valid_sources.retain(|s| {
                let deviation = (s.price - median).abs() / median;
                if deviation > outlier_threshold {
                    log::warn!("Outlier detected from {}: ${:.2} (median: ${:.2}, deviation: {:.2}%)", 
                               s.name, s.price, median, deviation * 100.0);
                    false
                } else {
                    true
                }
            });
            
            if valid_sources.len() < pre_filter_count {
                log::info!("Removed {} outlier price(s)", pre_filter_count - valid_sources.len());
            }
            
            // Make sure we still have enough sources after filtering outliers
            if valid_sources.len() < self.min_sources {
                return Err(PulserError::PriceFeedError(format!(
                    "Insufficient valid price sources after outlier removal: {}/{} required", 
                    valid_sources.len(), 
                    self.min_sources
                )));
            }
        }
        
        // Calculate volume-weighted average price
        let mut total_volume = 0.0;
        let mut weighted_sum = 0.0;
        
        for source in &valid_sources {
            total_volume += source.volume * source.weight;
            weighted_sum += source.price * source.volume * source.weight;
        }
        
        if total_volume <= 0.0 {
            return Err(PulserError::PriceFeedError("Zero volume reported by all sources".to_string()));
        }
        
        let vwap = weighted_sum / total_volume;
        
        // Create price feeds map for info
        let mut price_feeds = HashMap::new();
        for source in valid_sources {
            price_feeds.insert(source.name.clone(), source.price);
        }
        
        Ok(PriceInfo {
            raw_btc_usd: vwap,
            timestamp: now as i64,
            price_feeds,
        })
    }
    
    // Calculate the spread between spot VWAP and futures
    pub fn calculate_basis(&self, vwap: f64, futures_price: f64) -> f64 {
        if vwap <= 0.0 || futures_price <= 0.0 {
            return 0.0;
        }
        
        // Return as percentage (positive = futures premium, negative = backwardation)
        ((futures_price / vwap) - 1.0) * 100.0
    }
}

// In common/src/price_feed/aggregator.rs

pub async fn determine_best_price(
    source_manager: &SourceManager,
    price_feed: &Arc<PriceFeed>,
    user_id: &str,
) -> Result<(f64, String, f64), PulserError> {
    // Get prices from all available sources with retries
    let prices = source_manager.fetch_all_prices().await;
    
    // Get health information for each source
    let healthy_sources = source_manager.get_healthy_sources();
    
    // Log what sources we're using
    info!("Price sources for user {}: {}", user_id, 
         healthy_sources.iter()
            .map(|(name, score)| format!("{}={:.2}", name, score))
            .collect::<Vec<_>>()
            .join(", "));
    
    // Select valid sources with good health
    let mut valid_sources = Vec::new();
    for (name, result) in &prices {
        if let Ok(source) = result {
            // Only include sources with decent health and valid price
            if let Some((_, score)) = healthy_sources.iter().find(|(n, _)| n == name) {
                if *score > 0.5 && source.price > 1000.0 {
                    valid_sources.push((source.clone(), *score));
                }
            }
        }
    }
    
    if valid_sources.is_empty() {
        // Emergency fallback to HTTP API calls
        warn!("No valid price sources available for user {}, using emergency fallback", user_id);
        let fallback_price = http_sources::fetch_btc_usd_price(&price_feed.client).await?;
        return Ok((fallback_price, "HTTP_Fallback".to_string(), 0.0));
    }
    
    // Calculate weighted average price based on health scores
    let total_weight: f64 = valid_sources.iter().map(|(_, score)| score).sum();
    let weighted_price: f64 = valid_sources.iter()
        .map(|(source, score)| source.price * score)
        .sum::<f64>() / total_weight;
    
    // Debug log individual prices
    debug!("Price inputs for user {}: {}", user_id,
          valid_sources.iter()
            .map(|(source, score)| format!("{}=${:.2} (weight={:.2})", source.name, source.price, score))
            .collect::<Vec<_>>()
            .join(", "));
    
    // Get spot and futures prices for comparison
    let spot_price = weighted_price;
    let futures_price = match price_feed.get_deribit_price().await {
        Ok(price) => price,
        Err(e) => {
            warn!("Failed to get Deribit futures price: {}, using spot price", e);
            spot_price
        }
    };
    
    // Calculate basis
    let basis = ((futures_price / spot_price) - 1.0) * 100.0;
    
    // Select higher of spot and futures (maximizes user value)
    let (selected_price, source_desc) = if spot_price >= futures_price {
        (spot_price, format!("VWAP (backwardation {:.2}%)", basis))
    } else {
        (futures_price, format!("Deribit (contango {:.2}%)", basis))
    };
    
    info!("Selected price for user {}: ${:.2} using {}", user_id, selected_price, source_desc);
    Ok((selected_price, source_desc, basis))
}
