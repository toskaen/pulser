// Modified emergency_fetch_price function for the preferred sources
pub async fn emergency_fetch_price(client: &Client) -> Result<PriceInfo, PulserError> {
    let mut prices = Vec::new();
    let mut sources = Vec::new();
    
    // Try Kraken first (highest priority)
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
    
    // Try Bitfinex second (medium priority)
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
    
    // Try Binance third (lowest priority)
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
    
    // Build price feeds map
    let mut price_feeds = HashMap::new();
    for (idx, source) in sources.iter().enumerate() {
        price_feeds.insert(source.to_string(), prices[idx]);
    }
    
    info!("Calculated emergency median price: ${:.2} from sources: {}", median, sources.join(", "));
    
    Ok(PriceInfo {
        raw_btc_usd: median,
        timestamp: crate::utils::now_timestamp(),
        price_feeds,
    })
}

// Fetch price from Kraken
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

// Fetch price from Bitfinex
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

// Fetch price from Binance
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
