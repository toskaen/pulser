// deposit-service/src/main.rs
use actix_web::{web, App, HttpServer, middleware, HttpResponse, ResponseError};
use bitcoin::Network;
use common::error::PulserError;
use common::types::PriceInfo;
use common::utils::now_timestamp;
use structopt::StructOpt;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};
use log::{info, warn, error, debug, trace};
use bdk_esplora::esplora_client;
use tokio::time::sleep;

mod types;
mod config;
mod wallet;
mod blockchain;
mod handlers;

use config::{Config, load_config, init_logging, ensure_directories};
use blockchain::create_esplora_client;
use wallet::DepositWallet;
use types::{StableChain, Utxo};

// Constants
const PRICE_UPDATE_INTERVAL_SECS: u64 = 120; // Harmonized with wallet.rs and deposit_monitor.rs
const WALLET_SYNC_INTERVAL_SECS: u64 = 30;
const RETRY_MAX: u32 = 3;
const DEFAULT_BTC_PRICE: f64 = 95000.0; // Current market price as of March 2025
const DEFAULT_SYNTHETIC_DISCOUNT: f64 = 0.99; // 1% discount

// Command-line arguments
#[derive(StructOpt, Debug)]
#[structopt(name = "deposit-service", about = "Pulser Deposit Service")]
struct Opt {
    #[structopt(short, long, default_value = "8081")]
    port: u16,
    
    #[structopt(long, default_value = "config/service_config.toml")]
    config: String,
    
    #[structopt(long, default_value = "data")]
    data_dir: String,
    
    #[structopt(long)]
    testnet: bool,
    
    #[structopt(long)]
    debug: bool,
    
    #[structopt(long, default_value = "user")]
    role: String,
}

// App state shared across handlers
pub struct AppState {
    pub config: Arc<RwLock<Config>>,
    pub wallets: Arc<RwLock<HashMap<String, (DepositWallet, StableChain)>>>,
    pub http_client: reqwest::Client,
    pub current_price: Arc<RwLock<f64>>,
    pub synthetic_price: Arc<RwLock<f64>>,
    pub price_last_updated: Arc<RwLock<i64>>,
    pub network: Network,
    pub role: String,
}

impl AppState {
    pub fn new(
        config: Config,
        network: Network,
    ) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(format!("Pulser/{}", config.version))
            .connect_timeout(Duration::from_secs(10))
            .pool_idle_timeout(Some(Duration::from_secs(30)))
            .build()
            .unwrap_or_else(|e| {
                warn!("Failed to create custom HTTP client: {}, using default", e);
                reqwest::Client::new()
            });
            
        Self {
            config: Arc::new(RwLock::new(config.clone())),
            wallets: Arc::new(RwLock::new(HashMap::new())),
            http_client,
            current_price: Arc::new(RwLock::new(DEFAULT_BTC_PRICE)),
            synthetic_price: Arc::new(RwLock::new(DEFAULT_BTC_PRICE * DEFAULT_SYNTHETIC_DISCOUNT)),
            price_last_updated: Arc::new(RwLock::new(now_timestamp())),
            network,
            role: config.role,
        }
    }
    
    pub fn update_prices(&self, price_info: &PriceInfo) {
        let synthetic = price_info.synthetic_price.unwrap_or(price_info.raw_btc_usd * DEFAULT_SYNTHETIC_DISCOUNT);
        
        // Update prices in AppState
        *self.current_price.write().unwrap() = price_info.raw_btc_usd;
        *self.synthetic_price.write().unwrap() = synthetic;
        *self.price_last_updated.write().unwrap() = now_timestamp();
        
        // Update wallet.rs shared price cache
        wallet::DepositWallet::update_shared_price_cache(price_info.raw_btc_usd, price_info.synthetic_price);
        
        debug!("Updated prices: BTC-USD ${:.2}, Synthetic ${:.2}", 
               price_info.raw_btc_usd, synthetic);
        
        // Update wallet states with new prices
        let mut wallets = self.wallets.write().unwrap();
        for (_, (_, chain)) in wallets.iter_mut() {
            chain.raw_btc_usd = price_info.raw_btc_usd;
            chain.synthetic_price = Some(synthetic);
            
            // Update USD value of accumulated BTC
            chain.stabilized_usd = common::types::USD(
                chain.accumulated_btc.to_btc() * price_info.raw_btc_usd
            );
        }
    }
    
    // Get current prices
    pub fn get_current_prices(&self) -> (f64, f64) {
        let raw_price = *self.current_price.read().unwrap();
        let synthetic = *self.synthetic_price.read().unwrap();
        (raw_price, synthetic)
    }
    
    // Check if price is stale
    pub fn is_price_stale(&self) -> bool {
        let last_updated = *self.price_last_updated.read().unwrap();
        let now = now_timestamp();
        (now - last_updated) > PRICE_UPDATE_INTERVAL_SECS as i64
    }
}

// Error handler for actix-web
impl ResponseError for PulserError {
    fn error_response(&self) -> HttpResponse {
        let status = self.status_code();
        let message = self.to_string();
        
        HttpResponse::build(status)
            .json(serde_json::json!({
                "error": message,
                "status": status.as_u16(),
                "success": false
            }))
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Parse command line options
    let opt = Opt::from_args();
    
    // Load configuration
    let mut config = match load_config::<Config>(&opt.config) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Failed to load config: {}", e);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));
        }
    };
    
    // Override config with command line arguments
    if opt.port != 0 {
        config.listening_port = opt.port;
    }
    if opt.testnet {
        config.network = "testnet".to_string();
    }
    if !opt.data_dir.is_empty() {
        config.data_dir = opt.data_dir;
    }
    if !opt.role.is_empty() {
        config.role = opt.role;
    }
    
    // Initialize logging
    init_logging(&config, opt.debug);
    
    // Ensure directories exist
    if let Err(e) = ensure_directories(&config) {
        error!("Failed to create directories: {}", e);
        return Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));
    }
    
    // Determine network
    let network = match config.network.as_str() {
        "testnet" => Network::Testnet,
        "regtest" => Network::Regtest,
        "signet" => Network::Signet,
        _ => Network::Bitcoin,
    };
    
    // Log startup information
    info!("Starting Pulser Deposit Service v{} in {} mode on {} network", 
          config.version, config.role, config.network);
    
    // Create application state
    let app_state = web::Data::new(AppState::new(config.clone(), network));
    
    // Perform initial price fetch
    info!("Performing initial price fetch...");
    let http_client = app_state.http_client.clone();
    match common::price_feed::fetch_btc_usd_price(&http_client).await {
        Ok(price_info) => {
            app_state.update_prices(&price_info);
            info!("Initial prices fetched: BTC-USD ${:.2}, Synthetic ${:.2}", 
                 price_info.raw_btc_usd, price_info.synthetic_price.unwrap_or(price_info.raw_btc_usd * DEFAULT_SYNTHETIC_DISCOUNT));
        },
        Err(e) => {
            warn!("Initial price fetch failed: {}. Using fallback values.", e);
            // Fallback values already set in AppState constructor
        }
    }
    
    // Load existing wallets
    info!("Loading existing wallets...");
    load_existing_wallets(&app_state).await?;
    
    // Start background tasks
    info!("Starting background tasks...");
    
    // Price update task
    let app_state_clone = app_state.clone();
    tokio::spawn(async move {
        price_update_task(app_state_clone).await;
    });
    
    // Wallet sync task
    let app_state_clone = app_state.clone();
    tokio::spawn(async move {
        wallet_sync_task(app_state_clone).await;
    });
    
    // Start HTTP server
    let server_addr = format!("{}:{}", config.listening_address, config.listening_port);
    info!("Starting HTTP server on {}", server_addr);
    
    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .wrap(middleware::Logger::default())
            .route("/deposit", web::post().to(handlers::create_deposit))
            .route("/deposit/{user_id}", web::get().to(handlers::get_deposit_status))
            .route("/deposit/{user_id}/utxos", web::get().to(handlers::get_deposit_utxos))
            .route("/status", web::get().to(handlers::get_service_status))
            .route("/health", web::get().to(|| async { "OK" }))
            .default_service(web::route().to(handlers::not_found))
    })
    .bind(&server_addr)?
    .workers(4)
    .run()
    .await
}

// Load existing wallets from the data directory
async fn load_existing_wallets(app_state: &web::Data<AppState>) -> std::io::Result<()> {
    // Get configuration parameters
    let config = app_state.config.read().unwrap();
    let data_dir = &config.data_dir;
    let secrets_dir = format!("{}/secrets", data_dir);
    
    // Check if secrets directory exists
    let secrets_path = Path::new(&secrets_dir);
    if !secrets_path.exists() {
        info!("No secrets directory found, skipping wallet loading");
        return Ok(());
    }
    
    // List all key files
    let entries = match std::fs::read_dir(secrets_path) {
        Ok(entries) => entries,
        Err(e) => {
            warn!("Failed to read secrets directory: {}", e);
            return Err(e);
        }
    };
    
    // Process each key file
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => {
                warn!("Failed to read directory entry: {}", e);
                continue;
            }
        };
        
        let path = entry.path();
        
        // Check if this is a JSON file for a user key
        if path.extension().map_or(false, |ext| ext == "json") && 
           path.file_name().map_or(false, |name| name.to_string_lossy().contains("user_") && name.to_string_lossy().contains("_key")) {
            
            // Extract user ID from filename (user_123_key.json -> 123)
            let filename = path.file_stem().unwrap().to_string_lossy();
            let user_id = filename
                .strip_prefix("user_")
                .and_then(|s| s.split('_').next())
                .unwrap_or("");
            
            if user_id.is_empty() {
                warn!("Could not parse user ID from filename: {:?}", path);
                continue;
            }
            
            info!("Found key file for user {}, loading wallet", user_id);
            
            // Load wallet
            match load_wallet(app_state, user_id).await {
                Ok(_) => info!("Successfully loaded wallet for user {}", user_id),
                Err(e) => warn!("Failed to load wallet for user {}: {}", user_id, e),
            }
        }
    }
    
    // Log summary
    let wallet_count = app_state.wallets.read().unwrap().len();
    info!("Loaded {} wallets", wallet_count);
    
    Ok(())
}

// Load a single wallet
async fn load_wallet(app_state: &web::Data<AppState>, user_id: &str) -> Result<(), PulserError> {
    let config = app_state.config.read().unwrap().clone();
    let config_path = format!("config/service_config.toml");
    let network = match config.network.as_str() {
        "testnet" => Network::Testnet,
        "regtest" => Network::Regtest,
        "signet" => Network::Signet,
        _ => Network::Bitcoin,
    };
    
    // Check if wallet already exists
    {
        let wallets = app_state.wallets.read().unwrap();
        if wallets.contains_key(user_id) {
            debug!("Wallet for user {} already loaded, skipping", user_id);
            return Ok(());
        }
    }
    
    // Create primary Esplora client
    let esplora_client = esplora_client::Builder::new(&config.esplora_url)
        .timeout(30)
        .build_async()
        .map_err(|e| PulserError::NetworkError(format!("Failed to create primary Esplora client: {}", e)))?;
    
    // Create fallback Esplora client if configured
    let fallback_client = if let Some(fallback_url) = config.fallback_esplora_url {
        Some(
            esplora_client::Builder::new(&fallback_url)
                .timeout(30)
                .build_async()
                .map_err(|e| PulserError::NetworkError(format!("Failed to create fallback Esplora client: {}", e)))?
        )
    } else {
        None
    };
    
    // Create wallet with full configuration
    match DepositWallet::from_config(&config_path, user_id) {
        Ok((wallet, deposit_info, chain)) => {
            debug!("Created wallet for user {} with address {}", user_id, deposit_info.address);
            
            // Store wallet with updated prices
            let (raw_price, synthetic) = app_state.get_current_prices();
            
            // Update chain with current prices
            let mut updated_chain = chain;
            updated_chain.raw_btc_usd = raw_price;
            updated_chain.synthetic_price = Some(synthetic);
            
            // Add to wallets map
            let mut wallets = app_state.wallets.write().unwrap();
            wallets.insert(user_id.to_string(), (wallet, updated_chain));
            
            Ok(())
        },
        Err(e) => {
            error!("Failed to create wallet for user {}: {}", user_id, e);
            Err(e)
        }
    }
}

// Price update task
async fn price_update_task(app_state: web::Data<AppState>) {
    info!("Starting price update task");
    
    loop {
        // Sleep first to avoid double update after initial fetch
        sleep(Duration::from_secs(PRICE_UPDATE_INTERVAL_SECS)).await;
        
        // Only fetch if price is stale or about to be
        if app_state.is_price_stale() {
            info!("Fetching updated price information");
            
            // Start retry loop
            let mut last_error = None;
            for retry in 0..RETRY_MAX {
                if retry > 0 {
                    warn!("Price fetch retry attempt {}/{}", retry + 1, RETRY_MAX);
                    sleep(Duration::from_millis(500 * 2u64.pow(retry))).await;
                }
                
                match common::price_feed::fetch_btc_usd_price(&app_state.http_client).await {
                    Ok(price_info) => {
                        app_state.update_prices(&price_info);
                        info!("Updated prices: BTC-USD ${:.2}, Synthetic ${:.2}", 
                             price_info.raw_btc_usd, price_info.synthetic_price.unwrap_or(price_info.raw_btc_usd * DEFAULT_SYNTHETIC_DISCOUNT));
                        break; // Success, exit retry loop
                    },
                    Err(e) => {
                        warn!("Failed to update price (attempt {}/{}): {}", retry + 1, RETRY_MAX, e);
                        last_error = Some(e);
                        // Continue to next retry attempt
                    }
                }
            }
            
            // If all retries failed, log a more severe warning
            if let Some(e) = last_error {
                error!("All price update attempts failed: {}", e);
                error!("Using previous price: ${:.2}", *app_state.current_price.read().unwrap());
            }
        }
    }
}

// Wallet sync task
async fn wallet_sync_task(app_state: web::Data<AppState>) {
    info!("Starting wallet sync task");
    
    loop {
        sleep(Duration::from_secs(WALLET_SYNC_INTERVAL_SECS)).await;
        
        // Lock wallets for reading first to get IDs to minimize lock contention
        let user_ids: Vec<String> = {
            let wallets = app_state.wallets.read().unwrap();
            wallets.keys().cloned().collect()
        };
        
        debug!("Syncing {} wallets", user_ids.len());
        
        // Process each wallet individually to minimize lock time
        for user_id in user_ids {
            let start_time = Instant::now();
            
            debug!("Starting sync for user {}", user_id);
            match sync_single_wallet(&app_state, &user_id).await {
                Ok(_) => {
                    let duration = start_time.elapsed();
                    debug!("Sync completed for user {} in {}ms", user_id, duration.as_millis());
                },
                Err(e) => {
                    warn!("Failed to sync wallet for user {}: {}", user_id, e);
                }
            }
        }
    }
}

// Sync a single wallet
async fn sync_single_wallet(app_state: &web::Data<AppState>, user_id: &str) -> Result<(), PulserError> {
    // Get wallet and chain from shared storage
    let (mut wallet, mut chain) = {
        let wallets = app_state.wallets.read().unwrap();
        if let Some((wallet, chain)) = wallets.get(user_id) {
            (wallet.clone(), chain.clone())
        } else {
            return Err(PulserError::UserNotFound(format!("User {} not found", user_id)));
        }
    };
    
    // Perform the sync
    wallet.monitor_deposits(user_id, &mut chain).await?;
    
    // Update wallet and chain in shared storage
    {
        let mut wallets = app_state.wallets.write().unwrap();
        if let Some(entry) = wallets.get_mut(user_id) {
            *entry = (wallet, chain);
        }
    }
    
    Ok(())
}
