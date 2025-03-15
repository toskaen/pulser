// deposit-service/src/main.rs
use actix_web::{web, App, HttpServer, middleware, HttpResponse, ResponseError};
use bitcoin::Network;
use common::error::PulserError;
use common::price_feed;
use structopt::StructOpt;
use std::sync::{Arc, RwLock, Mutex};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use log::{info, warn, error, debug};
use bdk_esplora::esplora_client::EsploraClient;
use crate::monitoring::MonitoringService;


mod types;
mod config;
mod wallet;
mod blockchain;
mod handlers;
mod integration;
mod secure_storage;

use config::{Config, load_config, init_logging, ensure_directories};
use blockchain::create_esplora_client;
use secure_storage::SecureStorage;
use wallet::DepositWallet;
use types::{StableChain, Utxo};

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
    pub wallets: Arc<RwLock<HashMap<u32, (DepositWallet, StableChain)>>>,
    pub secure_storage: Arc<SecureStorage>,
    pub blockchain: Arc<EsploraClient>,
    pub http_client: reqwest::Client,
    pub current_price: Arc<RwLock<f64>>,
    pub synthetic_price: Arc<RwLock<f64>>,
    pub network: Network,
    pub role: String,
}

impl AppState {
    pub fn new(
        config: Config,
        blockchain: Arc<EsploraClient>,
        secure_storage: Arc<SecureStorage>,
        network: Network,
    ) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(format!("Pulser/{}", config.version))
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(10)
            .build()
            .unwrap_or_else(|e| {
                warn!("Failed to create custom HTTP client: {}, using default", e);
                reqwest::Client::new()
            });
            
        Self {
            config: Arc::new(RwLock::new(config.clone())),
            wallets: Arc::new(RwLock::new(HashMap::new())),
            secure_storage,
            blockchain,
            http_client,
            current_price: Arc::new(RwLock::new(0.0)),
            synthetic_price: Arc::new(RwLock::new(0.0)),
            network,
            role: config.role,
        }
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
    
    // Initialize secure storage
    let secure_storage = match SecureStorage::new(&config.data_dir) {
        Ok(storage) => Arc::new(storage),
        Err(e) => {
            error!("Failed to initialize secure storage: {}", e);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));
        }
    };
    
    // Create blockchain client
    let blockchain = match create_esplora_client(network) {
        Ok(client) => Arc::new(client),
        Err(e) => {
            error!("Failed to create blockchain client: {}", e);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));
        }
    };
    
    let wallets = Arc::new(RwLock::new(HashMap::<u32, (DepositWallet, StableChain)>::new()));
    let config_arc = Arc::new(RwLock::new(config.clone()));
    let current_price = Arc::new(RwLock::new(30000.0)); // Initial placeholder value
    let synthetic_price = Arc::new(RwLock::new(29700.0)); // Initial placeholder value
    
     let monitoring_service = MonitoringService::new(
        wallets.clone(),
        config_arc.clone(),
        current_price.clone(),
        synthetic_price.clone(),
    );
    monitoring_service.start().await;
    
    // Create application state
  let app_state = web::Data::new(AppState {
        config: config_arc.clone(),
        wallets: wallets.clone(),
        http_client: client.clone(),
        current_price: current_price.clone(),
        synthetic_price: synthetic_price.clone(),
        network: network,
        role: config.role.clone(),
    });
    
    // Perform initial price fetch
    info!("Performing initial price fetch...");
    let http_client = app_state.http_client.clone();
    match price_feed::fetch_btc_price(&http_client).await {
        Ok(price_info) => {
            *app_state.current_price.write().unwrap() = price_info.raw_btc_usd;
            
            // Calculate synthetic price
            let synthetic = price_info.synthetic_price.unwrap_or_else(|| {
                match price_feed::calculate_synthetic_price(price_info.price_feeds.clone()) {
                    Ok(price) => price,
                    Err(_) => price_info.raw_btc_usd * 0.99, // Simple 1% discount as fallback
                }
            });
            
            *app_state.synthetic_price.write().unwrap() = synthetic;
            
            info!("Initial prices fetched: BTC-USD ${:.2}, Synthetic ${:.2}", 
                 price_info.raw_btc_usd, synthetic);
        },
        Err(e) => {
            warn!("Initial price fetch failed: {}. Using fallback values.", e);
            // Set fallback values
            *app_state.current_price.write().unwrap() = 30000.0;
            *app_state.synthetic_price.write().unwrap() = 29700.0;
        }
    }
    
    // Load existing wallets
    info!("Loading existing wallets...");
    load_existing_wallets(&app_state).await;
    
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
            .route("/deposit/{user_id}/events", web::get().to(handlers::get_deposit_events))
            .route("/withdraw", web::post().to(handlers::process_withdrawal))
            .route("/sign_psbt", web::post().to(handlers::sign_psbt))
            .route("/check_psbt", web::post().to(handlers::check_psbt_status))
            .route("/initiate_channel/{user_id}", web::post().to(handlers::initiate_channel_opening))
            .route("/status", web::get().to(handlers::get_service_status))
            .route("/health", web::get().to(|| async { "OK" }))
            .default_service(web::route().to(handlers::not_found))
    })
    .bind(&server_addr)?
    .workers(4)
    .run()
    .await
}

// Load existing wallets from secure storage
async fn load_existing_wallets(app_state: &web::Data<AppState>) {
    let wallet_ids = match app_state.secure_storage.list_wallet_ids() {
        Ok(ids) => ids,
        Err(e) => {
            warn!("Failed to list wallet IDs: {}", e);
            return;
        }
    };
    
    info!("Found {} existing wallets", wallet_ids.len());
    
    for user_id in wallet_ids {
        match load_wallet(app_state, user_id).await {
            Ok(_) => info!("Loaded wallet for user {}", user_id),
            Err(e) => warn!("Failed to load wallet for user {}: {}", user_id, e),
        }
    }
}

// Load a single wallet
async fn load_wallet(app_state: &web::Data<AppState>, user_id: u32) -> Result<(), PulserError> {
    // Get wallet secrets
    let secrets = app_state.secure_storage.get_wallet(user_id)?;
    
    // Determine network
    let network = match secrets.network.as_str() {
        "testnet" => Network::Testnet,
        "regtest" => Network::Regtest,
        "signet" => Network::Signet,
        _ => Network::Bitcoin,
    };
    
    // Initialize wallet path
    let config = app_state.config.read().unwrap();
    let wallet_path = format!("{}/wallets/wallet_{}.db", config.data_dir, user_id);
    
    // Create wallet instance
    let wallet = DepositWallet::new(
        &secrets.descriptor,
        None, // No internal descriptor
        network,
        app_state.blockchain.clone(),
        &wallet_path,
    )?;
    
    // Create StableChain state
    let chain = StableChain {
        user_id,
        is_stable_receiver: true,
        counterparty: secrets.lsp_pubkey.clone(),
        accumulated_btc: types::Bitcoin::from_sats(0), // Will be updated by sync
        stabilized_usd: common::types::USD(0.0),      // Will be updated by sync
        timestamp: secrets.created_at,
        formatted_datetime: secure_storage::format_timestamp(secrets.created_at),
        sc_dir: format!("{}/wallets/wallet_{}", config.data_dir, user_id),
        raw_btc_usd: *app_state.current_price.read().unwrap(),
        synthetic_price: Some(*app_state.synthetic_price.read().unwrap()),
        prices: HashMap::new(), // Will be populated
        multisig_addr: secrets.address.clone(),
        utxos: Vec::new(), // Will be populated by sync
        pending_sweep_txid: None,
        events: Vec::new(), // Will be populated
        total_withdrawn_usd: 0.0,
        expected_usd: common::types::USD(0.0), // No expected amount
        hedge_position_id: None,
        pending_channel_id: None,
    };
    
    // Initialize the wallet by syncing with blockchain
    tokio::task::spawn_blocking(move || {
        // This is a placeholder - in a real implementation we would
        // make this asynchronous using blockchain.sync()
    }).await.map_err(|e| PulserError::InternalError(format!("Tokio task error: {}", e)))?;
    
    // Add to wallets map
    let mut wallets = app_state.wallets.write().unwrap();
    wallets.insert(user_id, (wallet, chain));
    
    Ok(())
}

// Price update task
async fn price_update_task(app_state: web::Data<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(60)); // Every minute
    
    loop {
        interval.tick().await;
        
        match price_feed::fetch_btc_price(&app_state.http_client).await {
            Ok(price_info) => {
                // Update current price
                *app_state.current_price.write().unwrap() = price_info.raw_btc_usd;
                
                // Calculate synthetic price
                let synthetic = match price_info.synthetic_price {
                    Some(price) => price,
                    None => match price_feed::calculate_synthetic_price(price_info.price_feeds.clone()) {
                        Ok(price) => price,
                        Err(e) => {
                            warn!("Failed to calculate synthetic price: {}, using simple fallback", e);
                            price_info.raw_btc_usd * 0.99 // Simple 1% discount
                        }
                    }
                };
                
                *app_state.synthetic_price.write().unwrap() = synthetic;
                
                debug!("Updated prices: BTC-USD ${:.2}, Synthetic ${:.2}", 
                     price_info.raw_btc_usd, synthetic);
                
                // Update wallet states with new prices
                let mut wallets = app_state.wallets.write().unwrap();
                for (_, (_, chain)) in wallets.iter_mut() {
                    chain.raw_btc_usd = price_info.raw_btc_usd;
                    chain.synthetic_price = Some(synthetic);
                    
                    // Update USD value of accumulated BTC
                    chain.stabilized_usd = common::types::USD(
                        chain.accumulated_btc.sats as f64 / 100_000_000.0 * price_info.raw_btc_usd
                    );
                }
            },
            Err(e) => {
                warn!("Failed to update price: {}", e);
            }
        }
    }
}

// Wallet sync task
async fn wallet_sync_task(app_state: web::Data<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(30)); // Every 30 seconds
    
    loop {
        interval.tick().await;
        
        // Lock wallets for reading first to get IDs
        let user_ids: Vec<u32> = {
            let wallets = app_state.wallets.read().unwrap();
            wallets.keys().cloned().collect()
        };
        
        // Process each wallet individually to minimize lock time
        for user_id in user_ids {
            if let Err(e) = sync_single_wallet(&app_state, user_id).await {
                warn!("Failed to sync wallet for user {}: {}", user_id, e);
            }
        }
    }
}

// Sync a single wallet
async fn sync_single_wallet(app_state: &web::Data<AppState>, user_id: u32) -> Result<(), PulserError> {
    // Get wallet and chain
    let mut wallet_and_chain = {
        let mut wallets = app_state.wallets.write().unwrap();
        if let Some(wallet_and_chain) = wallets.get_mut(&user_id) {
            (wallet_and_chain.0.clone(), wallet_and_chain.1.clone())
        } else {
            return Err(PulserError::UserNotFound(format!("User {} not found", user_id)));
        }
    };
    
    // Sync wallet with blockchain
    if let Err(e) = wallet_and_chain.0.sync().await {
        warn!("Wallet sync failed for user {}: {}", user_id, e);
        return Err(e);
    }
    
    // Get UTXOs
    match wallet_and_chain.0.list_utxos() {
        Ok(mut utxos) => {
            // Update UTXO confirmations
            if let Err(e) = wallet_and_chain.0.update_utxo_confirmations(&mut utxos).await {
                warn!("Failed to update UTXO confirmations for user {}: {}", user_id, e);
            }
            
            // Update chain state with UTXOs
            let old_utxo_count = wallet_and_chain.1.utxos.len();
            wallet_and_chain.1.utxos = utxos.clone();
            
            // Calculate accumulated BTC
            let total_sats = utxos.iter().map(|u| u.amount).sum::<u64>();
            wallet_and_chain.1.accumulated_btc = types::Bitcoin::from_sats(total_sats);
            
            // Update stabilized USD value
            wallet_and_chain.1.stabilized_usd = common::types::USD(
                total_sats as f64 / 100_000_000.0 * wallet_and_chain.1.raw_btc_usd
            );
            
            // Log deposit events if needed
            if utxos.len() > old_utxo_count {
                wallet_and_chain.1.events.push(common::types::Event {
                    timestamp: common::utils::now_timestamp(),
                    source: "DepositService".to_string(),
                    kind: "DepositReceived".to_string(),
                    details: format!(
                        "Received new deposit. Total: {} sats (${:.2})",
                        total_sats, wallet_and_chain.1.stabilized_usd.0
                    ),
                });
                
                info!("New deposit detected for user {}: {} sats (${:.2})",
                     user_id, total_sats, wallet_and_chain.1.stabilized_usd.0);
            }
        },
        Err(e) => {
            warn!("Failed to list UTXOs for user {}: {}", user_id, e);
        }
    }
    
    // Update in wallets map
    {
        let mut wallets = app_state.wallets.write().unwrap();
        if let Some(entry) = wallets.get_mut(&user_id) {
            *entry = (wallet_and_chain.0, wallet_and_chain.1);
        }
    }
    
    Ok(())
}
