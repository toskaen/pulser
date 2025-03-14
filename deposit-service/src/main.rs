// deposit-service/src/main.rs
use actix_web::{web, App, HttpServer, middleware};
use bitcoin::Network;
use common::{PulserError, price_feed};
use structopt::StructOpt;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use log::{info, warn, error, debug};

mod types;
mod config;
mod wallet;
mod blockchain;
mod handlers;
mod integration;

use config::{Config, load_config, init_logging, ensure_directories};
use blockchain::create_esplora_client;
use handlers::{
    AppState,
    create_deposit,
    get_deposit_status,
    get_deposit_utxos,
    get_deposit_events,
    process_withdrawal,
    sign_psbt,
    check_psbt_status,
    initiate_channel_opening,
    get_service_status,
    wallet_sync_task,
    not_found, // Make sure this is imported
};

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
    
    // Log startup information
    info!("Starting Pulser Deposit Service v{} in {} mode on {} network", 
          config.version, config.role, config.network);
    
    // Determine network
    let network = match config.network.as_str() {
        "testnet" => Network::Testnet,
        "regtest" => Network::Regtest,
        "signet" => Network::Signet,
        _ => Network::Bitcoin,
    };
    
    // Create HTTP client with reasonable timeout and retry behavior
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
    
    // Create blockchain client
    let blockchain = match create_esplora_client(network) {
        Ok(client) => Arc::new(client),
        Err(e) => {
            error!("Failed to create blockchain client: {}", e);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));
        }
    };
    
    // Initialize price feeds with placeholder values
    let current_price = Arc::new(RwLock::new(0.0));
    let synthetic_price = Arc::new(RwLock::new(0.0));
    
    // Create application state
    let app_state = web::Data::new(AppState {
        config: Arc::new(RwLock::new(config.clone())),
        wallets: Arc::new(RwLock::new(HashMap::new())),
        blockchain: blockchain.clone(),
        http_client: http_client.clone(),
        current_price: current_price.clone(),
        synthetic_price: synthetic_price.clone(),
        network,
        role: config.role.clone(),
    });
    
    // Initial price fetch to avoid starting with zeros
    info!("Performing initial price fetch...");
    match price_feed::fetch_btc_price(&http_client).await {
        Ok(price_info) => {
            *current_price.write().unwrap() = price_info.raw_btc_usd;
            
            // Calculate synthetic price
            let synthetic = price_info.synthetic_price.unwrap_or_else(|| {
                // If no synthetic price provided, use a simple calculation
                price_info.raw_btc_usd * 0.99 // 1% discount for simplicity
            });
            
            *synthetic_price.write().unwrap() = synthetic;
            
            info!("Initial prices fetched: BTC-USD ${:.2}, Synthetic ${:.2}", 
                 price_info.raw_btc_usd, synthetic);
        },
        Err(e) => {
            warn!("Initial price fetch failed: {}. Using fallback values.", e);
            // Set fallback values
            *current_price.write().unwrap() = 30000.0;
            *synthetic_price.write().unwrap() = 29700.0;
        }
    }
    
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
            .service(web::resource("/deposit").route(web::post().to(create_deposit)))
            .service(web::resource("/deposit/{user_id}").route(web::get().to(get_deposit_status)))
            .service(web::resource("/deposit/{user_id}/utxos").route(web::get().to(get_deposit_utxos)))
            .service(web::resource("/deposit/{user_id}/events").route(web::get().to(get_deposit_events)))
            .service(web::resource("/withdraw").route(web::post().to(process_withdrawal)))
            .service(web::resource("/sign_psbt").route(web::post().to(sign_psbt)))
            .service(web::resource("/check_psbt").route(web::post().to(check_psbt_status)))
            .service(web::resource("/initiate_channel/{user_id}").route(web::post().to(initiate_channel_opening)))
            .service(web::resource("/status").route(web::get().to(get_service_status)))
            .service(web::resource("/health").route(web::get().to(|| async { "OK" })))
            .default_service(web::route().to(not_found))
    })
    .bind(&server_addr)?
    .workers(4)
    .run()
    .await
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
                
                // Calculate synthetic price if we have one
                if let Some(synthetic) = price_info.synthetic_price {
                    *app_state.synthetic_price.write().unwrap() = synthetic;
                    debug!("Updated prices: BTC-USD ${:.2}, Synthetic ${:.2}", 
                         price_info.raw_btc_usd, synthetic);
                } else {
                    // If no synthetic price available, calculate a simple one
                    let synthetic = price_info.raw_btc_usd * 0.99; // 1% discount for simplicity
                    *app_state.synthetic_price.write().unwrap() = synthetic;
                    debug!("Updated prices: BTC-USD ${:.2}, Simple Synthetic ${:.2}", 
                         price_info.raw_btc_usd, synthetic);
                }
            },
            Err(e) => {
                warn!("Failed to update price: {}", e);
            }
        }
    }
}
