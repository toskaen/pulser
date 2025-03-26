use warp::Filter;
use serde_json::json;
use std::fs;
use deposit_service::wallet::DepositWallet;
use reqwest::Client;
use bdk_wallet::KeychainKind;
use warp::Rejection;

#[derive(Debug)]
struct CustomError(String);
impl warp::reject::Reject for CustomError {}

#[tokio::main]
async fn main() {
    let data_dir = "data_lsp";
    let data_dir_clone = data_dir.to_string();
    let init_wallet = warp::path("init_wallet")
        .and(warp::post())
        .and(warp::body::json())
        .and_then(move |data: serde_json::Value| {
            let data_dir = data_dir_clone.clone();
            async move {
                let user_id = data["user_id"].as_str().unwrap_or("unknown").to_string();
                let (wallet, deposit_info, _chain) = DepositWallet::from_config(
                    "/home/toskaen/pulser/deposit-service/config/service_config.toml", // Absolute path
                    &user_id
                )
                .await
                .map_err(|e| warp::reject::custom(CustomError(format!("Config error: {}", e))))?;
                let recovery_path = format!("{}/user_{}_recovery.txt", data_dir, user_id);
                let recovery_content = fs::read_to_string(&recovery_path)
                    .map_err(|e| warp::reject::custom(CustomError(format!("Read error: {}", e))))?;
                fs::remove_file(&recovery_path)
                    .map_err(|e| warp::reject::custom(CustomError(format!("Failed to delete recovery file: {}", e))))?;
                println!("Deleted recovery file for: {}", user_id);

                let public_data = json!({
                    "wallet_descriptor": deposit_info.descriptor,
                    "internal_descriptor": wallet.wallet.public_descriptor(KeychainKind::Internal).to_string(),
                    "lsp_pubkey": deposit_info.lsp_pubkey,
                    "trustee_pubkey": deposit_info.trustee_pubkey,
                    "user_pubkey": deposit_info.user_pubkey,
                    "user_id": user_id
                });
                let client = Client::new();
                client.post("http://localhost:8081/register")
                    .json(&public_data)
                    .send()
                    .await
                    .map_err(|e| warp::reject::custom(CustomError(format!("Failed to register with LSP: {}", e))))?;

                let response = json!({
                    "user_id": user_id,
                    "recovery_document": recovery_content,
                    "address": deposit_info.address,
                    "descriptor": deposit_info.descriptor
                });
                println!("Initialized wallet for: {}", user_id);
                Ok::<_, Rejection>(warp::reply::json(&response))
            }
        });

    println!("Wallet init service on port 8082 with HTTPS");
    warp::serve(init_wallet)
        .tls()
        .cert_path("/home/toskaen/pulser/cert.pem")
        .key_path("/home/toskaen/pulser/key.pem")
        .run(([0, 0, 0, 0], 8082))
        .await;
}
