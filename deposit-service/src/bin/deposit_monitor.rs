use bdk_wallet::{Wallet, KeychainKind, Update, CreateParams};
use bdk_esplora::{esplora_client, EsploraAsyncExt};
use bdk_chain::{local_chain::LocalChain, BlockId};
use bitcoin::{Network, Address, BlockHash};
use std::env;
use std::fs;
use std::str::FromStr;

async fn sync_wallet(wallet: &mut Wallet, blockchain: &esplora_client::AsyncClient) -> Result<(), String> {
    let spk_iters = wallet.all_unbounded_spk_iters();
    let external_spks: Vec<_> = spk_iters.get(&KeychainKind::External).unwrap().clone()
        .take(5)
        .map(|(_, spk)| spk)
        .collect();
    let internal_spks: Vec<_> = spk_iters.get(&KeychainKind::Internal).unwrap().clone()
        .take(5)
        .map(|(_, spk)| spk)
        .collect();
    let all_spks: Vec<_> = external_spks.into_iter().chain(internal_spks).collect();

    let request = bdk_chain::spk_client::SyncRequest::builder().spks(all_spks).build();
    let response = blockchain.sync(request, 10).await
        .map_err(|e| format!("Sync failed: {:?}", e))?;

    let chain_tip = blockchain.get_height().await
        .map_err(|e| format!("Get height failed: {:?}", e))?;
    let chain_tip_hash = blockchain.get_block_hash(chain_tip).await
        .map_err(|e| format!("Get block hash failed: {:?}", e))?;
    let mut chain = wallet.local_chain().clone();
    for (anchor, txid) in &response.tx_update.anchors {
        chain.insert_block(anchor.block_id)
            .map_err(|e| format!("Anchor insert failed for {}: {}", txid, e))?;
    }
    chain.insert_block(BlockId { height: chain_tip, hash: chain_tip_hash })
        .map_err(|e| format!("Tip insert failed: {}", e))?;

    let update = Update {
        tx_update: response.tx_update,
        chain: Some(chain.tip()),
        last_active_indices: Default::default(),
    };
    wallet.apply_update(update)
        .map_err(|e| format!("Apply update failed: {:?}", e))?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    let user_id = args.get(1).unwrap_or(&"1".to_string()).clone();

    eprintln!("Starting deposit monitor for user {}", user_id);

    let config_str = fs::read_to_string("config/service_config.toml")?;
    let config: toml::Value = toml::from_str(&config_str)?;
    let esplora_url = config["esplora_url"].as_str().unwrap();
    eprintln!("Using Esplora URL: {}", esplora_url);
    let blockchain = esplora_client::Builder::new(esplora_url).timeout(30).build_async()?;

    let key_path = format!("data/secrets/user_{}_key.json", user_id);
    let key_json = fs::read_to_string(&key_path)?;
    let key_material: serde_json::Value = serde_json::from_str(&key_json)?; // Placeholder

    let external_descriptor = key_material["wallet_descriptor"]
        .as_str()
        .unwrap_or_else(|| panic!("External descriptor missing for user {}", user_id))
        .to_string();
    let internal_descriptor = key_material["internal_descriptor"]
        .as_str()
        .unwrap_or_else(|| panic!("Internal descriptor missing for user {}", user_id))
        .to_string();

    eprintln!("External descriptor: {}", external_descriptor);
    eprintln!("Internal descriptor: {}", internal_descriptor);

    // Non-persistent wallet
    let params = Wallet::create(external_descriptor.clone(), internal_descriptor.clone())
        .network(Network::Testnet);
    let mut wallet = Wallet::create_with_params(params)?;

    let genesis_hash = BlockHash::from_str("000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943")?;
    let (chain, _) = LocalChain::from_genesis_hash(genesis_hash);
    wallet.apply_update(Update {
        tx_update: Default::default(),
        chain: Some(chain.tip()),
        last_active_indices: Default::default(),
    })?;

    // Reveal next unused address
    let next_addr = wallet.reveal_next_address(KeychainKind::External);
    eprintln!("Next deposit address: {}", next_addr.address);

    // Manual check for reference
    let target_addr = Address::from_str("tb1pwlxtmqxa8gampnup5mqew77xt4fh8670a5meucn662e33wds5ymqc6u54d")?.assume_checked();
    eprintln!("Manually checking address: {:?}", target_addr);
    let addr_txs = blockchain.get_address_txs(&target_addr, None).await?;
    for tx in &addr_txs {
        eprintln!("TX details: txid={}, outputs={:?}", tx.txid, tx.vout);
    }

    eprintln!("Attempting sync");
    sync_wallet(&mut wallet, &blockchain).await?;

    let utxos: Vec<_> = wallet.list_unspent().collect();
    eprintln!("UTXOs found: {}", utxos.len());
    for utxo in &utxos {
        if utxo.txout.value.to_sat() > 0 {
            let keychain = match utxo.keychain {
                KeychainKind::External => "external (deposit)",
                KeychainKind::Internal => "internal (change)",
            };
            eprintln!("Funds detected for user {}: {} BTC at {} ({})", 
                user_id, utxo.txout.value.to_sat() as f64 / 100_000_000.0, utxo.outpoint.txid, keychain);
        }
    }

    eprintln!("Done");
    Ok(())
}
