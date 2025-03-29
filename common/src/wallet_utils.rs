// common/src/wallet_utils.rs
use std::sync::Arc;
use bdk_wallet::bitcoin::{Address, Network};
use bdk_wallet::{Wallet, KeychainKind, Balance};
use bdk_esplora::EsploraAsyncExt;
use bdk_esplora::esplora_client::AsyncClient;
use bdk_chain::spk_client::FullScanResponse;
use crate::{types::{StableChain, UtxoInfo, USD, Bitcoin, Utxo}, error::PulserError, StateManager, price_feed::PriceFeed};
use crate::types::PriceInfo;
use std::str::FromStr;

pub async fn sync_and_stabilize_utxos(
    user_id: &str,
    wallet: &mut Wallet,
    esplora: &AsyncClient,
    chain: &mut StableChain,
    price_feed: Arc<PriceFeed>,
    price_info: &PriceInfo,
    deposit_addr: &Address,
    change_addr: &Address,
    state_manager: &StateManager,
    spent_utxos: Option<Vec<(String, u32)>>,
    min_confirmations: u32,
) -> Result<Vec<UtxoInfo>, PulserError> {
    let stabilization_price = price_feed.get_deribit_price().await?;
    let mut new_utxos = Vec::new();
    let previous_utxos = chain.utxos.clone();

    let request = wallet.start_full_scan();
    let update = esplora.full_scan(request, 10, 5).await?;
    wallet.apply_update(update)?;

    let utxos: Vec<UtxoInfo> = wallet.list_unspent().into_iter().map(|u| {
        let confirmations = match u.chain_position {
            bdk_chain::ChainPosition::Confirmed { anchor, .. } => wallet.latest_checkpoint().height() - anchor.block_id.height + 1,
            bdk_chain::ChainPosition::Unconfirmed { .. } => 0,
        };
        UtxoInfo {
            txid: u.outpoint.txid.to_string(),
            vout: u.outpoint.vout,
            amount_sat: u.txout.value.to_sat(),
            address: Address::from_script(&u.txout.script_pubkey, Network::Testnet).unwrap().to_string(),
            confirmations,
            spent: u.is_spent,
            stable_value_usd: 0.0,
            keychain: "External".to_string(),
            timestamp: chrono::Utc::now().timestamp() as u64,
            participants: vec!["user".to_string(), "lsp".to_string(), "trustee".to_string()],
            spendable: confirmations >= 1,
            derivation_path: "".to_string(),
        }
    }).collect();
    let utxos_info = utxos.into_iter().map(|u| Ok::<UtxoInfo, PulserError>(u)).collect::<Result<Vec<_>, _>>()?;
    let utxos: Vec<Utxo> = utxos_info.iter().map(|u| Ok::<Utxo, PulserError>(Utxo {
        txid: u.txid.clone(),
        vout: u.vout,
        amount: u.amount_sat,
        script_pubkey: Address::from_str(&u.address)?.require_network(Network::Testnet)?.script_pubkey().to_hex_string(),
        confirmations: u.confirmations,
        height: None,
        usd_value: Some(USD(u.stable_value_usd)),
        spent: u.spent,
    })).collect::<Result<Vec<_>, _>>()?;

    for utxo in &utxos_info {
        let stable_value_usd = (utxo.amount_sat as f64 / 100_000_000.0) * stabilization_price;
        if !previous_utxos.iter().any(|u| u.txid == utxo.txid && u.vout == utxo.vout) && utxo.confirmations >= min_confirmations {
            let mut utxo_info = utxo.clone();
            utxo_info.stable_value_usd = stable_value_usd;
            chain.history.push(utxo_info.clone());
            new_utxos.push(utxo_info);
        }
    }

    if let Some(spent_utxos) = spent_utxos {
        for (txid, vout) in spent_utxos {
            if let Some(utxo) = chain.history.iter_mut().find(|h| h.txid == txid && h.vout == vout) {
                utxo.spent = true;
            }
        }
    }

    chain.utxos = utxos;
    let balance = wallet.balance();
    chain.accumulated_btc = Bitcoin::from_sats(balance.confirmed.to_sat());
    chain.stabilized_usd = USD(chain.history.iter().filter(|h| !h.spent).map(|h| h.stable_value_usd).sum());
    chain.raw_btc_usd = stabilization_price;

    state_manager.save_stable_chain(user_id, chain).await?;
    if let Some(changeset) = wallet.take_staged() {
        state_manager.save_changeset(user_id, &changeset).await?;
    }

    Ok(new_utxos)
}
