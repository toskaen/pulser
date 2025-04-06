use bdk_wallet::bitcoin::{Network, Address, BlockHash};
use bdk_wallet::{Wallet, KeychainKind, ChangeSet, Update};
use bdk_chain::{local_chain, tx_graph, keychain_txout, CheckPoint, BlockId};
use bdk_esplora::esplora_client;
use common::error::PulserError;
use common::types::DepositAddressInfo;
use log::{info, debug};
use std::str::FromStr;
use crate::config::Config;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use tokio::runtime::Runtime;

pub fn apply_changeset(
    config: &Config,
    user_id: &str,
    external_descriptor: String,
    internal_descriptor: String,
    user_pubkey: String,
    changeset: Option<ChangeSet>,
) -> Result<(Wallet, DepositAddressInfo, Option<String>), PulserError> {
    let network = Network::from_str(&config.network)?;
    let mut wallet = Wallet::create(external_descriptor.clone(), internal_descriptor.clone())
        .network(network)
        .create_wallet_no_persist()?;

    if let Some(changeset) = changeset {
        let blocks: BTreeMap<u32, BlockHash> = changeset
            .local_chain
            .blocks
            .iter()
            .map(|(&height, &opt_hash)| (height, opt_hash.expect("Block hash exists")))
            .collect();
        let chain = local_chain::LocalChain::from_blocks(blocks)
            .map_err(|e| PulserError::WalletError(format!("Chain creation failed: {}", e)))?;

        debug!(
            "User {}: chain from changeset, tip: {}",
            user_id,
            chain.tip().height()
        );

        let genesis_checkpoint = chain
            .iter_checkpoints()
            .next()
            .map(|cp| cp)  // Use .map() to extract the checkpoint directly
            .expect("Genesis checkpoint exists");

        let update = Update {
            last_active_indices: BTreeMap::new(),
            chain: Some(genesis_checkpoint),
            tx_update: tx_graph::TxUpdate {
                txs: changeset.tx_graph.txs.into_iter().collect(),
                txouts: changeset.tx_graph.txouts,
                anchors: changeset.tx_graph.anchors,
                seen_ats: changeset.tx_graph.last_seen.into_iter().collect(),
            },
        };
        debug!("Applying update for user {}", user_id);
        wallet.apply_update(update)?;
        debug!("User {} tip after update: {}", user_id, wallet.local_chain().tip().height());
    } else {
        let blockchain = esplora_client::Builder::new(&config.esplora_url).build_async()?;
        let tip = Runtime::new()?.block_on(blockchain.get_height())?;
        let tip_hash = Runtime::new()?.block_on(blockchain.get_block_hash(tip))?;
        let genesis_hash = Runtime::new()?.block_on(blockchain.get_block_hash(0))?;

        let mut blocks = BTreeMap::new();
        blocks.insert(0, genesis_hash);
        blocks.insert(tip, tip_hash);
        let chain = local_chain::LocalChain::from_blocks(blocks)
            .map_err(|e| PulserError::WalletError(format!("Chain creation failed: {}", e)))?;

        let genesis_checkpoint = chain
            .iter_checkpoints()
            .next()
            .map(|cp| cp)  // Use .map() to extract the checkpoint directly
            .expect("Genesis checkpoint exists");

        let update = Update {
            last_active_indices: BTreeMap::new(),
            chain: Some(genesis_checkpoint),
            tx_update: tx_graph::TxUpdate {
                txs: Vec::new(),
                txouts: BTreeMap::new(),
                anchors: BTreeSet::new(),
                seen_ats: HashMap::new(),
            },
        };
        debug!(
            "Initializing user {} with chain from height 0 to {}",
            user_id, tip
        );
        wallet.apply_update(update)?;
        debug!("User {} tip after init: {}", user_id, wallet.local_chain().tip().height());
    }

    let initial_addr = wallet.reveal_next_address(KeychainKind::External).address;
    wallet.reveal_next_address(KeychainKind::Internal);

    let deposit_info = DepositAddressInfo {
        address: initial_addr.to_string(),
        user_id: user_id.parse().unwrap_or(0),
        multisig_type: "2-of-3".to_string(),
        participants: vec![
            user_pubkey.clone(),
            config.lsp_pubkey.clone(),
            config.trustee_pubkey.clone(),
        ],
        descriptor: Some(external_descriptor.clone()),
        path: "m/86'/1'/0'/0/0".to_string(),
        user_pubkey,
        lsp_pubkey: config.lsp_pubkey.clone(),
        trustee_pubkey: config.trustee_pubkey.clone(),
    };

    info!("Applied changeset for user {}: {}", user_id, initial_addr);
    Ok((wallet, deposit_info, None))
}
