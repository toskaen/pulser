use crate::error::PulserError;
use crate::health::{Component, ComponentType, HealthCheck, HealthResult};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

pub struct BlockchainCheck {
    component: Component,
    client: Arc<bdk_esplora::esplora_client::AsyncClient>,
    max_tip_age_secs: u64,
}

impl BlockchainCheck {
    pub fn new(client: Arc<bdk_esplora::esplora_client::AsyncClient>) -> Self {
        Self {
            component: Component {
                name: "blockchain".to_string(),
                component_type: ComponentType::Blockchain,
                description: "Esplora blockchain API".to_string(),
                is_critical: true,
            },
            client,
            max_tip_age_secs: 3600, // 1 hour max acceptable tip age
        }
    }

    pub fn with_max_tip_age(mut self, max_tip_age_secs: u64) -> Self {
        self.max_tip_age_secs = max_tip_age_secs;
        self
    }
}

#[async_trait]
impl HealthCheck for BlockchainCheck {
    async fn check(&self) -> Result<HealthResult, PulserError> {
        // Try to get the current block height
        match timeout(Duration::from_secs(5), self.client.get_height()).await {
            Ok(Ok(height)) => {
                // Now check the block timestamp to ensure it's recent
                match timeout(Duration::from_secs(5), self.client.get_block_hash(height)).await {
                    Ok(Ok(hash)) => {
                        // Get BlockSummary
                        match timeout(Duration::from_secs(5), self.client.get_blocks(Some(height))).await {
                            Ok(Ok(blocks)) => {
                                if let Some(block_info) = blocks.first() {
                                    // Access the timestamp field of BlockTime
                                    let block_time = block_info.time.timestamp as u64;
                                    let now = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_else(|_| Duration::from_secs(0))
                                        .as_secs();

                                    let block_age = now.saturating_sub(block_time);

                                    if block_age > self.max_tip_age_secs {
                                        return Ok(HealthResult::Degraded {
                                            reason: format!("Block tip is stale: {}s old", block_age),
                                        });
                                    }

                                    Ok(HealthResult::Healthy)
                                } else {
                                    Ok(HealthResult::Degraded {
                                        reason: "No block information returned".to_string(),
                                    })
                                }
                            }
                            Ok(Err(e)) => Ok(HealthResult::Degraded {
                                reason: format!("Failed to get block info: {}", e),
                            }),
                            Err(_) => Ok(HealthResult::Degraded {
                                reason: "Timeout fetching block info".to_string(),
                            }),
                        }
                    }
                    Ok(Err(e)) => Ok(HealthResult::Degraded {
                        reason: format!("Failed to get block hash: {}", e),
                    }),
                    Err(_) => Ok(HealthResult::Degraded {
                        reason: "Timeout fetching block hash".to_string(),
                    }),
                }
            }
            Ok(Err(e)) => Ok(HealthResult::Unhealthy {
                reason: format!("Failed to get blockchain height: {}", e),
            }),
            Err(_) => Ok(HealthResult::Unhealthy {
                reason: "Timeout fetching blockchain height".to_string(),
            }),
        }
    }

    fn component(&self) -> &Component {
        &self.component
    }
}
