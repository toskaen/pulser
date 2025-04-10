// common/src/task_manager.rs
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, Mutex};
use std::time::{Duration, Instant};
use log::{debug, warn, trace};
use std::sync::atomic::{AtomicU32, Ordering};

// Constants
const TASK_LOCK_TIMEOUT_SECS: u64 = 3;

/// Manages user task locks to prevent concurrent operations on the same user data
#[derive(Clone)]
pub struct UserTaskLock {
    // Use RwLock for better read concurrency when checking active users
    active_tasks: Arc<RwLock<HashMap<String, (Instant, String)>>>,
    // Mutex for task counters to track system load
    task_counters: Arc<Mutex<HashMap<String, u32>>>,
        // New performance tracking fields
       sync_time_total: Arc<AtomicU32>,
    sync_time_count: Arc<AtomicU32>,
    sync_time_max: Arc<AtomicU32>,
    price_time_total: Arc<AtomicU32>,
    price_time_count: Arc<AtomicU32>,
}


impl UserTaskLock {
    pub fn new() -> Self {
        Self {
                        active_tasks: Arc::new(RwLock::new(HashMap::new())),
            task_counters: Arc::new(Mutex::new(HashMap::new())),
            sync_time_total: Arc::new(AtomicU32::new(0)),
            sync_time_count: Arc::new(AtomicU32::new(0)),
            sync_time_max: Arc::new(AtomicU32::new(0)),
            price_time_total: Arc::new(AtomicU32::new(0)),
            price_time_count: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Check if a user has an active task without blocking other readers
    pub async fn is_user_active(&self, user_id: &str) -> bool {
        match tokio::time::timeout(
            Duration::from_secs(TASK_LOCK_TIMEOUT_SECS),
            self.active_tasks.read()
        ).await {
            Ok(tasks) => {
                // Also check if the task is stale (over 5 minutes old)
                if let Some((timestamp, _)) = tasks.get(user_id) {
                    if timestamp.elapsed() > Duration::from_secs(300) {
                        trace!("Detected stale lock for user {}", user_id);
                        return false; // Consider stale locks as inactive
                    }
                    true
                } else {
                    false
                }
            },
            Err(_) => {
                warn!("Timeout acquiring read lock for active_tasks when checking user {}", user_id);
                false // Assume not active if we can't get the lock
            }
        }
    }

    /// Mark a user as active with the task type
    pub async fn mark_user_active(&self, user_id: &str, task_type: &str) -> bool {
        // First check if the user is already active with read lock
        let already_active = self.is_user_active(user_id).await;
        if already_active {
            return false;
        }
        
        // Only use write lock if user isn't active
        match tokio::time::timeout(
            Duration::from_secs(TASK_LOCK_TIMEOUT_SECS),
            self.active_tasks.write()
        ).await {
            Ok(mut tasks) => {
                // Double-check in case another thread marked it active
                if let Some((timestamp, _)) = tasks.get(user_id) {
                    if timestamp.elapsed() <= Duration::from_secs(300) {
                        return false;
                    }
                }
                
                // Mark as active
                tasks.insert(user_id.to_string(), (Instant::now(), task_type.to_string()));
                
                // Increment task counter
                if let Ok(mut counters) = self.task_counters.try_lock() {
                    *counters.entry(task_type.to_string()).or_insert(0) += 1;
                }
                
                true
            },
            Err(_) => {
                warn!("Timeout acquiring write lock for active_tasks when marking user {} active", user_id);
                false
            }
        }
    }

    /// Mark a user as inactive
    pub async fn mark_user_inactive(&self, user_id: &str) {
        match tokio::time::timeout(
            Duration::from_secs(TASK_LOCK_TIMEOUT_SECS),
            self.active_tasks.write()
        ).await {
            Ok(mut tasks) => {
                // Get the task type before removing
                let task_type = if let Some((_, task_type)) = tasks.get(user_id) {
                    Some(task_type.clone())
                } else {
                    None
                };
                
                // Remove the task
                tasks.remove(user_id);
                
                // Decrement counter if we know the task type
                if let Some(task_type) = task_type {
                    if let Ok(mut counters) = self.task_counters.try_lock() {
                        if let Some(count) = counters.get_mut(&task_type) {
                            *count = count.saturating_sub(1);
                        }
                    }
                }
            },
            Err(_) => {
                warn!("Timeout acquiring write lock for active_tasks when marking user {} inactive", user_id);
            }
        }
    }
    
    /// Get all active user tasks - for monitoring
    pub async fn get_active_tasks(&self) -> HashMap<String, String> {
        match tokio::time::timeout(
            Duration::from_secs(TASK_LOCK_TIMEOUT_SECS),
            self.active_tasks.read()
        ).await {
            Ok(tasks) => {
                let mut result = HashMap::new();
                for (user_id, (timestamp, task_type)) in tasks.iter() {
                    // Only include non-stale tasks
                    if timestamp.elapsed() <= Duration::from_secs(300) {
                        result.insert(user_id.clone(), task_type.clone());
                    }
                }
                result
            },
            Err(_) => {
                warn!("Timeout acquiring read lock for listing active tasks");
                HashMap::new()
            }
        }
    }
    
    /// Get task type counts - for monitoring
    pub async fn get_task_counts(&self) -> HashMap<String, u32> {
        match tokio::time::timeout(
            Duration::from_secs(TASK_LOCK_TIMEOUT_SECS),
            self.task_counters.lock()
        ).await {
            Ok(counters) => {
                counters.clone()
            },
            Err(_) => {
                warn!("Timeout acquiring lock for task counters");
                HashMap::new()
            }
        }
    }
    
    /// Clean up stale tasks
    pub async fn clean_stale_tasks(&self) -> usize {
        match tokio::time::timeout(
            Duration::from_secs(TASK_LOCK_TIMEOUT_SECS),
            self.active_tasks.write()
        ).await {
            Ok(mut tasks) => {
                let stale_users: Vec<String> = tasks.iter()
                    .filter(|(_, (timestamp, _))| timestamp.elapsed() > Duration::from_secs(300))
                    .map(|(user_id, _)| user_id.clone())
                    .collect();
                
                let count = stale_users.len();
                for user_id in stale_users {
                    tasks.remove(&user_id);
                    debug!("Removed stale task lock for user {}", user_id);
                }
                count
            },
            Err(_) => {
                warn!("Timeout acquiring write lock for cleaning stale tasks");
                0
            }
        }
    }
    
    pub fn record_sync_time(&self, duration_ms: u32) {
        self.sync_time_total.fetch_add(duration_ms, Ordering::Relaxed);
        self.sync_time_count.fetch_add(1, Ordering::Relaxed);
        
        // Update max sync time if this one was longer
        let current_max = self.sync_time_max.load(Ordering::Relaxed);
        if duration_ms > current_max {
            self.sync_time_max.store(duration_ms, Ordering::Relaxed);
        }
    }
    
    pub fn record_price_fetch_time(&self, duration_ms: u32) {
        self.price_time_total.fetch_add(duration_ms, Ordering::Relaxed);
        self.price_time_count.fetch_add(1, Ordering::Relaxed);
    }
    
    // Method to get average sync time
    pub fn get_avg_sync_time(&self) -> u32 {
        let total = self.sync_time_total.load(Ordering::Relaxed);
        let count = self.sync_time_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0;
        }
        total / count
    }
    
    // Method to get max sync time
    pub fn get_max_sync_time(&self) -> u32 {
        self.sync_time_max.load(Ordering::Relaxed)
    }
    
    // Method to get average price fetch time
    pub fn get_avg_price_fetch_time(&self) -> u32 {
        let total = self.price_time_total.load(Ordering::Relaxed);
        let count = self.price_time_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0;
        }
        total / count
    }
    
    // Extend get_task_counts to include performance metrics
    pub async fn get_performance_metrics(&self) -> HashMap<String, u32> {
        let mut metrics = HashMap::new();
        metrics.insert("avg_sync_time_ms".to_string(), self.get_avg_sync_time());
        metrics.insert("max_sync_time_ms".to_string(), self.get_max_sync_time());
        metrics.insert("avg_price_fetch_time_ms".to_string(), self.get_avg_price_fetch_time());
        metrics
    }
}

/// Scoped task tracking for automatic task cleanup on drop
pub struct ScopedTask<'a> {
    task_manager: &'a UserTaskLock,
    user_id: String,
}

impl<'a> ScopedTask<'a> {
    pub async fn new(task_manager: &'a UserTaskLock, user_id: &str, task_type: &str) -> Option<Self> {
        if task_manager.mark_user_active(user_id, task_type).await {
            Some(Self {
                task_manager,
                user_id: user_id.to_string(),
            })
        } else {
            None
        }
    }
}

impl<'a> Drop for ScopedTask<'a> {
    fn drop(&mut self) {
        // Use blocking to ensure cleanup happens even in async contexts
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.task_manager.mark_user_inactive(&self.user_id).await;
            })
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_user_task_lock() {
        let task_manager = UserTaskLock::new();
        
        // Mark user as active
        assert!(task_manager.mark_user_active("user1", "sync").await);
        
        // Check if user is active
        assert!(task_manager.is_user_active("user1").await);
        
        // Try to mark same user as active again
        assert!(!task_manager.mark_user_active("user1", "sync").await);
        
        // Mark user as inactive
        task_manager.mark_user_inactive("user1").await;
        
        // Check if user is inactive
        assert!(!task_manager.is_user_active("user1").await);
    }
    
    #[tokio::test]
    async fn test_scoped_task() {
        let task_manager = UserTaskLock::new();
        
        {
            let _task = ScopedTask::new(&task_manager, "user2", "sync").await;
            assert!(task_manager.is_user_active("user2").await);
        }
        
        // Task should be cleaned up when it goes out of scope
        assert!(!task_manager.is_user_active("user2").await);
    }
}
