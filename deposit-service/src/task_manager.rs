// deposit-service/src/task_manager.rs
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct ActiveTasksManager {
    active_tasks: Arc<Mutex<HashMap<String, bool>>>,
}

impl ActiveTasksManager {
    pub fn new() -> Self {
        Self {
            active_tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

// Fix methods
pub async fn is_user_active(&self, user_id: &str) -> bool {
    self.active_tasks.lock().await.contains_key(user_id)
}

pub async fn mark_user_active(&self, user_id: &str) {
    self.active_tasks.lock().await.insert(user_id.to_string(), true);
}

pub async fn mark_user_inactive(&self, user_id: &str) {
    self.active_tasks.lock().await.remove(user_id);
}
}
