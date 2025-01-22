use std::{clone, collections::HashMap, ptr::hash};

use tokio::sync::RwLock;
use uuid::Uuid;
use async_trait::async_trait;

use crate::{error::SchedulerError, task::Task, task::TaskStatus};

use super::StateStore;


pub struct InMemoryStore {
    tasks: RwLock<HashMap<Uuid, Task>>
}


impl InMemoryStore {
    fn new() -> Self {
        Self { tasks: RwLock::new(HashMap::new()) }
    }
}

#[async_trait]
impl StateStore for InMemoryStore {

    async fn store_task(&self, task : &Task) -> Result<(), SchedulerError> {
        
        let mut exclusive_hashmap_access = self.tasks.write().await;
        exclusive_hashmap_access.insert(task.id, task.clone());
        Ok(())
    }

    async fn get_task(&self, task_id: Uuid) -> Result<Option<Task> , SchedulerError> {

        let hashmap = self.tasks.read().await;
        let task = hashmap.get(&task_id);

        Ok(task.cloned())
    }

    async fn update_task(&self, task_id: Uuid, status: TaskStatus) -> Result<(), SchedulerError> {

        let mut hashmap = self.tasks.write().await;
        
        if let Some(task) = hashmap.get_mut(&task_id) {
            task.status = status;
            return Ok(())
        }
        else {
            return Err(SchedulerError::StorageError(format!("Unable to update the task with id {}", task_id)));
        }
    }

    async fn get_pending_tasks(&self) -> Result<Vec<Task>, SchedulerError> {
        
        let hashmap = self.tasks.read().await;

        let pending_tasks = hashmap
                            .values()
                            .filter(|t| { t.status == TaskStatus::Pending })
                            .cloned()
                            .collect();

        Ok(pending_tasks)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::task::{Schedule, RetryPolicy};
    use std::time::Duration;

    fn create_task(name: &str) -> Task {
        Task {
            id: Uuid::new_v4(),
            name: name.to_string(),
            payload: serde_json::json!({"test": "data"}),
            schedule: Schedule::Once(chrono::Utc::now()),
            dependencies: vec![],
            retry_policy: RetryPolicy::NoRetry,
            time_out: Duration::from_secs(60),
            status: TaskStatus::Pending
        }
    }

    #[tokio::test]
    async fn insert_task_hashmap() {

        let store = InMemoryStore::new();

        let task = create_task("task1");
        
        store.store_task(&task).await.expect("Should insert the task to the hashmap");

        assert_eq!(store.tasks.read().await.len(), 1, "Length should be 1 here");
        
    }

    #[tokio::test]
    async fn concurrent_inserts() {
        use std::sync::Arc;

        let store = Arc::new(InMemoryStore::new());
        let mut handles = vec![];

        for i in 0..5 {

            let store_clone = store.clone();

            let handle = tokio::spawn(async move{
                let task = create_task(&format!("task{}", i));
                store_clone.store_task(&task).await.expect("Task should have been stored");
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("Should join all the aync tasks");
        }

        assert_eq!(store.tasks.read().await.len(), 5, "5 tasks should be stored in this hashmap");
    }
}


