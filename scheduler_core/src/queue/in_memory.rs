use std::{collections::VecDeque, sync::Mutex};
use crate::task::Task;
use async_trait::async_trait;
use crate::error::SchedulerError;

use super::MessageQueue;

pub struct InMemoryQueue {
    tasks: Mutex<VecDeque<Task>>
}

impl InMemoryQueue {
    pub fn new() -> Self {
        Self { tasks: Mutex::new(VecDeque::new()) }
    }
}

#[async_trait]
impl MessageQueue for InMemoryQueue {

    async fn publish_task(&self, task : Task) -> Result<(), SchedulerError> {
        
        // "need to handle the error logic if we don't get a lock on the vecDequeue"
        let mut lock_on_queue = self.tasks.lock().unwrap();
        lock_on_queue.push_back(task);
        Ok(())
    }

    async fn consume_task(&self) -> Result<Option<Task>, SchedulerError> {
        
        // "need to handle the error logic if we don't get a lock on the vecDequeue"
        let mut lock_on_queue = self.tasks.lock().unwrap();
        Ok(lock_on_queue.pop_front())

    }
}



// tests
#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use std::time::Duration;
    use crate::task::{Schedule, RetryPolicy, TaskStatus};

    // create a task
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
    async fn publish_and_consume_task() {

        let queue = InMemoryQueue::new();
        let task = create_task("task1");
        let task_id = task.id;  // Save the ID before publishing
        
        // publish
        queue.publish_task(task).await.expect("failed to pushlish the task");

        let consumed_task = queue.consume_task().await.expect("failed to consume a task from the queue");
        assert!(consumed_task.is_some(), "Should get something from the queue");

        let received_task = consumed_task.unwrap();
        assert_eq!(received_task.id, task_id, "Task IDs should match");
        assert_eq!(received_task.name, "task1", "Task names should match");
    }

    #[tokio::test]
    async fn concurrent_task_publish() {
        use std::sync::Arc;
        let queue = Arc::new(InMemoryQueue::new());
        let mut handles = vec![];

        for i in 0..5 {
            let queue_clone = Arc::clone(&queue);
            let h1 = tokio::spawn( async move {
                let task = create_task(&format!("task{}", i));
                queue_clone.publish_task(task).await.expect("Failed to publish a task");
            });
            handles.push(h1);
        }

        // await all the async tasks
        for handle in handles {
            handle.await.expect("failed to process the async tasks");
        }

        let mut recievec_tasks = vec![];
        while let Some(task) = queue.consume_task().await.expect("failed to consume a task from the queue") {
            recievec_tasks.push(task);
        }

        assert_eq!(recievec_tasks.len() , 5, "The length should have been equal to 5");


    }
}