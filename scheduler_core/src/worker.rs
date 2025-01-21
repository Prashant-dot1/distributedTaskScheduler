use std::{sync::Arc, time::Duration};

use uuid::Uuid;

use crate::{error::SchedulerError, queue::MessageQueue, state::StateStore, task::{Task, TaskStatus}};

pub struct Worker {
    id : Uuid,
    queue: Arc<dyn MessageQueue>,
    state_store: Arc<dyn StateStore>
}

impl Worker{
    pub fn new(queue: Arc<dyn MessageQueue>, state_store: Arc<dyn StateStore>) -> Self {
        Self {
            id: Uuid::new_v4(),
            queue,
            state_store
        }
    }

    pub async  fn start(&self) -> Result<(), SchedulerError> {
        // continously consume tasks from the queue
        loop {
            if let Some(task) = self.queue.consume_task().await? {
                // i need to process this
                self.process_task(task).await?;

                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }

    async fn process_task(&self, task: Task) -> Result<(), SchedulerError>{
        // update the status of the task 
        self.state_store.update_task(task.id, TaskStatus::Running { worker_id: self.id.to_string() }).await?;


        // need to have the logic to run the particular task 


        // update if comleted
        self.state_store.update_task(task.id, TaskStatus::Completed { 
            result: serde_json::json!({"status": "succcess"})
        })
        .await?;
        
        Ok(())
    }
}