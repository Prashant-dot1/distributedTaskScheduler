use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{error::SchedulerError, state::StateStore, task::{RetryPolicy, Task, TaskStatus}};

pub struct Worker {
    pub id: Uuid,
    pub state_store: Arc<dyn StateStore>,
    pub inner: Mutex<WorkerState>
}

pub struct WorkerState {
    pub load: usize,
    pub status: WorkerStatus,
}

#[derive(Clone,Serialize)]
pub enum WorkerStatus {
    Idle,
    Busy,
    Offline,
}

impl Worker{
    pub fn new(state_store: Arc<dyn StateStore>) -> Self {
        Self {
            id: Uuid::new_v4(),
            state_store,
            inner: Mutex::new(WorkerState {
                load: 0,
                status: WorkerStatus::Idle
            })
        }
    }

    pub async fn start(&self, task_id: Uuid) -> Result<(), SchedulerError> {
        self.process_assigned_task(task_id).await
    }

    async fn process_assigned_task(&self, task_id: Uuid) -> Result<(), SchedulerError> {
        let task = self.state_store.get_task(task_id).await?;
        
        if let Some(task) = task {

            {
                let mut worker_state = self.inner.lock().unwrap();
                worker_state.load += 1;
                worker_state.status = WorkerStatus::Busy;
            }

            self.process_task(task).await?;

            {
                let mut worker_state = self.inner.lock().unwrap();
                worker_state.load -= 1;
                if worker_state.load == 0 {
                    worker_state.status = WorkerStatus::Idle;
                }
            }
        }

        Ok(())
    }

    async fn process_task(&self, task: Task) -> Result<(), SchedulerError> {
        println!("Processing task: {} (ID: {})", task.name, task.id);
        
        // Update task status to Running
        self.state_store.update_task(task.id, TaskStatus::Running { 
            worker_id: self.id.to_string() 
        }).await?;

        // Set up timeout for the task
        let timeout_duration = task.time_out;
        
        // Simulate task processing with timeout
        match tokio::time::timeout(timeout_duration, async {
            // TODO: Replace this with actual task processing logic
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            Ok::<(), SchedulerError>(())
        }).await {
            Ok(result) => {
                match result {
                    Ok(_) => {
                        // Task completed successfully
                        self.state_store.update_task(task.id, TaskStatus::Completed { 
                            result: serde_json::json!({
                                "status": "success",
                                "completed_by": self.id.to_string(),
                                "completed_at": chrono::Utc::now().to_rfc3339()
                            })
                        }).await?;
                        println!("Task {} completed successfully", task.id);
                    },
                    Err(e) => {
                        // Task failed during processing
                        self.handle_task_failure(task, e).await?;
                    }
                }
            },
            Err(_) => {
                // Task timed out
                self.handle_task_failure(
                    task, 
                    SchedulerError::WorkerError("Task execution timed out".to_string())
                ).await?;
            }
        }

        Ok(())
    }

    async fn handle_task_failure(&self, task: Task, error: SchedulerError) -> Result<(), SchedulerError> {
        match &task.retry_policy {
            RetryPolicy::NoRetry => {
                self.state_store.update_task(task.id, TaskStatus::Failed { 
                    error: error.to_string(), 
                    attempts: 1 
                }).await?;
            },
            RetryPolicy::Failed { attempts, delay } => {
                // TODO: Implement retry logic
                
                // marking as failed for now
                self.state_store.update_task(task.id, TaskStatus::Failed { 
                    error: error.to_string(), 
                    attempts: 1 
                }).await?;
            },
            RetryPolicy::ExponentialBackoff { .. } => {
                // TODO: Implement exponential backoff retry logic
                
                // marking as failed for now
                self.state_store.update_task(task.id, TaskStatus::Failed { 
                    error: error.to_string(), 
                    attempts: 1 
                }).await?;
            }
        }
        
        println!("Task {} failed: {}", task.id, error);
        Ok(())
    }

}