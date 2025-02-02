use std::{sync::{Arc, Mutex}, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::time::{sleep, timeout, Sleep};
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
                self.retry_without_backoff( attempts ,&task, delay, None, None).await?;
            },
            RetryPolicy::ExponentialBackoff { max_attempts , max_delay ,initial_delay , multiplier } => {
                // TODO: Implement exponential backoff retry logic
                self.retry_without_backoff( max_attempts ,&task, max_delay , Some(initial_delay), Some(multiplier)).await?;
            }
        }
        
        println!("Task {} failed: {}", task.id, error);
        Ok(())
    }


    async fn retry_without_backoff(&self,attempts : &u32 , task : &Task, delay : &Duration, initial_delay: Option<&Duration> , multiplier: Option<&f32>) -> Result<(), SchedulerError> {

        let mut cur_attempts = 1;
        let mut cur_delay: f32 = 1.0;

        if initial_delay.is_some() {
            tokio::time::sleep(*initial_delay.unwrap()).await;
        }

        loop {
            let res = tokio::time::timeout(task.time_out, async {

                tokio::time::sleep(Duration::from_secs(2)).await;
                Ok::<(), SchedulerError>(())
            }).await;

            match res {
                Ok(result) => {
                    match result {
                        Ok(_) => {
                            self.state_store.update_task(task.id, TaskStatus::Completed { 
                                result: serde_json::json!({
                                    "status": "success",
                                    "completed_by": self.id.to_string(),
                                    "completed_at": chrono::Utc::now().to_rfc3339()
                                })
                            }).await?;
                            println!("Task {} completed successfully", task.id);
                            return Ok(())
                        },
                        Err(e) if cur_attempts < *attempts => {
                            println!("Attempts {} failed: {}... retrying..", cur_attempts , e.to_string());


                            self.state_store.update_task(task.id, TaskStatus::Failed { 
                                error: format!("Attempts {} failed: {}... retrying..", cur_attempts , e.to_string()),
                                attempts: cur_attempts
                            }).await?;

                            cur_attempts += 1;

                            if multiplier.is_some() {
                                cur_delay = cur_delay * multiplier.unwrap();
                                tokio::time::sleep( Duration::from_secs_f32(cur_delay)).await;
                            }
                            else {
                                tokio::time::sleep(*delay).await;
                            }
                        },
                        Err(e) => {
                            println!("Attempts exhausted returning error");

                            self.state_store.update_task(task.id, TaskStatus::Failed { 
                                error: format!("All the attempts exhausted: {}", e.to_string()), 
                                attempts: cur_attempts
                            }).await?;

                            return Err(e);
                        }
                    }
                },
                Err(e) => {
                    // Task timed out
                    return Err(SchedulerError::WorkerError(format!("The task execution timed out {}",e.to_string())))
                }
            }

        }
    }

}



