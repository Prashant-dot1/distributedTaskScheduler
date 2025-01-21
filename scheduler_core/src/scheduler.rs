use crate::{error::SchedulerError, queue::MessageQueue, state::{self, StateStore}, task::{Task, TaskStatus}};
use std::sync::{Arc, RwLock};

pub struct Scheduler {
    queue: Arc<dyn MessageQueue>,
    state_store: Arc<dyn StateStore>,
    running_tasks: Arc<RwLock<Vec<Task>>>
}

impl Scheduler {
    pub fn new(queue: Arc<dyn MessageQueue>, state_store: Arc<dyn StateStore>) -> Self {
        Self { queue, state_store, running_tasks: Arc::new(RwLock::new(Vec::new())) }
    }


    pub async  fn schedule_task(&self, task : Task) -> Result<(), SchedulerError>{
        // we need to store the task in the state storage - metadata
        self.state_store.store_task(&task).await?;

        //check for the dependencies - if all of them are met then proceed
        for dep_id in &task.dependencies {
            let status_dep = self.state_store.get_task(*dep_id).await?;

            if let Some(dep_task) = status_dep {
                match dep_task.status {
                    TaskStatus::Completed { .. } => continue,
                    _ => return Err(SchedulerError::DependeciesNotMet(
                        format!("Dependency with id {} not completed", dep_id)
                    ))
                }
            }
        }

        // we need to publish it to the queue if everything is fine
        self.queue.publish_task(task).await?;
        Ok(())
    }
}