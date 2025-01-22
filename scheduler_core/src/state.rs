use crate::{error::SchedulerError, task::Task, task::TaskStatus};
use async_trait::async_trait;
use uuid::Uuid;

pub mod in_memory_store;

#[async_trait]
pub trait StateStore {
    async fn store_task(&self, task : &Task) -> Result<(), SchedulerError>;
    async fn get_task(&self, task_id: Uuid) -> Result<Option<Task> , SchedulerError>;
    async fn update_task(&self, task_id: Uuid, status: TaskStatus) -> Result<(), SchedulerError>;
    async fn get_pending_tasks(&self) -> Result<Vec<Task>, SchedulerError>;
}