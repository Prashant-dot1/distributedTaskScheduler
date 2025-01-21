use crate::{error::SchedulerError, task::Task};
use async_trait::async_trait;

#[async_trait]
pub trait MessageQueue {
    async fn publish_task(&self, task: &Task) -> Result<(), SchedulerError>;
    async fn consume_task(&self) -> Result<Option<Task>, SchedulerError>;
}