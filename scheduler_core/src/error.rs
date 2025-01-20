use std::{error::Error, fmt::{write, Display}};

#[derive(Debug)]
pub enum SchedulerError {
    TaskNotFound(String),
    DependeciesNotMet(String),
    QueueError(String),
    WorkerError(String),
    StorageError(String)
}

impl Error for SchedulerError {}

impl Display for SchedulerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchedulerError::TaskNotFound(e) => write!(f, "Task not found {}", e),
            SchedulerError::DependeciesNotMet(e) => write!(f, "Dependencies are not met {}", e),
            SchedulerError::QueueError(msg) => write!(f, "Queue error: {}", msg),
            SchedulerError::WorkerError(msg) => write!(f, "Worker error: {}", msg),
            SchedulerError::StorageError(msg) => write!(f, "Storage error: {}", msg),
        }
    }
}