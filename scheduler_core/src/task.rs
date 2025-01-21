use std::time::Duration;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: Uuid,
    pub name: String, 
    pub schedule: Schedule,
    pub dependencies: Vec<Uuid>,
    pub status: TaskStatus,
    pub time_out: Duration,
    pub retry_policy: RetryPolicy    
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Schedule {
    Once(chrono::DateTime<chrono::Utc>),
    Recurring(RecurringSchedule)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecurringSchedule {
    Daily {
        hour: u32,
        minute: u32
    },
    Weekly {
        day: chrono::Weekday,
        hour: u32,
        minute: u32
    },
    Cron(String)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Running {
        worker_id : String
    },
    Completed{
        result: serde_json::Value
    },
    Failed {
        error: String, 
        attempts: u32
    }

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetryPolicy {
    Noretry,
    Failed {
        attempts: u32,
        delay: Duration
    },
    ExponentialBackoff {
        max_attempts: u32,
        initial_delay: Duration,
        multiplier: f32,
        max_delay: Duration
    }
}
