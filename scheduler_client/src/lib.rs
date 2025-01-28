use std::time::Duration;

use reqwest::Client;
use uuid::Uuid;
use scheduler_core::task::{RetryPolicy,Schedule, Task};

pub struct ShcedulerClient {
    base_url:String,
    client: Client
}

pub enum ClientError {
    HttpError,
    SchedulerError,
    ConfigError
}

impl ShcedulerClient {
    pub fn new(base_url : String) -> Self {
        Self { base_url, client: Client::new() }
    }

    pub async fn schedule_task(&self, name: String, payload: serde_json::Value,
    schedule: Schedule , dependencies: Vec<Uuid> , timeout : Duration, retry_policy : RetryPolicy) -> Result<Uuid,ClientError> {

        let task = Task {
            id: Uuid::new_v4(),
            name,
            payload,
            schedule,
            dependencies,
            status: TaskStatus::Pending,
            time_out: timeout,
            retry_policy,
        };

        let response = self.client
                        .post(format!("{}/task",self.base_url))
                        .json(&task)
                        .send()
                        .await
                        .map_err(|_| ClientError::HttpError)?;

        if response.status().is_success() {
            let task_id = response.json::<Task>().await
                        .map_err(|_| ClientError::SchedulerError)?;
            Ok(task.id)
        }
        else {
            Err(ClientError::SchedulerError)
        }
    }
}