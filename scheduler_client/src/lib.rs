use std::time::Duration;
use thiserror::Error;

use reqwest::{Client, StatusCode};
use uuid::Uuid;
use scheduler_core::{error::SchedulerError, task::{RetryPolicy,Schedule, Task, TaskStatus}};

pub struct ShcedulerClient {
    base_url:String,
    client: Client
}

#[derive(Debug , Error)]
pub enum ClientError {
    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("Scheduler error: {0}")]
    SchedulerError(#[from] SchedulerError),
    
    #[error("Invalid configuration: {0}")]
    ConfigError(String)
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

    pub async fn get_task_status(&self, task_id : Uuid) -> Result<Option<TaskStatus>, ClientError> {

        let response = self.client.get(format!("{}/task/{}", self.base_url ,   task_id))
                    .send()
                    .await
                    .map_err(|_| ClientError::HttpError)?;

        if response.status() == StatusCode::NOT_FOUND {
            Ok(None)
        }

        let task = response.error_for_status()?.json::<Task>().await
                            .map_err(|_| ClientError::SchedulerError)?;
        
        Ok(Some(task.status))

    } 

    pub async fn get_task_by_id(&self, task_id : Uuid) -> Result<Option<Task> , ClientError> {

        let response = self.client.get(format!("{}/task/{}", self.base_url , task_id))
                    .send()
                    .await
                    .map_err(|_| ClientError::HttpError)?;

        if response.status().is_success() {
            let task = response.json::<Task>().await
                    .map_err(|_| ClientError::SchedulerError)?;
            
            Ok(Some(task))
        }
        else {
            Err(ClientError::SchedulerError)
        }
    }

    pub async fn cancel_task(&self, task_id : Uuid) -> Result<bool,ClientError> {
        let response = self.client
                        .delete(format!("{}/task/{}", self.base_url, task_id))
                        .send()
                        .await
                        .map_err(|| ClientError::HttpError)?;
        

        if response.status() == StatusCode::NOT_FOUND {
            Ok(false)
        }

        response.error_for_status()?;
        Ok(true)
    }
}