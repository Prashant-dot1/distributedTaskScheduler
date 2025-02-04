use dotenv::dotenv;
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;
use std::{sync::Arc, time::Duration};
use scheduler_core::{
    error::SchedulerError, queue::{rabbitmq::RabbitMQ, InMemoryQueue, MessageQueue}, state::PostgresStore, worker::{Worker, WorkerStatus}
};
use reqwest::Client;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use axum::{
    extract::State, http::StatusCode, routing::post, Json, Router
};
use std::net::SocketAddr;

#[derive(Serialize)]
struct HeartbeatPayload {
    worker_id: Uuid,
    status: WorkerStatus
}

#[derive(Clone)]
pub struct WorkerHandle {
    inner: Arc<Worker>
}

impl WorkerHandle {
    fn new(worker: Arc<Worker>) -> Self {
        Self { inner: worker.clone() }
    }

    pub async fn send_heartbeat(&self, discovery_service_url: &str) -> Result<(), SchedulerError> {
        let client = Client::new();
        let payload = {
            
            let worker_state = {
                let guard = self.inner.inner.lock()
                            .map_err(|e| {
                                error!("Failed to acquire lock on the worker state {}", e);
                                SchedulerError::WorkerError(format!("Failed to acquire a lock: {}", e.to_string()))
                            })?;

                guard.status.clone()
            };

            HeartbeatPayload {
                worker_id: self.inner.id,
                status: worker_state
            }
        };


        debug!("Sending heartbeat for worker {}", self.inner.id);

        client.post(format!("{}/heartbeat", discovery_service_url))
            .json(&payload)
            .send()
            .await
            .map_err(|e| SchedulerError::WorkerError(e.to_string()))?;


        debug!("Heartbeat sent");
        Ok(())
    }

    pub async fn handle_task_assignment(&self, task: TaskAssignment) -> Result<(), SchedulerError> {
        // Process the assigned task
        // need to look into this logic process the task
        self.inner.start(task.task_id).await?;

        Ok(())
    }
}

#[derive(Deserialize, Debug)]
pub struct TaskAssignment {
    task_id: Uuid,
    // Add other task-related fields
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    dotenv().ok();


    tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env().add_directive("worker_service=info".parse().unwrap())).init();

    info!("Intialising worker service....");

    let state_store = Arc::new(PostgresStore::new(None).await.map_err(|e| {
       error!("Failed to initialised the database connection..");
       std::io::Error::new(std::io::ErrorKind::Other , e)
    })?);

    let discovery_service_url = std::env::var("DISCOVERY_SERVICE_URL")
        .map_err(|e| {
            error!("DISCOVERY_SERVICE_URL env variable is not set");
            std::io::Error::new(std::io::ErrorKind::Other , e)
        })?;

    let worker = Arc::new(Worker::new(state_store));
    info!("Created a worker handler with ID : {}", worker.id);
    let worker_handle = WorkerHandle::new(worker.clone());

    // Periodically send heartbeat
    let worker_handle_clone = worker_handle.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            if let Err(e) = worker_handle_clone.send_heartbeat(&discovery_service_url).await {
                error!("Failed to send heartbeat {}", e);
            }
        }
    });

    // Set up HTTP server
    let app = Router::new()
        .route("/task", post(handle_task_assignment))
        .with_state(worker_handle);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3001));
    info!("Worker service initialised successfully..");
    info!("Listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener,app.into_make_service())
        .await?;

    Ok(())
}



/* handler for accepting task from the discovery service */

#[axum::debug_handler]
pub async fn handle_task_assignment(
    State(worker_handle): State<WorkerHandle>,
    Json(task): Json<TaskAssignment>,
) -> Result<StatusCode, (StatusCode, String)> {

    info!("Received a task assignment with task id {}", task.task_id);

    let res = worker_handle.handle_task_assignment(task).await;

    match res {
        Ok(_) => 
        {
            info!("Successfully processed the task");
            Ok(StatusCode::OK)
        },
        Err(e) => {
            error!("Failed to process the task: {}", e);
            Err((StatusCode::BAD_REQUEST , e.to_string()))
        }
    }
    
}