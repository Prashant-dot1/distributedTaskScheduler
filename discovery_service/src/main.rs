use std::{sync::{Arc,Mutex}, time::Duration};
use scheduler_core::{
    error::SchedulerError, 
    queue::{rabbitmq::RabbitMQ, InMemoryQueue, MessageQueue}, 
    worker::{Worker, WorkerStatus}
};
use tokio::time;
use axum::{
    routing::post,
    Router,
    Json,
    extract::State
};
use serde::Deserialize;
use uuid::Uuid;
use reqwest;

// Make DiscoveryService clonable by wrapping internal state in Arc
#[derive(Clone)]
struct DiscoveryService {
    workers: Arc<Mutex<Vec<Worker>>>,
    task_queue: Arc<dyn MessageQueue>
}

impl DiscoveryService {

    pub fn new(queue : Arc<dyn MessageQueue>) -> Self {
        Self {
            workers: Arc::new(Mutex::new(Vec::new())),
            task_queue: queue
        }
    }

    pub fn add_worker(&self, worker: Worker) {
        let mut workers = self.workers.lock().unwrap();
        workers.push(worker);
    }
    

    // this is to delegate the task to the worker nodes
    pub async fn delegate_task(&self) -> Result<(), SchedulerError>{
        let mut workers = self.workers.lock().unwrap();

        if let Some(task) = self.task_queue.consume_task().await? {
            if let Some(worker) = workers.iter_mut()
                .min_by_key(|w| w.load) {
                
                // Send task assignment to worker via HTTP
                let client = reqwest::Client::new();
                let worker_url = format!("http://{}:3001/task", worker.address);

                client.post(&worker_url)
                    .json(&task)
                    .send()
                    .await
                    .map_err(|e| SchedulerError::WorkerError(e.to_string()))?;

                worker.status = WorkerStatus::Busy;
                worker.load += 1;
            }
        } 

        Ok(())
    }

    pub async fn run(&self) -> Result<(), SchedulerError>{
        
        let mut interval = time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            self.delegate_task().await?; // every 5 sec the discovery service tries to delegate task to worker
        }
    }

    pub async fn update_worker_status(&self, worker_id: Uuid, status: WorkerStatus) -> Result<(), SchedulerError> {
        let mut workers = self.workers.lock().unwrap();
        if let Some(worker) = workers.iter_mut().find(|w| w.id == worker_id) {
            worker.status = status;
            Ok(())
        } else {
            Err(SchedulerError::WorkerError("Worker not found".to_string()))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error>{


    let queue_type = std::env::var("QUEUE_TYPE");

    let queue = match queue_type.unwrap_or_else(|_ | "memory".to_string()).as_str() {
        "memory" => Arc::new(InMemoryQueue::new()) as Arc<dyn MessageQueue + Send + Sync>,
        "rabbitmq" => {
            let url = std::env::var("RABBITMQ_URL").expect("This should be set if you want to use the rabbitmq service");

            Arc::new(RabbitMQ::new(&url).await
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other,e))?
                    )
        },
        _ =>  panic!("The queue type value should be provided")
    };

    let discovery_service = Arc::new(DiscoveryService::new(queue));
    
    let service_clone = discovery_service.clone();
    tokio::spawn(async move {
        if let Err(e) = service_clone.run().await {
            eprintln!("Discovery service error: {}", e);
        }
    });

    
    let app = Router::new()
        .route("/heartbeat", post(process_heartbeat))
        .with_state(discovery_service);

    println!("Discovery service listening on 0.0.0.0:3000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn process_heartbeat(
    State(discovery_service): State<Arc<DiscoveryService>>,
    Json(payload): Json<HeartbeatPayload>
) -> Result<(), SchedulerError> {

    discovery_service.update_worker_status(payload.worker_id, payload.status).await?;
}


#[derive(Deserialize)]
struct HeartbeatPayload {
    worker_id: Uuid,
    status: WorkerStatus
}