use std::{sync::{Arc,Mutex}, time::Duration};
use scheduler_core::{error::SchedulerError, queue::{rabbitmq::RabbitMQ, InMemoryQueue, MessageQueue}, worker::Worker};
use tokio::time;

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
            if let Some(worker) = workers.iter_mut().min_by_key(|w| w.load) {
                worker.assign_task(task).await?;
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

    let discovery_service = DiscoveryService::new(queue);

    discovery_service.run().await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other , e))?;

    Ok(())
}