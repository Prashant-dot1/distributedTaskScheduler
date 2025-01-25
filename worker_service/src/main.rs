use dotenv::dotenv;
use scheduler_core::{queue::{self, rabbitmq::RabbitMQ, InMemoryQueue, MessageQueue}, state::PostgresStore, worker::Worker};

use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(),std::io::Error> {

    dotenv().ok();

    let state_store = Arc::new(PostgresStore::new(None).await.expect("Failed to get the db store"));

    // queue
    let queue_type = std::env::var("QUEUE_TYPE");
    let queue = match queue_type.unwrap_or_else(| _ | "memory".to_string()).as_str() {
        "memory" => Arc::new(InMemoryQueue::new()) as Arc<dyn MessageQueue>,
        "rabbitmq" => {
            let url = std::env::var("RABBITMQ_URL")
            .expect("RABBITMQ_URL - should be set to create the queue");
            

            Arc::new(RabbitMQ::new(&url).await.map_err(|e| 
                std::io::Error::new(std::io::ErrorKind::Other,e)
            )?)
        },
        _ => panic!("Unknown queue type")
    };

    let worker = Worker::new(queue, state_store);

    println!("worker service running successfully,Worker ID: {}",worker.id);

    worker.start().await.map_err(|e| 
        std::io::Error::new(std::io::ErrorKind::Other, e)    
    )?;

    Ok(())
}
