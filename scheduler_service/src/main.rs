use std::sync::Arc;
use scheduler_core::queue::rabbitmq::RabbitMQ;
use scheduler_core::state::PostgresStore;
use scheduler_core::queue::{InMemoryQueue, MessageQueue};
use scheduler_core::scheduler::Scheduler;
use dotenv::dotenv;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {

    dotenv().ok();

    let state_store = Arc::new(PostgresStore::new(None).await.map_err(|e| 
        std::io::Error::new(std::io::ErrorKind::Other, e))?);
    
    let queue_type = std::env::var("QUEUE_TYPE");

    let queue = match queue_type.unwrap_or_else(|_ | "memory".to_string()).as_str() {
        "memory" => Arc::new(InMemoryQueue::new()) as Arc<dyn MessageQueue>,
        "rabbitmq" => {
            let url = std::env::var("RABBITMQ_URL").expect("This should be set if you want to use the rabbitmq service");

            Arc::new(RabbitMQ::new(&url).await
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other,e))?
                    )
        },
        _ =>  panic!("The message type value is not supported")
    };

    // Create scheduler with concrete implementations
    let _scheduler = Scheduler::new(queue, state_store);

    println!("Scheduler service started successfully!");

    // Keep the service running
    tokio::signal::ctrl_c().await?;
    println!("Shutting down scheduler service...");

    Ok(())
}