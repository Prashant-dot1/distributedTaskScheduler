use std::sync::Arc;
use scheduler_core::state::PostgresStore;
use scheduler_core::queue::InMemoryQueue;
use scheduler_core::scheduler::Scheduler;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let state_store = Arc::new(PostgresStore::new(None).await.map_err(|e| 
        std::io::Error::new(std::io::ErrorKind::Other, e))?);

    let queue = Arc::new(InMemoryQueue::new());

    // Create scheduler with concrete implementations
    let scheduler = Scheduler::new(queue, state_store);

    println!("Scheduler service started successfully!");

    // Keep the service running
    tokio::signal::ctrl_c().await?;
    println!("Shutting down scheduler service...");

    Ok(())
}