use std::sync::Arc;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use scheduler_core::error::SchedulerError;
use scheduler_core::queue::rabbitmq::RabbitMQ;
use scheduler_core::state::PostgresStore;
use scheduler_core::queue::{InMemoryQueue, MessageQueue};
use scheduler_core::scheduler::Scheduler;
use dotenv::dotenv;
use scheduler_core::task::Task;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {

    dotenv().ok();

    let state_store = Arc::new(PostgresStore::new(None).await.map_err(|e| 
        std::io::Error::new(std::io::ErrorKind::Other, e))?);
    
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

    // Create scheduler with concrete implementations
    let scheduler = Arc::new(Scheduler::new(queue, state_store));

    // run the axum server
    // routes for this
    let app = Router::new().
                                    route("/task", post(create_task))
                                    .with_state(scheduler);


    let port = std::env::var("PORT")
                .unwrap_or_else(|_| "8080".to_string());
    
    let addr = format!("0.0.0.0:{}", port).parse::<std::net::SocketAddr>().unwrap();

    println!("Scheduler service running on {}", addr);
    // start 
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await?;

    Ok(())

}


async fn create_task(State(scheduler) : State<Arc<Scheduler>> , Json(task): Json<Task> ) 
 -> Result<(StatusCode, Json<Task>), (StatusCode, String)>{

    let res = scheduler.schedule_task(task.clone()).await;

    match res {
        Ok(_) => Ok((StatusCode::CREATED, Json(task))),
        Err(e) => match e {
            SchedulerError::DependeciesNotMet(_) => {
                Err((StatusCode::BAD_REQUEST, e.to_string()))
            }
            _ => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        }
    }
    
}
