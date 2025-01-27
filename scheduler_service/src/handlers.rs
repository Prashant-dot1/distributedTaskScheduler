use scheduler_core::{error::SchedulerError, scheduler::Scheduler, task::Task};
use axum::extract::{State, Json,Path};
use std::sync::Arc;
use axum::http::status::StatusCode;
use uuid::Uuid;


pub async fn create_task(State(scheduler) : State<Arc<Scheduler>> , Json(task): Json<Task> ) 
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

pub async fn get_tasks(State(scheduler) : State<Arc<Scheduler>> , Path(task_id) : Path<Uuid>) 
-> Result<(StatusCode, Json<Option<Task>>) , (StatusCode , String)> {


    let res = scheduler.state_store.get_task(task_id)
            .await;

    match res {
        Ok(Some(task)) => Ok((StatusCode::OK, Json(Some(task)))),
        Ok(None) => Ok((StatusCode::OK , Json(None))),
        Err(e) => {
            match e {
                SchedulerError::TaskNotFound(e) => Err((StatusCode::BAD_REQUEST , e.to_string())),
                _ => Err((StatusCode::INTERNAL_SERVER_ERROR , e.to_string()))
            }
        }
    }
}