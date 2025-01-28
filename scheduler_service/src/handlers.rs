use axum::extract::{Json, Path, State};
use scheduler_core::{error::SchedulerError, scheduler::Scheduler, task::{Task, TaskStatus}};
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

pub async fn get_task_by_id(State(scheduler) : State<Arc<Scheduler>> , Path(task_id) : Path<Uuid>) 
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

pub async fn get_all_pending_tasks(State(scheduler) : State<Arc<Scheduler>>) 
-> Result<(StatusCode, Json<Option<Vec<Task>>>),(StatusCode , String)> {


    let res = scheduler.state_store.get_pending_tasks().await;
    
    match res {
        Ok(tasks) => match tasks.len() {
            0 => Ok((StatusCode::OK , Json(None))),
            _ => Ok((StatusCode::OK , Json(Some(tasks))))
        },
        Err(e) => Err((StatusCode::BAD_GATEWAY , e.to_string()))
    }
}

pub async fn update_task(State(scheduler) : State<Arc<Scheduler>>, Path((task_id, status)) : Path<(Uuid,TaskStatus)>) 
-> Result<StatusCode , (StatusCode , String)>{


    let res = scheduler.state_store.update_task(task_id, status).await;

    match res {
        Ok(_) => Ok(StatusCode::OK),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR , e.to_string()))
    }
}