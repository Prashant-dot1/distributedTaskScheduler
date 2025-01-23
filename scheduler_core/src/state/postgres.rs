use async_trait::async_trait;
use sqlx::{postgres::PgPoolOptions, PgPool, migrate::MigrateDatabase};
use dotenv::dotenv;

use crate::{error::SchedulerError, task::{Task,TaskStatus}};
use uuid::Uuid;

use super::StateStore;

pub struct PostgresStore {
    pool: PgPool
}

impl PostgresStore {
    pub async fn new(database_url : Option<&str>) ->  Result<PostgresStore, SchedulerError>{
        dotenv().ok();

        let database_url = database_url
            .map(String::from)
            .unwrap_or_else(|| std::env::var("DATABASE_URL")
            .expect("DATABASE URL must be set"));

        if !sqlx::Postgres::database_exists(&database_url).await
            .map_err(|e| SchedulerError::StorageError(e.to_string()))? 
        {
            sqlx::Postgres::create_database(&database_url).await
                .map_err(|e| SchedulerError::StorageError(e.to_string()))?;
        }

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .map_err(|e| SchedulerError::StorageError(e.to_string()))?;

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| SchedulerError::StorageError(e.to_string()))?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl StateStore for PostgresStore {
    async fn store_task(&self, task : &Task) -> Result<(), SchedulerError> {
        sqlx::query(
            "INSERT INTO tasks (
                id, name, payload, schedule, dependencies, 
                status, time_out, retry_policy
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
        )
        .bind(task.id)
        .bind(&task.name)
        .bind(&task.payload)
        .bind(&serde_json::to_value(&task.schedule)
            .map_err(|e| SchedulerError::StorageError(e.to_string()))?)
        .bind(&task.dependencies)
        .bind(&serde_json::to_value(&task.status)
            .map_err(|e| SchedulerError::StorageError(e.to_string()))?)
        .bind(task.time_out.as_millis() as i64)
        .bind(&serde_json::to_value(&task.retry_policy)
            .map_err(|e| SchedulerError::StorageError(e.to_string()))?)
        .execute(&self.pool)
        .await
        .map_err(|e| SchedulerError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn get_task(&self, task_id: Uuid) -> Result<Option<Task> , SchedulerError> {
        todo!()
    }

    async fn update_task(&self, task_id: Uuid, status: TaskStatus) -> Result<(), SchedulerError> {
        todo!()
    }

    async fn get_pending_tasks(&self) -> Result<Vec<Task>, SchedulerError> {
        todo!()
    }
}