pub mod sqlite;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::job::{DashboardStats, Job, JobId, JobState, RecurringJob};

pub use sqlite::SqliteStorage;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

pub type Result<T> = std::result::Result<T, StorageError>;

#[async_trait]
pub(crate) trait Storage: Send + Sync {
    async fn enqueue(&self, job: Job) -> Result<JobId>;
    async fn fetch_next(&self, queue: &str) -> Result<Option<Job>>;
    async fn update_state(&self, id: &JobId, state: JobState) -> Result<()>;
    async fn increment_retries(&self, id: &JobId, state: JobState) -> Result<()>;
    async fn get_job(&self, id: &JobId) -> Result<Option<Job>>;
    async fn get_jobs_by_state(&self, state: &str, limit: u32) -> Result<Vec<Job>>;
    async fn delete_job(&self, id: &JobId) -> Result<()>;
    async fn enqueue_scheduled_jobs(&self, before: DateTime<Utc>) -> Result<()>;
    async fn upsert_recurring(&self, job: RecurringJob) -> Result<()>;
    async fn fetch_due_recurring(&self, now: DateTime<Utc>) -> Result<Vec<RecurringJob>>;
    async fn promote_recurring_job(&self, id: &str, current_run: DateTime<Utc>, next_run: DateTime<Utc>) -> Result<bool>;
    async fn get_all_recurring(&self) -> Result<Vec<RecurringJob>>;
    async fn get_stats(&self) -> Result<DashboardStats>;
}
