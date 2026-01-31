pub mod sqlite;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{DashboardStats, Job, JobId, JobState, RecurringJob};

pub use sqlite::SqliteStorage;

/// Error type for storage operations
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Job not found: {0}")]
    NotFound(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, StorageError>;

#[async_trait]
pub trait Storage: Send + Sync {
    /// Enqueue a new job
    async fn enqueue(&self, job: Job) -> Result<JobId>;

    /// Fetch the next available job from a queue
    async fn fetch_next(&self, queue: &str) -> Result<Option<Job>>;

    /// Update a job's state
    async fn update_state(&self, id: &JobId, state: JobState) -> Result<()>;

    /// Increment retry count and update state
    async fn increment_retries(&self, id: &JobId, state: JobState) -> Result<()>;

    /// Get a job by ID
    async fn get_job(&self, id: &JobId) -> Result<Option<Job>>;

    /// Get jobs by state with limit
    async fn get_jobs_by_state(&self, state: &str, limit: u32) -> Result<Vec<Job>>;

    /// Delete a job
    async fn delete_job(&self, id: &JobId) -> Result<()>;

    /// Move scheduled jobs that are ready to the enqueued state
    async fn enqueue_scheduled_jobs(&self, before: DateTime<Utc>) -> Result<()>;

    /// Get jobs waiting for a parent job to complete
    async fn get_continuation_jobs(&self, parent_id: &JobId) -> Result<Vec<Job>>;

    // Recurring jobs

    /// Insert or update a recurring job
    async fn upsert_recurring(&self, job: RecurringJob) -> Result<()>;

    /// Get recurring jobs that are due to run 
    async fn fetch_due_recurring(&self, now: DateTime<Utc>) -> Result<Vec<RecurringJob>>;

   
    /// Returns 1 if updated, 0 if not 
    async fn update_recurring_schedule(&self, id: &str, current_run: DateTime<Utc>, new_run: DateTime<Utc>) -> Result<u64>;

    /// Get all recurring jobs
    async fn get_all_recurring(&self) -> Result<Vec<RecurringJob>>;

    /// Delete a recurring job
    async fn delete_recurring(&self, id: &str) -> Result<()>;

    // Dashboard

    /// Get dashboard statistics
    async fn get_stats(&self) -> Result<DashboardStats>;

    /// Reset jobs of a specific type stuck in "processing" state back to "enqueued"
    /// Called when a job handler is registered to recover from crashes
    async fn recover_stuck_jobs_by_type(&self, job_type: &str) -> Result<u64>;

    /// Recover orphaned continuation jobs of a specific type where parent succeeded
    async fn recover_orphaned_continuations_by_type(&self, job_type: &str) -> Result<u64>;

    ///  update the recurring job schedule and enqueue the new job instance
    async fn promote_recurring_job(&self, id: &str, current_run: DateTime<Utc>, next_run: DateTime<Utc>) -> Result<bool>;
}
