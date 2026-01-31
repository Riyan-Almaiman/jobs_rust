use std::future::Future;
use std::sync::{Arc};
use std::time::Duration;

use chrono::Utc;
use serde::{de::DeserializeOwned, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tokio::sync::Mutex;

use crate::SqliteStorage;
use crate::{EnqueuedJob, Job, JobId, JobState, RecurringJob};
use crate::{BackgroundJob, HandlerRegistry};
use crate::Scheduler;
use crate::Storage;
use crate::Worker;

/// Main job queue that coordinates workers and scheduler
pub struct JobQueue {
    storage: Arc<dyn Storage>,
    handlers: Arc<HandlerRegistry>,
    shutdown: CancellationToken,
    worker_handles: Vec<JoinHandle<()>>,
    scheduler_handle: Mutex<Option<JoinHandle<()>>>,
}

impl JobQueue {
    /// Create a new job queue with the given connection string.
    ///
    /// Workers are not started until you call `start()`.
    /// Register your jobs with `register_background_job()` before calling `start()`.
    ///
    /// # Example
    /// ```ignore
    /// let mut queue = JobQueue::new("sqlite://jobs.db").await?;
    ///
    /// let email_job = queue.register_background_job("send_email", send_email, 3, "default");
    ///
    /// queue.start(2);  // Start 2 workers
    /// ```
    pub async fn new(connection_string: &str) -> Result<Self, String> {
        let storage = SqliteStorage::new(connection_string)
            .await
            .map_err(|e| e.to_string())?;

     

        Ok(JobQueue {
            storage: Arc::new(storage),
            handlers: Arc::new(HandlerRegistry::new()),
            shutdown: CancellationToken::new(),
            worker_handles: Vec::new(),
            scheduler_handle: Mutex::new(None),
        })
    }

    /// Start workers for the default queue.
    ///
    /// Call this after registering all your jobs with `register_background_job()`.
    pub fn start(&mut self, worker_count: usize) {
        self.create_queue("default", worker_count);
    }
    pub fn create_dashboard(&self) -> axum::Router {
        crate::dashboard::router(Arc::clone(&self.storage))
    }
    /// Get a reference to the storage backend
    pub fn storage(&self) -> Arc<dyn Storage> {
        Arc::clone(&self.storage)
    }

    /// Get a reference to the handler registry
    pub fn handlers(&self) -> Arc<HandlerRegistry> {
        Arc::clone(&self.handlers)
    }

    /// Create and register a background job
    ///
    /// This also recovers any stuck jobs of this type from a previous crash.
    ///
    /// # Example
    /// ```ignore
    /// let send_email = queue.register_background_job("send_email", send_email_handler, 3, "default").await;
    /// let critical_job = queue.register_background_job("critical", handler, 5, "critical").await;
    /// ```
    pub async fn register_background_job<T, F, Fut>(
        &self,
        name: &'static str,
        handler: F,
        max_retries: u32,
        queue: &'static str,
    ) -> BackgroundJob<T>
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        // Recover any stuck jobs of this type before registering the handler
        let _ = self.storage.recover_stuck_jobs_by_type(name).await;
        let _ = self.storage.recover_orphaned_continuations_by_type(name).await;

        let job = BackgroundJob::new(name, handler)
            .max_retries(max_retries)
            .queue(queue);
        self.handlers.register(&job);
        job
    }

    /// Enqueue a job, returning an EnqueuedJob for chaining continuations
    pub async fn enqueue<T>(
        &self,
        job_def: &BackgroundJob<T>,
        args: T,
    ) -> Result<EnqueuedJob, String>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
    {
        let payload_bytes = serde_json::to_vec(&args).map_err(|e| e.to_string())?;
        let job = Job::new(job_def.name(), payload_bytes)
            .with_queue(job_def.get_queue())
            .with_max_retries(job_def.get_max_retries());

        let id = self.storage.enqueue(job).await.map_err(|e| e.to_string())?;

        let stored_job = self
            .storage
            .get_job(&id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("Failed to fetch enqueued job")?;

        Ok(EnqueuedJob::new(stored_job, Arc::clone(&self.storage)))
    }

    /// Schedule a job to run after a delay
    pub async fn schedule<T>(
        &mut self,
        job_def: &BackgroundJob<T>,
        args: T,
        delay: Duration,
    ) -> Result<EnqueuedJob, String>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
    {
        let payload_bytes = serde_json::to_vec(&args).map_err(|e| e.to_string())?;
        let run_at = Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default();
        let job = Job::new(job_def.name(), payload_bytes)
            .with_queue(job_def.get_queue())
            .with_max_retries(job_def.get_max_retries())
            .scheduled_at(run_at);
        self.start_scheduler().await;
        let id = self.storage.enqueue(job).await.map_err(|e| e.to_string())?;

        let stored_job = self
            .storage
            .get_job(&id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("Failed to fetch scheduled job")?;

        Ok(EnqueuedJob::new(stored_job, Arc::clone(&self.storage)))
    }

    /// Schedule a job to run at a specific time
    pub async fn schedule_at<T>(
        &self,
        job_def: &BackgroundJob<T>,
        args: T,
        run_at: chrono::DateTime<Utc>,
    ) -> Result<EnqueuedJob, String>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
    {
        self.start_scheduler().await;
        let payload_bytes = serde_json::to_vec(&args).map_err(|e| e.to_string())?;
        let job = Job::new(job_def.name(), payload_bytes)
            .with_queue(job_def.get_queue())
            .with_max_retries(job_def.get_max_retries())
            .scheduled_at(run_at);

        let id = self.storage.enqueue(job).await.map_err(|e| e.to_string())?;

        let stored_job = self
            .storage
            .get_job(&id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("Failed to fetch scheduled job")?;

        Ok(EnqueuedJob::new(stored_job, Arc::clone(&self.storage)))
    }

    /// Add a recurring job with a cron schedule
    pub async fn recurring<T>(
        &self,
        id: &str,
        cron: &str,
        job_def: &BackgroundJob<T>,
        args: T,
    ) -> Result<(), String>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
    {
        self.start_scheduler().await;
        let payload_bytes = serde_json::to_vec(&args).map_err(|e| e.to_string())?;

        let next_run = Scheduler::calculate_next_run(cron)?;

        let recurring = RecurringJob {
            id: id.to_string(),
            cron: cron.to_string(),
            job_type: job_def.name().to_string(),
            payload: payload_bytes,
            queue: job_def.get_queue().to_string(),
            next_run,
            updated_at: Utc::now(),
        };

        self.storage
            .upsert_recurring(recurring)
            .await
            .map_err(|e| e.to_string())
    }

    /// Remove a recurring job
    pub async fn remove_recurring(&self, id: &str) -> Result<(), String> {
        self.storage
            .delete_recurring(id)
            .await
            .map_err(|e| e.to_string())
    }

    /// Retry a failed job
    pub async fn retry(&self, job_id: &JobId) -> Result<(), String> {
        self.storage
            .update_state(job_id, JobState::Enqueued)
            .await
            .map_err(|e| e.to_string())
    }

    /// Delete a job
    pub async fn delete(&self, job_id: &JobId) -> Result<(), String> {
        self.storage
            .delete_job(job_id)
            .await
            .map_err(|e| e.to_string())
    }

    /// Start a worker for the default queue
    pub fn start_worker(&mut self) {
        self.start_worker_for("default");
    }

    /// Start a worker for a specific queue
    pub fn start_worker_for(&mut self, queue: &str) {
        let worker = Worker::new(
            Arc::clone(&self.storage),
            Arc::clone(&self.handlers),
            queue.to_string(),
        );

        let shutdown = self.shutdown.clone();
        let handle = tokio::spawn(async move {
            worker.run(shutdown).await;
        });

        self.worker_handles.push(handle);
        info!(queue = queue, "Started worker");
    }

    /// Start multiple workers for a queue
    pub fn create_queue(&mut self, queue: &str, count: usize) {
        for _ in 0..count {
            self.start_worker_for(queue);
        }
    }

    /// Start the scheduler
    pub async fn start_scheduler(&self) {
        let mut handle_guard = self
            .scheduler_handle
            .lock() 
            .await;

        if handle_guard.is_some() {
            return;
        }

        let scheduler = Scheduler::new(Arc::clone(&self.storage));
        let shutdown = self.shutdown.clone();

        let handle = tokio::spawn(async move {
            scheduler.run(shutdown).await;
        });

        *handle_guard = Some(handle);
        info!("Started scheduler");
    }

    /// Get the shutdown token for external shutdown control
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Trigger graceful shutdown
    pub async fn shutdown(&mut self) {
        info!("Initiating shutdown");
        self.shutdown.cancel();

        for handle in self.worker_handles.drain(..) {
            let _ = handle.await;
        }

        let scheduler_handle = {
            let mut handle_guard = self
                .scheduler_handle
                .lock()
                .await;
            handle_guard.take()
        };

        if let Some(handle) = scheduler_handle {
            let _ = handle.await;
        }

        info!("Shutdown complete");
    }

    /// Wait for shutdown signal (e.g., Ctrl+C)
    pub async fn wait_for_shutdown(&mut self) {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");

        self.shutdown().await;
    }
}
