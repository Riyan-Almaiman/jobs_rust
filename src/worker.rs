use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{Job, JobState};
use crate::registry::{HandlerRegistry, JobError};
use crate::Storage;


/// Worker that processes jobs from a queue
pub struct Worker {
    storage: Arc<dyn Storage>,
    handlers: Arc<HandlerRegistry>,
    queue: String,
    poll_interval: Duration,
}

impl Worker {
    /// Create a new worker for the specified queue
    pub fn new(
        storage: Arc<dyn Storage>,
        handlers: Arc<HandlerRegistry>,
        queue: String,
    ) -> Self {
        Self {
            storage,
            handlers,
            queue,
            poll_interval: Duration::from_millis(500),
        }
    }

    
    // /// Set the poll interval for checking new jobs
    // pub fn with_poll_interval(mut self, interval: Duration) -> Self {
    //     self.poll_interval = interval;
    //     self
    // }

    /// Run the worker until shutdown is signaled
    pub async fn run(&self, shutdown: CancellationToken) {
        info!(queue = %self.queue, "Worker started");

        loop {
            tokio::select! {
                
                _ = shutdown.cancelled() => {
                    info!(queue = %self.queue, "Worker shutting down");
                    break;
                }
                _ = self.poll_and_process() => {}
            }
        }
    }

    /// Poll for the next job and process it
    async fn poll_and_process(&self) {
        match self.storage.fetch_next(&self.queue).await {
            Ok(Some(job)) => {
                debug!(job_id = %job.id, job_type = %job.job_type, "Processing job");

                // Execute the job
                match self.handlers.execute(&job).await {
                    Ok(()) => {
                        info!(job_id = %job.id, job_type = %job.job_type, "Job succeeded");

                        if let Err(e) = self
                            .storage
                            .update_state(&job.id, JobState::Succeeded)
                            .await
                        {
                            error!(job_id = %job.id, error = %e, "Failed to update job state to succeeded");
                        }

                        // Trigger continuation jobs
                        if let Err(e) = self.trigger_continuations(&job).await {
                            error!(job_id = %job.id, error = %e, "Failed to trigger continuations");
                        }
                    }
                    Err(e) => {
                        warn!(job_id = %job.id, job_type = %job.job_type, error = %e, "Job failed");

                        // Don't retry if handler doesn't exist 
                        let should_retry = !matches!(e, JobError::HandlerNotFound(_));

                        if let Err(e) = self.handle_failure(&job, e.to_string(), should_retry).await {
                            error!(job_id = %job.id, error = %e, "Failed to handle job failure");
                        }
                    }
                }
            }
            Ok(None) => {
                // No jobs available
                tokio::time::sleep(self.poll_interval).await;
            }
            Err(e) => {
                error!(queue = %self.queue, error = %e, "Failed to fetch next job");
                tokio::time::sleep(self.poll_interval).await;
            }
        }
    }

    /// Handle a job failure with retry logic
    async fn handle_failure(&self, job: &Job, error: String, should_retry: bool) -> Result<(), String> {
        let new_retries = job.retries + 1;

        if should_retry && new_retries <= job.max_retries {
            info!(
                job_id = %job.id,
                retries = new_retries,
                max_retries = job.max_retries,
                "Scheduling job for retry"
            );

            self.storage
                .increment_retries(&job.id, JobState::Enqueued)
                .await
                .map_err(|e| e.to_string())?;
        } else {
            // Max retries exceeded
            warn!(
                job_id = %job.id,
                retries = new_retries,
                "Job failed permanently"
            );

            self.storage
                .increment_retries(&job.id, JobState::Failed { error })
                .await
                .map_err(|e| e.to_string())?;
        }

        Ok(())
    }

    /// Trigger jobs that are waiting for this job to complete
    async fn trigger_continuations(&self, parent_job: &Job) -> Result<(), String> {
        let continuations = self
            .storage
            .get_continuation_jobs(&parent_job.id)
            .await
            .map_err(|e| e.to_string())?;

        for job in continuations {
            debug!(
                job_id = %job.id,
                parent_id = %parent_job.id,
                "Triggering continuation job"
            );

            self.storage
                .update_state(&job.id, JobState::Enqueued)
                .await
                .map_err(|e| e.to_string())?;
        }

        Ok(())
    }
}
