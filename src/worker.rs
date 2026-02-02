use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::handler::{HandlerRegistry, JobError};
use crate::job::{Job, JobState};
use crate::storage::Storage;

pub(crate) struct Worker {
    storage: Arc<dyn Storage>,
    handlers: Arc<HandlerRegistry>,
    queue: String,
    poll_interval: Duration,
}

impl Worker {
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

    async fn poll_and_process(&self) {
        match self.storage.fetch_next(&self.queue).await {
            Ok(Some(job)) => {
                debug!(job_id = %job.id, job_type = %job.job_type, "Processing job");
                self.process_job(job).await;
            }
            Ok(None) => {
                tokio::time::sleep(self.poll_interval).await;
            }
            Err(e) => {
                error!(queue = %self.queue, error = %e, "Failed to fetch job");
                tokio::time::sleep(self.poll_interval).await;
            }
        }
    }

    async fn process_job(&self, job: Job) {
        match self.handlers.execute(&job).await {
            Ok(()) => {
                info!(job_id = %job.id, "Job succeeded");

                if let Err(e) = self.storage.update_state(&job.id, JobState::Succeeded).await {
                    error!(job_id = %job.id, error = %e, "Failed to update state");
                }
            }
            Err(e) => {
                warn!(job_id = %job.id, error = %e, "Job failed");

                let should_retry = !matches!(e, JobError::HandlerNotFound(_));
                if let Err(e) = self.handle_failure(&job, e.to_string(), should_retry).await {
                    error!(job_id = %job.id, error = %e, "Failed to handle failure");
                }
            }
        }
    }

    async fn handle_failure(&self, job: &Job, error: String, should_retry: bool) -> Result<(), String> {
        let new_retries = job.retries + 1;

        if should_retry && new_retries <= job.max_retries {
            info!(job_id = %job.id, retries = new_retries, "Retrying job");
            self.storage
                .increment_retries(&job.id, JobState::Enqueued)
                .await
                .map_err(|e| e.to_string())
        } else {
            warn!(job_id = %job.id, "Job failed permanently");
            self.storage
                .increment_retries(&job.id, JobState::Failed { error })
                .await
                .map_err(|e| e.to_string())
        }
    }

}
