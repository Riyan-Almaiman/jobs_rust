use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cron::Schedule;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::Storage;

pub struct Scheduler {
    storage: Arc<dyn Storage>,
    poll_interval: Duration,
}

impl Scheduler {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            poll_interval: Duration::from_secs(1),
        }
    }

    // /// Set the poll interval
    // pub fn with_poll_interval(mut self, interval: Duration) -> Self {
    //     self.poll_interval = interval;
    //     self
    // }

    pub async fn run(&self, shutdown: CancellationToken) {
        info!("Scheduler started");

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Scheduler shutting down");
                    break;
                }
                _ = self.tick() => {
                    tokio::time::sleep(self.poll_interval).await;
                }
            }
        }
    }

   
    async fn tick(&self) {
        // Process scheduled 
        if let Err(e) = self.process_scheduled_jobs().await {
            error!(error = %e, "Failed to process scheduled jobs");
        }

        // Process recurring jobs
        if let Err(e) = self.process_recurring_jobs().await {
            error!(error = %e, "Failed to process recurring jobs");
        }
    }

    /// Move scheduled jobs that are ready to the enqueued state
    async fn process_scheduled_jobs(&self) -> Result<(), String> {
        let now = Utc::now();
        //enqueue ready jobs
        self.storage
            .enqueue_scheduled_jobs(now)
            .await
            .map_err(|e| e.to_string())
    }

    /// Process recurring jobs that are due
    async fn process_recurring_jobs(&self) -> Result<(), String> {
        let now = Utc::now();
        
        let recurring_jobs = self
            .storage
            .fetch_due_recurring(now)
            .await
            .map_err(|e| e.to_string())?;

        for recurring in recurring_jobs {
            // Calculate next run time 
            let next_run = match Self::calculate_next_run(&recurring.cron) {
                Ok(next) => next,
                Err(e) => {
                    error!(
                        recurring_id = %recurring.id,
                        cron = %recurring.cron,
                        error = %e,
                        "Invalid cron expression"
                    );
                    continue;
                }
            };

            // Attempt to advance the schedule 
            let promoted = match self
                .storage
                .promote_recurring_job(&recurring.id, recurring.next_run, next_run)
                .await
            {
                Ok(success) => success,
                Err(e) => {
                    error!(recurring_id = %recurring.id, error = %e, "Failed to promote recurring job");
                    false
                }
            };

            if promoted {
                debug!(
                    recurring_id = %recurring.id,
                    job_type = %recurring.job_type,
                    "Successfully promoted recurring job"
                );
            } else {
                debug!(recurring_id = %recurring.id, "Recurring job already claimed");
            }
        }

        Ok(())
    }

    /// Calculate the next run time from a cron expression
    pub fn calculate_next_run(cron_expr: &str) -> Result<chrono::DateTime<Utc>, String> {
        let schedule =
            Schedule::from_str(cron_expr).map_err(|e| format!("Invalid cron expression: {}", e))?;

        schedule
            .upcoming(Utc)
            .next()
            .ok_or_else(|| "No upcoming schedule".to_string())
    }
}
