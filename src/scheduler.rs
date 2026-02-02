use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cron::Schedule;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::storage::Storage;

pub(crate) struct Scheduler {
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
        if let Err(e) = self.process_scheduled_jobs().await {
            error!(error = %e, "Failed to process scheduled jobs");
        }

        if let Err(e) = self.process_recurring_jobs().await {
            error!(error = %e, "Failed to process recurring jobs");
        }
    }

    async fn process_scheduled_jobs(&self) -> Result<(), String> {
        self.storage
            .enqueue_scheduled_jobs(Utc::now())
            .await
            .map_err(|e| e.to_string())
    }

    async fn process_recurring_jobs(&self) -> Result<(), String> {
        let recurring_jobs = self.storage
            .fetch_due_recurring(Utc::now())
            .await
            .map_err(|e| e.to_string())?;

        for recurring in recurring_jobs {
            let next_run = match Self::calculate_next_run(&recurring.cron) {
                Ok(next) => next,
                Err(e) => {
                    error!(recurring_id = %recurring.id, error = %e, "Invalid cron");
                    continue;
                }
            };

            let promoted = self.storage
                .promote_recurring_job(&recurring.id, recurring.next_run, next_run)
                .await
                .unwrap_or(false);

            if promoted {
                debug!(recurring_id = %recurring.id, "Promoted recurring job");
            }
        }

        Ok(())
    }

    pub fn calculate_next_run(cron_expr: &str) -> Result<chrono::DateTime<Utc>, String> {
        let schedule = Schedule::from_str(cron_expr)
            .map_err(|e| format!("Invalid cron: {}", e))?;

        schedule
            .upcoming(Utc)
            .next()
            .ok_or_else(|| "No upcoming schedule".to_string())
    }
}
