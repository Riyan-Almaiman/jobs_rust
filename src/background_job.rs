use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde::{Serialize, de::DeserializeOwned};

use crate::job::{Job, JobId, RecurringJob};
use crate::scheduler::Scheduler;
use crate::storage::Storage;

pub struct BackgroundJob<T> {
    pub(crate) name: &'static str,
    pub(crate) max_retries: u32,
    pub(crate) queue: &'static str,
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Send + 'static> BackgroundJob<T> {
    pub async fn enqueue(&self, args: T) -> Result<JobId, String> {
        let payload = serde_json::to_vec(&args).map_err(|e| e.to_string())?;

        let job = Job::new(self.name, payload)
            .with_queue(self.queue)
            .with_max_retries(self.max_retries);

        self.storage.enqueue(job).await.map_err(|e| e.to_string())
    }

    pub async fn schedule(&self, args: T, delay: Duration) -> Result<JobId, String> {
        let payload = serde_json::to_vec(&args).map_err(|e| e.to_string())?;
        let run_at = Utc::now() + chrono::Duration::from_std(delay).unwrap();

        let job = Job::new(self.name, payload)
            .with_queue(self.queue)
            .with_max_retries(self.max_retries)
            .scheduled_at(run_at);

        self.storage.enqueue(job).await.map_err(|e| e.to_string())
    }

    pub async fn recurring(&self, id: &str, cron: &str, args: T) -> Result<(), String> {
        let payload = serde_json::to_vec(&args).map_err(|e| e.to_string())?;
        let next_run = Scheduler::calculate_next_run(cron)?;

        let recurring = RecurringJob {
            id: id.to_string(),
            cron: cron.to_string(),
            job_type: self.name.to_string(),
            payload,
            queue: self.queue.to_string(),
            next_run,
        };

        self.storage.upsert_recurring(recurring).await.map_err(|e| e.to_string())
    }
}
