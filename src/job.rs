use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{fmt, sync::Arc};

use crate::BackgroundJob;
use crate::Storage;

/// Unique identifier for a job
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(pub String);

impl JobId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for JobId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl AsRef<str> for JobId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Current state of a job
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JobState {
    Enqueued,
    Processing,
    Succeeded,
    Failed { error: String },
    Scheduled { run_at: DateTime<Utc> },
    Deleted,
}

impl JobState {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobState::Enqueued => "enqueued",
            JobState::Processing => "processing",
            JobState::Succeeded => "succeeded",
            JobState::Failed { .. } => "failed",
            JobState::Scheduled { .. } => "scheduled",
            JobState::Deleted => "deleted",
        }
    }

    pub fn from_db(
        state: &str,
        error: Option<String>,
        run_at: Option<DateTime<Utc>>,
    ) -> Self {
        match state {
            "enqueued" => JobState::Enqueued,
            "processing" => JobState::Processing,
            "succeeded" => JobState::Succeeded,
            "failed" => JobState::Failed {
                error: error.unwrap_or_default(),
            },
            "scheduled" => JobState::Scheduled {
                run_at: run_at.unwrap_or_else(Utc::now),
            },
            "deleted" => JobState::Deleted,
            _ => JobState::Enqueued,
        }
    }
}

/// A background job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub queue: String,
    pub job_type: String,
    pub payload: Vec<u8>,
    pub state: JobState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub max_retries: u32,
    pub retries: u32,
    pub parent_id: Option<JobId>,
}

impl Job {
    pub fn new<S: Into<String>>(job_type: S, payload: Vec<u8>) -> Self {
        let now = Utc::now();
        Self {
            id: JobId::new(),
            queue: "default".to_string(),
            job_type: job_type.into(),
            payload,
            state: JobState::Enqueued,
            created_at: now,
            updated_at: now,
            max_retries: 3,
            retries: 0,
            parent_id: None,
        }
    }

    pub fn with_queue<S: Into<String>>(mut self, queue: S) -> Self {
        self.queue = queue.into();
        self
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn with_parent(mut self, parent_id: JobId) -> Self {
        self.parent_id = Some(parent_id);
        self
    }

    pub fn scheduled_at(mut self, run_at: DateTime<Utc>) -> Self {
        self.state = JobState::Scheduled { run_at };
        self
    }
}

/// A job that has been enqueued, with access to storage for chaining
pub struct EnqueuedJob {
    pub job: Job,
    storage: Arc<dyn Storage>,
}

impl EnqueuedJob {
    pub fn new(job: Job, storage: Arc<dyn Storage>) -> Self {
        Self { job, storage }
    }

    pub fn id(&self) -> &JobId {
        &self.job.id
    }

    pub async fn continue_with<T>(
        &self,
        job_def: &BackgroundJob<T>,
        args: T,
    ) -> Result<EnqueuedJob, String>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
    {
        let payload_bytes = serde_json::to_vec(&args).map_err(|e| e.to_string())?;

        let stored_job = Job::new(job_def.name(), payload_bytes)
            .with_queue(job_def.get_queue())
            .with_max_retries(job_def.get_max_retries())
            .with_parent(self.job.id.clone())
            .scheduled_at(Utc::now());

        let id = self
            .storage
            .enqueue(stored_job)
            .await
            .map_err(|e| e.to_string())?;

        let job = self
            .storage
            .get_job(&id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("Failed to fetch continuation job")?;

        Ok(EnqueuedJob::new(job, Arc::clone(&self.storage)))
    }
}

impl std::ops::Deref for EnqueuedJob {
    type Target = Job;

    fn deref(&self) -> &Self::Target {
        &self.job
    }
}

impl fmt::Display for EnqueuedJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.job.id)
    }
}

/// A recurring job definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecurringJob {
    pub id: String,
    pub cron: String,
    pub job_type: String,
    pub payload: Vec<u8>,
    pub queue: String,
    pub next_run: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl RecurringJob {
    pub fn new<S1, S2>(id: S1, cron: S2, job_type: S1, payload: Vec<u8>) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Self {
            id: id.into(),
            cron: cron.into(),
            job_type: job_type.into(),
            payload,
            queue: "default".to_string(),
            next_run: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    pub fn with_queue<S: Into<String>>(mut self, queue: S) -> Self {
        self.queue = queue.into();
        self
    }
}

/// Statistics for the dashboard
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DashboardStats {
    pub enqueued: u64,
    pub processing: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub scheduled: u64,
    pub recurring: u64,
}
