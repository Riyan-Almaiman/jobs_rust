use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(pub(crate) String);

#[derive(Debug, Clone)]
pub struct JobInfo {
    pub id: JobId,
    pub job_type: String,
    pub queue: String,
    pub status: String,
    pub error: Option<String>,
    pub retries: u32,
    pub max_retries: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<Job> for JobInfo {
    fn from(job: Job) -> Self {
        let (status, error) = match &job.state {
            JobState::Enqueued => ("enqueued".to_string(), None),
            JobState::Processing => ("processing".to_string(), None),
            JobState::Succeeded => ("succeeded".to_string(), None),
            JobState::Failed { error } => ("failed".to_string(), Some(error.clone())),
            JobState::Scheduled { .. } => ("scheduled".to_string(), None),
        };

        Self {
            id: job.id,
            job_type: job.job_type,
            queue: job.queue,
            status,
            error,
            retries: job.retries,
            max_retries: job.max_retries,
            created_at: job.created_at,
            updated_at: job.updated_at,
        }
    }
}

impl JobId {
    pub(crate) fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum JobState {
    Enqueued,
    Processing,
    Succeeded,
    Failed { error: String },
    Scheduled { run_at: DateTime<Utc> },
}

impl JobState {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            JobState::Enqueued => "enqueued",
            JobState::Processing => "processing",
            JobState::Succeeded => "succeeded",
            JobState::Failed { .. } => "failed",
            JobState::Scheduled { .. } => "scheduled",
        }
    }

    pub(crate) fn from_db(state: &str, error: Option<String>, run_at: Option<DateTime<Utc>>) -> Self {
        match state {
            "processing" => JobState::Processing,
            "succeeded" => JobState::Succeeded,
            "failed" => JobState::Failed { error: error.unwrap_or_default() },
            "scheduled" => JobState::Scheduled { run_at: run_at.unwrap_or_else(Utc::now) },
            _ => JobState::Enqueued,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Job {
    pub id: JobId,
    pub queue: String,
    pub job_type: String,
    pub payload: Vec<u8>,
    pub state: JobState,
    pub retries: u32,
    pub max_retries: u32,
    pub parent_id: Option<JobId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Job {
    pub(crate) fn new(job_type: &str, payload: Vec<u8>) -> Self {
        let now = Utc::now();
        Self {
            id: JobId::new(),
            queue: "default".to_string(),
            job_type: job_type.to_string(),
            payload,
            state: JobState::Enqueued,
            retries: 0,
            max_retries: 3,
            parent_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub(crate) fn with_queue(mut self, queue: &str) -> Self {
        self.queue = queue.to_string();
        self
    }

    pub(crate) fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub(crate) fn scheduled_at(mut self, run_at: DateTime<Utc>) -> Self {
        self.state = JobState::Scheduled { run_at };
        self
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RecurringJob {
    pub id: String,
    pub cron: String,
    pub job_type: String,
    pub payload: Vec<u8>,
    pub queue: String,
    pub next_run: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct DashboardStats {
    pub enqueued: u64,
    pub processing: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub scheduled: u64,
    pub recurring: u64,
}
