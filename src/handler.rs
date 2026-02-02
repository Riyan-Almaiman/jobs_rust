use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::task::JoinError;

use crate::job::Job;

#[derive(Debug, thiserror::Error)]
pub(crate) enum JobError {
    #[error("Handler not found: {0}")]
    HandlerNotFound(String),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
    #[error("Execution error: {0}")]
    Execution(String),
    #[error("Timeout after {0:?}")]
    Timeout(Duration),
}

pub(crate) type JobResult = Result<(), JobError>;
pub(crate) type BoxedHandler = Arc<
    dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = JobResult> + Send>> + Send + Sync,
>;

pub(crate) struct HandlerRegistry {
    handlers: RwLock<HashMap<String, (BoxedHandler, Option<Duration>)>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self { handlers: RwLock::new(HashMap::new()) }
    }

    pub fn register(&self, name: &str, handler: BoxedHandler, timeout: Option<Duration>) {
        let mut handlers = self.handlers.write().unwrap_or_else(|e| e.into_inner());
        handlers.insert(name.to_string(), (handler, timeout));
    }

    pub async fn execute(&self, job: &Job) -> JobResult {
        let (handler, timeout) = {
            let handlers = self.handlers.read().unwrap_or_else(|e| e.into_inner());
            let (h, t) = handlers
                .get(&job.job_type)
                .ok_or_else(|| JobError::HandlerNotFound(job.job_type.clone()))?;
            (h.clone(), *t)
        };

        let future = handler(job.payload.clone());
        let mut handle = tokio::spawn(async move { future.await });

        let join_to_error = |e: JoinError| {
            if e.is_panic() {
                JobError::Execution("Handler panicked".to_string())
            } else {
                JobError::Execution("Handler cancelled".to_string())
            }
        };

        match timeout {
            Some(duration) => {
                tokio::select! {
                    res = &mut handle => res.map_err(join_to_error)?,
                    _ = tokio::time::sleep(duration) => {
                        handle.abort();
                        Err(JobError::Timeout(duration))
                    }
                }
            }
            None => handle.await.map_err(join_to_error)?,
        }
    }
}
