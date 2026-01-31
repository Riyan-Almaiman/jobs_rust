use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::de::DeserializeOwned;
use tokio::task::JoinError;

use crate::Job;

/// Error type for job execution
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    #[error("Job handler not found: {0}")]
    HandlerNotFound(String),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Job timed out after {0:?}")]
    Timeout(Duration),
}

pub type JobResult = Result<(), JobError>;

type BoxedHandler = Arc<
    dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = JobResult> + Send>> + Send + Sync,
>;

/// A background job definition
///
/// Create jobs using `server.register_background_job()`.
///
/// # Example
/// ```ignore
/// let send_email = server.register_background_job("send_email", |args: SendEmailArgs| async move {
///     println!("Sending email to {}", args.to);
///     Ok(())
/// }, 3, "default")
/// .timeout(Duration::from_secs(30));  // optional
///
/// server.enqueue(&send_email, SendEmailArgs { to: "user@example.com".into() }).await?;
/// ```
pub struct BackgroundJob<T> {
    name: &'static str,
    handler: BoxedHandler,
    max_retries: u32,
    queue: &'static str,
    timeout: Option<Duration>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> BackgroundJob<T>
where
    T: DeserializeOwned + Send + 'static,
{
    /// Create a new background job with a name and handler function
    pub(crate) fn new<F, Fut>(name: &'static str, handler: F) -> Self
    where
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        let handler = Arc::new(handler);

        let boxed: BoxedHandler = Arc::new(move |payload: Vec<u8>| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let args: T = serde_json::from_slice(&payload)?;
                handler(args).await.map_err(JobError::Execution)
            })
        });

        Self {
            name,
            handler: boxed,
            max_retries: 3,
            queue: "default",
            timeout: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Set the maximum number of retry attempts 
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set the queue for this job 
    pub fn queue(mut self, queue: &'static str) -> Self {
        self.queue = queue;
        self
    }

    /// Set an optional timeout for this job 
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Get the job name
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Get the max retries
    pub fn get_max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Get the queue
    pub fn get_queue(&self) -> &'static str {
        self.queue
    }

    /// Get the timeout
    pub fn get_timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Get a clone of the handler
    pub(crate) fn handler(&self) -> BoxedHandler {
        Arc::clone(&self.handler)
    }
}

struct RegisteredHandler {
    handler: BoxedHandler,
    timeout: Option<Duration>,
}

/// Registry for job handlers
pub struct HandlerRegistry {
    handlers: RwLock<HashMap<String, RegisteredHandler>>,
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl HandlerRegistry {
    /// Create a new empty handler registry
    pub fn new() -> Self {
        Self {
            handlers: RwLock::new(HashMap::new()),
        }
    }

    /// Register a background job
    pub fn register<T>(&self, job: &BackgroundJob<T>)
    where
        T: DeserializeOwned + Send + 'static,
    {
        let mut handlers = self.handlers.write().unwrap_or_else(|e| e.into_inner());
        handlers.insert(
            job.name.to_string(),
            RegisteredHandler {
                handler: job.handler(),
                timeout: job.get_timeout(),
            },
        );
    }

    /// Execute a job using its registered handler
    pub async fn execute(&self, job: &Job) -> JobResult {
        let registered = {
            let handlers = self.handlers.read().unwrap_or_else(|e| e.into_inner());
            let reg = handlers
                .get(&job.job_type)
                .ok_or_else(|| JobError::HandlerNotFound(job.job_type.clone()))?;
            (reg.handler.clone(), reg.timeout)
        };

        let (handler, timeout) = registered;
        let future = handler(job.payload.clone());
        let mut handle = tokio::spawn(async move { future.await });

        let join_to_error = |e: JoinError| {
            if e.is_panic() {
                JobError::Execution("Job handler panicked".to_string())
            } else {
                JobError::Execution("Job handler cancelled".to_string())
            }
        };

        let result: JobResult = match timeout {
            Some(duration) => {
                tokio::select! {
                    res = &mut handle => {
                        res.map_err(join_to_error)?
                    }
                    _ = tokio::time::sleep(duration) => {
                        handle.abort();
                        return Err(JobError::Timeout(duration));
                    }
                }
            }
            None => handle.await.map_err(join_to_error)?,
        };

        result
    }

    /// Check if a handler is registered for a job type
    pub fn has_handler(&self, job_type: &str) -> bool {
        let handlers = self.handlers.read().unwrap_or_else(|e| e.into_inner());
        handlers.contains_key(job_type)
    }

    /// Get the list of registered job types
    pub fn job_types(&self) -> Vec<String> {
        let handlers = self.handlers.read().unwrap_or_else(|e| e.into_inner());
        handlers.keys().cloned().collect()
    }
}
