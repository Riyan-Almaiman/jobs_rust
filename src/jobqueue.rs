use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::background_job::BackgroundJob;
use crate::handler::{BoxedHandler, HandlerRegistry, JobError};
use crate::job::{JobId, JobInfo};
use crate::scheduler::Scheduler;
use crate::storage::{SqliteStorage, Storage};
use crate::worker::Worker;

pub struct JobQueue {
    storage: Arc<dyn Storage>,
    handlers: Arc<HandlerRegistry>,
    shutdown: CancellationToken,
    worker_handles: Vec<JoinHandle<()>>,
    scheduler_handle: Option<JoinHandle<()>>,
}

impl JobQueue {
    pub async fn new(connection_string: &str) -> Result<Self, String> {
        let storage = SqliteStorage::new(connection_string)
            .await
            .map_err(|e| e.to_string())?;

        Ok(Self {
            storage: Arc::new(storage),
            handlers: Arc::new(HandlerRegistry::new()),
            shutdown: CancellationToken::new(),
            worker_handles: Vec::new(),
            scheduler_handle: None,
        })
    }

    pub fn register<T, F, Fut>(
        &self,
        name: &'static str,
        handler: F,
        max_retries: u32,
        queue: &'static str,
        timeout: Option<Duration>,
    ) -> BackgroundJob<T>
    where
        T: DeserializeOwned + Send + 'static,
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

        self.handlers.register(name, boxed, timeout);

        BackgroundJob {
            name,
            max_retries,
            queue,
            storage: Arc::clone(&self.storage),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn start(&mut self, worker_count: usize) {
        for _ in 0..worker_count {
            self.spawn_worker("default");
        }
        self.start_scheduler();
    }

    pub fn add_workers(&mut self, queue: &str, count: usize) {
        for _ in 0..count {
            self.spawn_worker(queue);
        }
    }

    fn spawn_worker(&mut self, queue: &str) {
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
        info!(queue = queue, "Worker started");
    }

    fn start_scheduler(&mut self) {
        if self.scheduler_handle.is_some() {
            return;
        }

        let scheduler = Scheduler::new(Arc::clone(&self.storage));
        let shutdown = self.shutdown.clone();

        let handle = tokio::spawn(async move {
            scheduler.run(shutdown).await;
        });

        self.scheduler_handle = Some(handle);
        info!("Scheduler started");
    }

    pub fn dashboard(&self) -> axum::Router {
        crate::dashboard::router(Arc::clone(&self.storage))
    }

    pub async fn get_job(&self, id: &JobId) -> Result<Option<JobInfo>, String> {
        self.storage
            .get_job(id)
            .await
            .map(|opt| opt.map(JobInfo::from))
            .map_err(|e| e.to_string())
    }

    pub async fn wait_for_shutdown(&mut self) {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");

        info!("Shutting down...");
        self.shutdown.cancel();

        for handle in self.worker_handles.drain(..) {
            let _ = handle.await;
        }

        if let Some(handle) = self.scheduler_handle.take() {
            let _ = handle.await;
        }

        info!("Shutdown complete");
    }
}
