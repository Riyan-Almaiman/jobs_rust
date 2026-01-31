//! # Jobs Rust
//!
//! A background job processing library for Rust, inspired by .NET's Hangfire.
//!
//! ## Features
//!
//! - **Fire-and-forget jobs**: Enqueue and process immediately
//! - **Delayed jobs**: Process after a specified duration
//! - **Recurring jobs**: Cron-based scheduling
//! - **Continuations**: Job B runs after Job A completes
//! - **Web dashboard**: Monitor queues, jobs, retry failed jobs
//! - **Retry policies**: Configurable retry on failure
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use jobs_rust::JobQueue;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct EmailArgs {
//!     to: String,
//!     subject: String,
//! }
//!
//! async fn send_email(args: EmailArgs) -> Result<(), String> {
//!     println!("Sending email to {}", args.to);
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create queue
//!     let mut queue = JobQueue::new("sqlite://jobs.db").await?;
//!
//!     // Register jobs first
//!     let send_email_job = queue.create_background_job("send_email", send_email, 3, "default").await;
//!
//!     // Start workers
//!     queue.start(2);
//!
//!     // Enqueue
//!     queue.enqueue(&send_email_job, EmailArgs {
//!         to: "user@example.com".to_string(),
//!         subject: "Hello!".to_string(),
//!     }).await?;
//!
//!     queue.wait_for_shutdown().await;
//!     Ok(())
//! }
//! ```

pub(crate) mod dashboard;
pub(crate) mod job;
pub(crate) mod registry;
pub(crate) mod scheduler;
pub(crate) mod server;
pub(crate) mod storage;
pub(crate) mod worker;

pub(crate) use job::{DashboardStats, EnqueuedJob, Job, JobId, JobState, RecurringJob};
pub(crate) use registry::{BackgroundJob, HandlerRegistry};
pub(crate) use scheduler::Scheduler;
pub use server::JobQueue;
pub(crate) use storage::{SqliteStorage, Storage};
pub(crate) use worker::Worker;
