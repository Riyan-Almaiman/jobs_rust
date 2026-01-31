//! Example usage of jobs_rust library
//!
//! Run with: cargo run --example example
//! (Move this file to examples/example.rs for that to work)

use std::time::Duration;
use jobs_rust::JobQueue;

mod jobs;
use jobs::cleanup::CleanupJob;
use jobs::process_payment::{ProcessPayment, SendPaymentEmail};
use jobs::send_email::SendEmail;
 
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create queue with SQLite storage
    let mut queue = JobQueue::new("sqlite://jobs.db?mode=rwc").await?;

    // 2. Register jobs
    let send_email_job = queue  
        .register_background_job("send_email", SendEmail::handler, 3, "default")
        .await;

    let cleanup_job = queue
        .register_background_job("cleanup", CleanupJob::handler, 1, "default")
        .await;

    let payment_job = queue
        .register_background_job("process_payment", ProcessPayment::handler, 5, "critical")
        .await;
    let send_payment_email_job = queue  
        .register_background_job("send_payment_email", ProcessPayment::send_payment_email, 3, "default")
        .await;
    // 3. Start workers
    queue.start(2);                      // 2 workers for "default" queue
    queue.create_queue("critical", 1);   // 1 worker for "critical" queue

    // 4. Enqueue a fire-and-forget job
    queue.enqueue(&send_email_job, SendEmail {
        to: "user@example.com".to_string(),
        subject: "Welcome!".to_string(),
    }).await?;

    // 5. Enqueue a job with no arguments
    queue.enqueue(&cleanup_job, ()).await?;

    // 6. Schedule a delayed job (runs after 30 seconds)
    queue.schedule(&send_email_job, SendEmail {
        to: "user@example.com".to_string(),
        subject: "Reminder".to_string(),
    }, Duration::from_secs(30)).await?;

    // 7. Create a recurring job (cron: every hour)
    queue.recurring(
        "hourly-cleanup",           // unique ID
        "0 0 * * * *",              // cron expression (sec min hour day month weekday)
        &cleanup_job,
        (),
    ).await?;

    // 8. Job continuation (notify after payment completes)
    queue.enqueue(&payment_job, ProcessPayment {
        order_id: "ORD-123".to_string(),
        amount: 99.99,
    }).await?
    .continue_with(&send_payment_email_job, SendPaymentEmail {
        to: "customer@example.com".to_string(),
        subject: "Payment received!".to_string(),
    }).await?;

    // 9. Start web dashboard
    let _ = axum::Router::new().merge(queue.create_dashboard());


    loop {
        
    }
 
}
