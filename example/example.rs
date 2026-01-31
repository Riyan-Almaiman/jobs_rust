//! Example usage of jobs_rust library
//!
//! Run with: cargo run --example example
//! (Move this file to examples/example.rs for that to work)

use std::time::Duration;
use jobs_rust::JobServer;

mod example;
use example::{SendEmailArgs, ProcessPaymentArgs, CleanupJob};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create server with SQLite storage
    let mut server = JobServer::new("sqlite://jobs.db?mode=rwc").await;

    // 2. Register jobs
    let send_email_job = server
        .register_background_job("send_email", SendEmailArgs::handler, 3, "default")
        .await;

    let cleanup_job = server
        .register_background_job("cleanup", CleanupJob::handler, 1, "default")
        .await;

    let payment_job = server
        .register_background_job("process_payment", ProcessPaymentArgs::handler, 5, "critical")
        .await;

    // 3. Start workers
    server.start(2);                      // 2 workers for "default" queue
    server.create_queue("critical", 1);   // 1 worker for "critical" queue

    // 4. Enqueue a fire-and-forget job
    server.enqueue(&send_email_job, SendEmailArgs {
        to: "user@example.com".to_string(),
        subject: "Welcome!".to_string(),
    }).await?;

    // 5. Enqueue a job with no arguments
    server.enqueue(&cleanup_job, ()).await?;

    // 6. Schedule a delayed job (runs after 30 seconds)
    server.schedule(&send_email_job, SendEmailArgs {
        to: "user@example.com".to_string(),
        subject: "Reminder".to_string(),
    }, Duration::from_secs(30)).await?;

    // 7. Create a recurring job (cron: every hour)
    server.recurring(
        "hourly-cleanup",           // unique ID
        "0 0 * * * *",              // cron expression (sec min hour day month weekday)
        &cleanup_job,
        (),
    ).await?;

    // 8. Job continuation (notify after payment completes)
    server.enqueue(&payment_job, ProcessPaymentArgs {
        order_id: "ORD-123".to_string(),
        amount: 99.99,
    }).await?
    .continue_with(&send_email_job, SendEmailArgs {
        to: "customer@example.com".to_string(),
        subject: "Payment received!".to_string(),
    }).await?;

    // 9. Start web dashboard
    let app = axum::Router::new().merge(server.create_dashboard());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:5000").await?;
    tokio::spawn(axum::serve(listener, app));
    println!("Dashboard at http://127.0.0.1:5000");

    // 10. Wait for Ctrl+C then shutdown gracefully
    println!("Server running. Press Ctrl+C to stop.");
    server.wait_for_shutdown().await;

    Ok(())
}
