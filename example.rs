use std::time::Duration;
use serde::{Deserialize, Serialize};
use jobs_rust::JobQueue;

#[derive(Debug, Serialize, Deserialize)]
struct SendEmailArgs {
    to: String,
    subject: String,
}

async fn send_email(args: SendEmailArgs) -> Result<(), String> {
    println!("Sending email to {}: {}", args.to, args.subject);
    Ok(())
}

async fn cleanup(_: ()) -> Result<(), String> {
    println!("Running cleanup");
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct ProcessPaymentArgs {
    order_id: String,
    amount: f64,
}

async fn process_payment(args: ProcessPaymentArgs) -> Result<(), String> {
    println!("Processing ${} for order {}", args.amount, args.order_id);
    if args.amount > 1000.0 {
        return Err("Requires manual review".to_string());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut queue = JobQueue::new("sqlite://jobs.db?mode=rwc").await?;

    // Register jobs
    let send_email_job = queue.register("send_email", send_email, 3, "default", None);
    let cleanup_job = queue.register("cleanup", cleanup, 1, "default", None);
    let payment_job = queue.register("process_payment", process_payment, 5, "critical", Some(Duration::from_secs(30)));

    // Start workers + scheduler
    queue.start(2);
    queue.add_workers("critical", 1);

    // Fire-and-forget
    send_email_job.enqueue(SendEmailArgs {
        to: "user@example.com".to_string(),
        subject: "Welcome!".to_string(),
    }).await?;

    // No-args job
    cleanup_job.enqueue(()).await?;

    // Delayed job
    send_email_job.schedule(SendEmailArgs {
        to: "user@example.com".to_string(),
        subject: "Reminder".to_string(),
    }, Duration::from_secs(30)).await?;

    // Recurring job
    cleanup_job.recurring("hourly-cleanup", "0 0 * * * *", ()).await?;

    // Payment job
    payment_job.enqueue(ProcessPaymentArgs {
        order_id: "ORD-123".to_string(),
        amount: 99.99,
    }).await?;

    println!("Running. Press Ctrl+C to stop.");
    queue.wait_for_shutdown().await;

    Ok(())
}
