use std::time::Duration;
use serde::{Deserialize, Serialize};
use jobs_rust::JobQueue;

#[derive(Serialize, Deserialize)]
struct SendEmailArgs {
    to: String,
    subject: String,
}

async fn send_email(args: SendEmailArgs) -> Result<(), String> {
    println!("HANDLER STARTED for {}", args.to);
    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("HANDLER FINISHED for {}", args.to);
    Ok(())
}

async fn cleanup(_: ()) -> Result<(), String> {
    println!("Running cleanup");
    Ok(())
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create queue
    let mut queue = JobQueue::new("sqlite://examples/jobs.db?mode=rwc").await?;

    // Register jobs
    let send_email_job = queue.register("send_email", send_email, 3, "default", Some(Duration::from_secs(30)));
    let cleanup_job = queue.register("cleanup", cleanup, 1, "default", None);

    queue.start(2);

    // Fire-and-forget
    let send_email_job_id = send_email_job.enqueue(SendEmailArgs {
        to: "user@example.com".to_string(),
        subject: "Welcome!".to_string(),
    }).await?;

    println!("Enqueued job with ID: {}", send_email_job_id);

    println!("Job info: {:?}", queue.get_job(&send_email_job_id).await?);

    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("Job info after 3 seconds {:?}", queue.get_job(&send_email_job_id).await?);

    // No args job
    cleanup_job.enqueue(()).await?;

    // Delayed job
    send_email_job.schedule(SendEmailArgs {
        to: "user@example.com".to_string(),
        subject: "Reminder".to_string(),
    }, Duration::from_secs(30)).await?;

    // Recurring job (cron)
    cleanup_job.recurring("hourly-cleanup", "0 0 * * * *", ()).await?;

   

    // minimal dashboard (WIP)
    let app = axum::Router::new().merge(queue.dashboard());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:5000").await?;
        axum::serve(listener, app).await.unwrap();



    Ok(())
}
