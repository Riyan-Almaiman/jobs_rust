use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SendEmail {
    pub to: String,
    pub subject: String,
}

impl SendEmail{
    pub async fn handler(args: Self) -> Result<(), String> {
        println!("Sending email to {}: {}", args.to, args.subject);
        Ok(())
    }
}
