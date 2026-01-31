use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessPayment {
    pub order_id: String,
    pub amount: f64,
}
#[derive(Debug, Serialize, Deserialize)]

pub struct SendPaymentEmail {
    pub to: String,
    pub subject: String,
}

impl ProcessPayment {
    pub async fn handler(args: Self) -> Result<(), String> {
        println!("Processing payment for order {}: ${}", args.order_id, args.amount);

        if args.amount > 1000.0 {
            return Err("Payment requires manual review".to_string());
        }

        Ok(())
    }  
      pub async fn send_payment_email(args: SendPaymentEmail) -> Result<(), String> {
        println!("Sending email to {}: {}", args.to, args.subject);
        Ok(())
    }
}
