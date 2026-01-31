pub struct CleanupJob;

impl CleanupJob {
    pub async fn handler(_: ()) -> Result<(), String> {
        println!("Running cleanup task");
        Ok(())
    }
}
