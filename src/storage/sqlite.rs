use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{Row, SqlitePool};

use crate::{DashboardStats, Storage, Job, JobId, JobState, RecurringJob};

use super::{Result};

pub struct SqliteStorage {
    pub pool: SqlitePool,
}

impl SqliteStorage {
    pub async fn new(database_url: &str) -> std::result::Result<Self, sqlx::Error> {
        let pool = SqlitePool::connect(database_url).await?;
        let storage = Self { pool };
        storage.configure().await?;
        storage.migrate().await?;
        Ok(storage)
    }

    async fn configure(&self) -> std::result::Result<(), sqlx::Error> {
        sqlx::query("PRAGMA journal_mode=WAL;")
            .execute(&self.pool)
            .await?;

        sqlx::query("PRAGMA busy_timeout=5000;")
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Create a new SQLite storage from an existing pool
    // pub async fn from_pool(pool: SqlitePool) -> std::result::Result<Self, sqlx::Error> {
    //     let storage = Self { pool };
    //     storage.configure().await?;
    //     storage.migrate().await?;
    //     Ok(storage)
    // }

    // pub async fn in_memory() -> std::result::Result<Self, sqlx::Error> {
    //     Self::new("sqlite::memory:").await
    // }

    /// Run database migrations
    async fn migrate(&self) -> std::result::Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                queue TEXT NOT NULL,
                job_type TEXT NOT NULL,
                payload BLOB NOT NULL,
                state TEXT NOT NULL,
                error TEXT,
                retries INTEGER DEFAULT 0,
                run_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                max_retries INTEGER NOT NULL DEFAULT 3,
                parent_id TEXT,
                FOREIGN KEY (parent_id) REFERENCES jobs(id)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_jobs_queue_state ON jobs(queue, state)
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_jobs_parent_id ON jobs(parent_id)
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS recurring_jobs (
                id TEXT PRIMARY KEY,
                cron TEXT NOT NULL,
                job_type TEXT NOT NULL,
                payload BLOB NOT NULL,
                queue TEXT NOT NULL,
                next_run TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    fn row_to_job(&self, row: sqlx::sqlite::SqliteRow) -> Result<Job> {
        let id: String = row.get("id");
        let queue: String = row.get("queue");
        let job_type: String = row.get("job_type");
        let payload: Vec<u8> = row.get("payload");
        let state_str: String = row.get("state");
        let error: Option<String> = row.get("error");
        let retries: Option<i32> = row.get("retries");
        let run_at_str: Option<String> = row.get("run_at");
        let created_at_str: String = row.get("created_at");
        let updated_at_str: String = row.get("updated_at");
        let max_retries: i32 = row.get("max_retries");
        let parent_id: Option<String> = row.get("parent_id");

        let run_at = run_at_str.and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&Utc));

        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        let state = JobState::from_db(&state_str, error, run_at);

        Ok(Job {
            id: JobId(id),
            queue,
            job_type,
            payload,
            state,
            created_at,
            updated_at,
            max_retries: max_retries as u32,
            retries: retries.unwrap_or(0) as u32,
            parent_id: parent_id.map(JobId),
        })
    }

    fn row_to_recurring(&self, row: sqlx::sqlite::SqliteRow) -> Result<RecurringJob> {
        let id: String = row.get("id");
        let cron: String = row.get("cron");
        let job_type: String = row.get("job_type");
        let payload: Vec<u8> = row.get("payload");
        let queue: String = row.get("queue");
        let next_run_str: String = row.get("next_run");
        let updated_at_str: String = row.get("updated_at");

        let next_run = DateTime::parse_from_rfc3339(&next_run_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(RecurringJob {
            id,
            cron,
            job_type,
            payload,
            queue,
            next_run,
            updated_at,
        })
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn enqueue(&self, job: Job) -> Result<JobId> {
        let (state_str, error, run_at) = match &job.state {
            JobState::Enqueued => ("enqueued", None, None),
            JobState::Processing => ("processing", None, None),
            JobState::Succeeded => ("succeeded", None, None),
            JobState::Failed { error } => ("failed", Some(error.clone()), None),
            JobState::Scheduled { run_at } => ("scheduled", None, Some(run_at.to_rfc3339())),
            JobState::Deleted => ("deleted", None, None),
        };

        sqlx::query(
            r#"
            INSERT INTO jobs (id, queue, job_type, payload, state, error, retries, run_at, created_at, updated_at, max_retries, parent_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&job.id.0)
        .bind(&job.queue)
        .bind(&job.job_type)
        .bind(&job.payload)
        .bind(state_str)
        .bind(error)
        .bind(job.retries as i32)
        .bind(run_at)
        .bind(job.created_at.to_rfc3339())
        .bind(job.updated_at.to_rfc3339())
        .bind(job.max_retries as i32)
        .bind(job.parent_id.as_ref().map(|id| &id.0))
        .execute(&self.pool)
        .await?;

        Ok(job.id)
    }

    async fn fetch_next(&self, queue: &str) -> Result<Option<Job>> {
        // Atomically claim a job by updating state to 'processing' in the same query
        // This prevents multiple workers from grabbing the same job
        let row = sqlx::query(
            r#"
            UPDATE jobs
            SET state = 'processing', updated_at = ?
            WHERE id = (
                SELECT id FROM jobs
                WHERE queue = ? AND state = 'enqueued'
                ORDER BY created_at ASC
                LIMIT 1
            )
            RETURNING *
            "#,
        )
        .bind(Utc::now().to_rfc3339())
        .bind(queue)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Ok(Some(self.row_to_job(row)?)),
            None => Ok(None),
        }
    }

    async fn update_state(&self, id: &JobId, state: JobState) -> Result<()> {
        let now = Utc::now().to_rfc3339();

        let (state_str, error, run_at) = match &state {
            JobState::Enqueued => ("enqueued", None, None),
            JobState::Processing => ("processing", None, None),
            JobState::Succeeded => ("succeeded", None, None),
            JobState::Failed { error } => ("failed", Some(error.clone()), None),
            JobState::Scheduled { run_at } => ("scheduled", None, Some(run_at.to_rfc3339())),
            JobState::Deleted => ("deleted", None, None),
        };

        sqlx::query(
            r#"
            UPDATE jobs
            SET state = ?, error = ?, run_at = ?, updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(state_str)
        .bind(error)
        .bind(run_at)
        .bind(now)
        .bind(&id.0)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn increment_retries(&self, id: &JobId, state: JobState) -> Result<()> {
        let now = Utc::now().to_rfc3339();

        let (state_str, error, run_at) = match &state {
            JobState::Enqueued => ("enqueued", None, None),
            JobState::Processing => ("processing", None, None),
            JobState::Succeeded => ("succeeded", None, None),
            JobState::Failed { error } => ("failed", Some(error.clone()), None),
            JobState::Scheduled { run_at } => ("scheduled", None, Some(run_at.to_rfc3339())),
            JobState::Deleted => ("deleted", None, None),
        };


        sqlx::query(
            r#"
            UPDATE jobs
            SET state = ?, error = ?, run_at = ?, retries = COALESCE(retries, 0) + 1, updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(state_str)
        .bind(error)
        .bind(run_at)
        .bind(now)
        .bind(&id.0)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_job(&self, id: &JobId) -> Result<Option<Job>> {
        let row = sqlx::query("SELECT * FROM jobs WHERE id = ?")
            .bind(&id.0)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some(row) => Ok(Some(self.row_to_job(row)?)),
            None => Ok(None),
        }
    }

    async fn get_jobs_by_state(&self, state: &str, limit: u32) -> Result<Vec<Job>> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM jobs
            WHERE state = ?
            ORDER BY updated_at DESC
            LIMIT ?
            "#,
        )
        .bind(state)
        .bind(limit as i32)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(|row| self.row_to_job(row)).collect()
    }

    async fn delete_job(&self, id: &JobId) -> Result<()> {
        sqlx::query("DELETE FROM jobs WHERE id = ?")
            .bind(&id.0)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn enqueue_scheduled_jobs(&self, before: DateTime<Utc>) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE jobs
            SET state = 'enqueued', updated_at = ?
            WHERE state = 'scheduled' 
              AND run_at <= ?
              AND parent_id IS NULL
            "#,
        )
        .bind(Utc::now().to_rfc3339())
        .bind(before.to_rfc3339())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_continuation_jobs(&self, parent_id: &JobId) -> Result<Vec<Job>> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM jobs
            WHERE parent_id = ? AND state = 'scheduled'
            "#,
        )
        .bind(&parent_id.0)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(|row| self.row_to_job(row)).collect()
    }

    async fn upsert_recurring(&self, job: RecurringJob) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO recurring_jobs (id, cron, job_type, payload, queue, next_run, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                cron = excluded.cron,
                job_type = excluded.job_type,
                payload = excluded.payload,
                queue = excluded.queue,
                next_run = excluded.next_run,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&job.id)
        .bind(&job.cron)
        .bind(&job.job_type)
        .bind(&job.payload)
        .bind(&job.queue)
        .bind(job.next_run.to_rfc3339())
        .bind(job.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn fetch_due_recurring(&self, now: DateTime<Utc>) -> Result<Vec<RecurringJob>> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM recurring_jobs
            WHERE next_run <= ?
            "#,
        )
        .bind(now.to_rfc3339())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| self.row_to_recurring(row))
            .collect()
    }

    async fn update_recurring_schedule(&self, id: &str, current_run: DateTime<Utc>, new_run: DateTime<Utc>) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE recurring_jobs
            SET next_run = ?, updated_at = ?
            WHERE id = ? AND next_run = ?
            "#,
        )
        .bind(new_run.to_rfc3339())
        .bind(Utc::now().to_rfc3339())
        .bind(id)
        .bind(current_run.to_rfc3339())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    async fn get_all_recurring(&self) -> Result<Vec<RecurringJob>> {
        let rows = sqlx::query("SELECT * FROM recurring_jobs ORDER BY id")
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(|row| self.row_to_recurring(row))
            .collect()
    }

    async fn delete_recurring(&self, id: &str) -> Result<()> {
        sqlx::query("DELETE FROM recurring_jobs WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn get_stats(&self) -> Result<DashboardStats> {
        let enqueued: i32 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE state = 'enqueued'")
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);

        let processing: i32 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE state = 'processing'")
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);

        let succeeded: i32 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE state = 'succeeded'")
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);

        let failed: i32 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE state = 'failed'")
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);

        let scheduled: i32 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE state = 'scheduled'")
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);

        let recurring: i32 = sqlx::query_scalar("SELECT COUNT(*) FROM recurring_jobs")
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);

        Ok(DashboardStats {
            enqueued: enqueued as u64,
            processing: processing as u64,
            succeeded: succeeded as u64,
            failed: failed as u64,
            scheduled: scheduled as u64,
            recurring: recurring as u64,
        })
    }

    async fn recover_stuck_jobs_by_type(&self, job_type: &str) -> Result<u64> {
        // Only recover jobs that haven't been updated in 5 minutes
        let stale_threshold = Utc::now() - chrono::Duration::minutes(5);

        // Increment retry count to prevent infinite crash loops
        // If max retries exceeded, mark as failed
        let result = sqlx::query(
            r#"
            UPDATE jobs
            SET 
                state = CASE WHEN retries + 1 > max_retries THEN 'failed' ELSE 'enqueued' END,
                error = CASE WHEN retries + 1 > max_retries THEN 'Crash recovery limit exceeded' ELSE error END,
                retries = retries + 1,
                updated_at = ?
            WHERE state = 'processing' 
              AND job_type = ?
              AND updated_at < ?
            "#,
        )
        .bind(Utc::now().to_rfc3339())
        .bind(job_type)
        .bind(stale_threshold.to_rfc3339())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    async fn recover_orphaned_continuations_by_type(&self, job_type: &str) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE jobs
            SET state = 'enqueued', updated_at = ?
            WHERE state = 'scheduled'
              AND job_type = ?
              AND parent_id IS NOT NULL
              AND parent_id IN (SELECT id FROM jobs WHERE state = 'succeeded')
            "#,
        )
        .bind(Utc::now().to_rfc3339())
        .bind(job_type)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    async fn promote_recurring_job(&self, id: &str, current_run: DateTime<Utc>, next_run: DateTime<Utc>) -> Result<bool> {
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            UPDATE recurring_jobs
            SET next_run = ?, updated_at = ?
            WHERE id = ? AND next_run = ?
            RETURNING job_type, payload, queue
            "#
        )
        .bind(next_run.to_rfc3339())
        .bind(Utc::now().to_rfc3339())
        .bind(id)
        .bind(current_run.to_rfc3339())
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(row) = row {
            let job_type: String = row.get("job_type");
            let payload: Vec<u8> = row.get("payload");
            let queue: String = row.get("queue");
            let new_id = JobId::new();
            let now = Utc::now();

            sqlx::query(
                r#"
                INSERT INTO jobs (id, queue, job_type, payload, state, retries, created_at, updated_at, max_retries, parent_id)
                VALUES (?, ?, ?, ?, 'enqueued', 0, ?, ?, 3, NULL)
                "#
            )
            .bind(&new_id.0)
            .bind(queue)
            .bind(job_type)
            .bind(payload)
            .bind(now.to_rfc3339())
            .bind(now.to_rfc3339())
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
