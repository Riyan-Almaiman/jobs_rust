mod templates;

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    response::{Html, Redirect},
    routing::{get, post},
    Router,
};

use crate::job::{JobId, JobState};
use crate::storage::Storage;

#[derive(Clone)]
struct DashboardState {
    storage: Arc<dyn Storage>,
}

pub fn router(storage: Arc<dyn Storage>) -> Router {
    let state = DashboardState { storage };

    Router::new()
        .route("/", get(overview))
        .route("/jobs/enqueued", get(jobs_enqueued))
        .route("/jobs/processing", get(jobs_processing))
        .route("/jobs/succeeded", get(jobs_succeeded))
        .route("/jobs/failed", get(jobs_failed))
        .route("/jobs/scheduled", get(jobs_scheduled))
        .route("/jobs/{id}/retry", post(retry_job))
        .route("/jobs/{id}/delete", post(delete_job))
        .route("/recurring", get(recurring_jobs))
        .with_state(state)
}

async fn overview(State(state): State<DashboardState>) -> Html<String> {
    let stats = state.storage.get_stats().await.unwrap_or_default();
    Html(templates::render_overview(&stats))
}

async fn jobs_enqueued(State(state): State<DashboardState>) -> Html<String> {
    let jobs = state.storage.get_jobs_by_state("enqueued", 100).await.unwrap_or_default();
    Html(templates::render_jobs("Enqueued", &jobs, false))
}

async fn jobs_processing(State(state): State<DashboardState>) -> Html<String> {
    let jobs = state.storage.get_jobs_by_state("processing", 100).await.unwrap_or_default();
    Html(templates::render_jobs("Processing", &jobs, false))
}

async fn jobs_succeeded(State(state): State<DashboardState>) -> Html<String> {
    let jobs = state.storage.get_jobs_by_state("succeeded", 100).await.unwrap_or_default();
    Html(templates::render_jobs("Succeeded", &jobs, false))
}

async fn jobs_failed(State(state): State<DashboardState>) -> Html<String> {
    let jobs = state.storage.get_jobs_by_state("failed", 100).await.unwrap_or_default();
    Html(templates::render_jobs("Failed", &jobs, true))
}

async fn jobs_scheduled(State(state): State<DashboardState>) -> Html<String> {
    let jobs = state.storage.get_jobs_by_state("scheduled", 100).await.unwrap_or_default();
    Html(templates::render_jobs("Scheduled", &jobs, false))
}

async fn retry_job(State(state): State<DashboardState>, Path(id): Path<String>) -> Redirect {
    let _ = state.storage.update_state(&JobId(id), JobState::Enqueued).await;
    Redirect::to("/jobs/failed")
}

async fn delete_job(State(state): State<DashboardState>, Path(id): Path<String>) -> Redirect {
    let _ = state.storage.delete_job(&JobId(id)).await;
    Redirect::to("/")
}

async fn recurring_jobs(State(state): State<DashboardState>) -> Html<String> {
    let jobs = state.storage.get_all_recurring().await.unwrap_or_default();
    Html(templates::render_recurring(&jobs))
}
