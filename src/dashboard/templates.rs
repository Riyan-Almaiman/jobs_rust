use crate::job::{DashboardStats, Job, JobState, RecurringJob};

fn layout(title: &str, content: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - Jobs Dashboard</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; color: #333; line-height: 1.6; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        header {{ background: #2563eb; color: white; padding: 20px; margin-bottom: 20px; }}
        header h1 {{ font-size: 1.5rem; }}
        nav {{ background: white; padding: 10px 20px; margin-bottom: 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        nav a {{ color: #2563eb; text-decoration: none; margin-right: 20px; padding: 8px 16px; border-radius: 4px; }}
        nav a:hover {{ background: #eff6ff; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .stat-card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); text-align: center; }}
        .stat-card h3 {{ font-size: 2rem; color: #2563eb; }}
        .stat-card p {{ color: #666; text-transform: uppercase; font-size: 0.8rem; }}
        .card {{ background: white; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow: hidden; }}
        .card-header {{ background: #f8fafc; padding: 15px 20px; border-bottom: 1px solid #e5e7eb; font-weight: 600; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 12px 20px; text-align: left; border-bottom: 1px solid #e5e7eb; }}
        th {{ background: #f8fafc; font-weight: 600; color: #666; font-size: 0.85rem; text-transform: uppercase; }}
        tr:hover {{ background: #f8fafc; }}
        .badge {{ display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 0.75rem; font-weight: 600; }}
        .badge-enqueued {{ background: #dbeafe; color: #1d4ed8; }}
        .badge-processing {{ background: #fef3c7; color: #d97706; }}
        .badge-succeeded {{ background: #d1fae5; color: #059669; }}
        .badge-failed {{ background: #fee2e2; color: #dc2626; }}
        .badge-scheduled {{ background: #e0e7ff; color: #4f46e5; }}
        .btn {{ display: inline-block; padding: 6px 12px; border: none; border-radius: 4px; font-size: 0.85rem; cursor: pointer; }}
        .btn-primary {{ background: #2563eb; color: white; }}
        .btn-danger {{ background: #dc2626; color: white; }}
        .btn-sm {{ padding: 4px 8px; font-size: 0.75rem; }}
        .error-text {{ color: #dc2626; font-size: 0.85rem; }}
        .mono {{ font-family: monospace; font-size: 0.85rem; }}
        .empty-state {{ text-align: center; padding: 40px; color: #666; }}
    </style>
</head>
<body>
    <header><div class="container"><h1>Jobs Dashboard</h1></div></header>
    <div class="container">
        <nav>
            <a href="/">Overview</a>
            <a href="/jobs/enqueued">Enqueued</a>
            <a href="/jobs/processing">Processing</a>
            <a href="/jobs/succeeded">Succeeded</a>
            <a href="/jobs/failed">Failed</a>
            <a href="/jobs/scheduled">Scheduled</a>
            <a href="/recurring">Recurring</a>
        </nav>
        {content}
    </div>
</body>
</html>"#
    )
}

pub fn render_overview(stats: &DashboardStats) -> String {
    let content = format!(
        r#"<div class="stats">
            <div class="stat-card"><h3>{}</h3><p>Enqueued</p></div>
            <div class="stat-card"><h3>{}</h3><p>Processing</p></div>
            <div class="stat-card"><h3>{}</h3><p>Succeeded</p></div>
            <div class="stat-card"><h3>{}</h3><p>Failed</p></div>
            <div class="stat-card"><h3>{}</h3><p>Scheduled</p></div>
            <div class="stat-card"><h3>{}</h3><p>Recurring</p></div>
        </div>"#,
        stats.enqueued, stats.processing, stats.succeeded, stats.failed, stats.scheduled, stats.recurring
    );
    layout("Overview", &content)
}

fn state_badge(state: &str) -> &'static str {
    match state {
        "enqueued" => "badge-enqueued",
        "processing" => "badge-processing",
        "succeeded" => "badge-succeeded",
        "failed" => "badge-failed",
        "scheduled" => "badge-scheduled",
        _ => "badge-enqueued",
    }
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

pub fn render_jobs(title: &str, jobs: &[Job], show_retry: bool) -> String {
    let rows = if jobs.is_empty() {
        r#"<tr><td colspan="6" class="empty-state">No jobs found</td></tr>"#.to_string()
    } else {
        jobs.iter()
            .map(|job| {
                let state_str = job.state.as_str();
                let error_display = match &job.state {
                    JobState::Failed { error } => format!(r#"<br><span class="error-text">{}</span>"#, html_escape(error)),
                    _ => String::new(),
                };
                let actions = if show_retry {
                    format!(
                        r#"<form style="display:inline" method="post" action="/jobs/{}/retry"><button class="btn btn-primary btn-sm">Retry</button></form>
                        <form style="display:inline" method="post" action="/jobs/{}/delete"><button class="btn btn-danger btn-sm">Delete</button></form>"#,
                        job.id, job.id
                    )
                } else {
                    format!(r#"<form style="display:inline" method="post" action="/jobs/{}/delete"><button class="btn btn-danger btn-sm">Delete</button></form>"#, job.id)
                };
                format!(
                    r#"<tr><td class="mono">{}</td><td>{}</td><td>{}</td><td><span class="badge {}">{}</span>{}</td><td>{}</td><td>{}</td></tr>"#,
                    &job.id.0[..8.min(job.id.0.len())],
                    html_escape(&job.job_type),
                    html_escape(&job.queue),
                    state_badge(state_str),
                    state_str,
                    error_display,
                    job.created_at.format("%Y-%m-%d %H:%M:%S"),
                    actions
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let content = format!(
        r#"<div class="card"><div class="card-header">{} Jobs</div>
        <table><thead><tr><th>ID</th><th>Type</th><th>Queue</th><th>State</th><th>Created</th><th>Actions</th></tr></thead>
        <tbody>{}</tbody></table></div>"#,
        title, rows
    );
    layout(title, &content)
}

pub fn render_recurring(jobs: &[RecurringJob]) -> String {
    let rows = if jobs.is_empty() {
        r#"<tr><td colspan="5" class="empty-state">No recurring jobs</td></tr>"#.to_string()
    } else {
        jobs.iter()
            .map(|job| {
                format!(
                    r#"<tr><td class="mono">{}</td><td>{}</td><td class="mono">{}</td><td>{}</td><td>{}</td></tr>"#,
                    html_escape(&job.id),
                    html_escape(&job.job_type),
                    html_escape(&job.cron),
                    html_escape(&job.queue),
                    job.next_run.format("%Y-%m-%d %H:%M:%S")
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let content = format!(
        r#"<div class="card"><div class="card-header">Recurring Jobs</div>
        <table><thead><tr><th>ID</th><th>Type</th><th>Cron</th><th>Queue</th><th>Next Run</th></tr></thead>
        <tbody>{}</tbody></table></div>"#,
        rows
    );
    layout("Recurring", &content)
}
