use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use hickory_resolver::{config::{ResolverConfig, ResolverOpts}, TokioAsyncResolver};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, FromRow, PgPool};
use std::{collections::HashSet, env, net::SocketAddr, time::Duration};
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, net::TcpStream, time::timeout};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use uuid::Uuid;
use validator::Validate;

#[derive(Clone)] struct AppState { db: PgPool, api_key: String, max_bytes: usize, max_emails: usize, smtp_enabled: bool, resolver: TokioAsyncResolver }
#[derive(Serialize)] struct ApiError { error: String }
fn err(status: StatusCode, message: impl Into<String>) -> Response { (status, Json(ApiError { error: message.into() })).into_response() }

async fn auth(State(state): State<AppState>, headers: HeaderMap, request: axum::extract::Request, next: Next) -> Response {
    let supplied = headers.get("x-api-key").and_then(|v| v.to_str().ok()).unwrap_or("");
    if supplied.is_empty() || supplied != state.api_key { return err(StatusCode::UNAUTHORIZED, "Missing or invalid x-api-key"); }
    next.run(request).await
}
async fn health() -> &'static str { "ok" }

#[derive(Serialize, FromRow)] struct Job {
    id: Uuid, filename: String, status: String, total_count: i32, processed_count: i32,
    valid_count: i32, invalid_count: i32, risky_count: i32, unknown_count: i32,
    error: Option<String>, created_at: DateTime<Utc>, started_at: Option<DateTime<Utc>>, finished_at: Option<DateTime<Utc>>,
}
#[derive(Deserialize)] struct Page { limit: Option<i64>, offset: Option<i64> }
#[derive(Serialize, FromRow)] struct ResultRow {
    row_number: i32, original_email: String, normalized_email: String, syntax_valid: bool,
    domain: Option<String>, domain_status: String, mx_status: String, mx_host: Option<String>,
    disposable: bool, smtp_status: String, result_status: String, reason: String,
}

async fn create_job(State(state): State<AppState>, mut multipart: Multipart) -> Response {
    let mut filename = "emails.csv".to_string();
    let mut bytes: Option<Vec<u8>> = None;
    loop {
        match multipart.next_field().await {
            Ok(Some(field)) => {
                if field.name() == Some("file") {
                    filename = field.file_name().unwrap_or("emails.csv").to_string();
                    match field.bytes().await { Ok(b) => { if b.len() > state.max_bytes { return err(StatusCode::PAYLOAD_TOO_LARGE, "File exceeds upload size limit"); } bytes = Some(b.to_vec()); }, Err(_) => return err(StatusCode::BAD_REQUEST, "Could not read uploaded file") }
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => return err(StatusCode::BAD_REQUEST, "Invalid multipart upload"),
        }
    }
    let Some(bytes) = bytes else { return err(StatusCode::BAD_REQUEST, "Expected multipart field named 'file'"); };
    if bytes.is_empty() { return err(StatusCode::BAD_REQUEST, "Uploaded file is empty"); }
    let text = match String::from_utf8(bytes) { Ok(s) => s, Err(_) => return err(StatusCode::BAD_REQUEST, "CSV must be UTF-8 text") };
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(text.as_bytes());
    let headers = match rdr.headers() { Ok(h) => h.clone(), Err(_) => return err(StatusCode::BAD_REQUEST, "Could not parse CSV headers") };
    let email_col = headers.iter().position(|h| h.trim().eq_ignore_ascii_case("email"));
    let mut emails: Vec<String> = Vec::new();
    if email_col.is_some() {
        for record in rdr.records() {
            let record = match record { Ok(r) => r, Err(_) => return err(StatusCode::BAD_REQUEST, "Malformed CSV row") };
            if let Some(value) = email_col.and_then(|i| record.get(i)) { if !value.trim().is_empty() { emails.push(value.trim().to_string()); } }
            if emails.len() > state.max_emails { return err(StatusCode::PAYLOAD_TOO_LARGE, format!("Maximum {} emails per job", state.max_emails)); }
        }
    } else {
        // No `email` header: treat the first column of every record, including the first line, as data.
        let mut raw = csv::ReaderBuilder::new().has_headers(false).flexible(true).from_reader(text.as_bytes());
        for record in raw.records() {
            let record = match record { Ok(r) => r, Err(_) => return err(StatusCode::BAD_REQUEST, "Malformed CSV row") };
            if let Some(value) = record.get(0) { if !value.trim().is_empty() { emails.push(value.trim().to_string()); } }
            if emails.len() > state.max_emails { return err(StatusCode::PAYLOAD_TOO_LARGE, format!("Maximum {} emails per job", state.max_emails)); }
        }
    }
    if emails.is_empty() { return err(StatusCode::BAD_REQUEST, "No email addresses found in the file"); }
    let id = Uuid::new_v4();
    let mut tx = match state.db.begin().await { Ok(t) => t, Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()) };
    if let Err(e) = sqlx::query("INSERT INTO jobs (id, filename, status, total_count) VALUES ($1,$2,'queued',$3)").bind(id).bind(filename).bind(emails.len() as i32).execute(&mut *tx).await { return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()); }
    for (idx, email) in emails.iter().enumerate() {
        if let Err(e) = sqlx::query("INSERT INTO email_results (job_id,row_number,original_email,normalized_email,syntax_valid,domain_status,mx_status,disposable,smtp_status,result_status,reason) VALUES ($1,$2,$3,$4,FALSE,'pending','pending',FALSE,'not_checked','pending','Queued for validation')")
            .bind(id).bind((idx+1) as i32).bind(email).bind(email.trim().to_lowercase()).execute(&mut *tx).await { return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()); }
    }
    if let Err(e) = tx.commit().await { return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()); }
    match sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id=$1").bind(id).fetch_one(&state.db).await { Ok(job) => (StatusCode::CREATED, Json(job)).into_response(), Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()) }
}
async fn list_jobs(State(state): State<AppState>) -> Response {
    match sqlx::query_as::<_, Job>("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 50").fetch_all(&state.db).await { Ok(jobs) => Json(jobs).into_response(), Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()) }
}
async fn get_job(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    match sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id=$1").bind(id).fetch_optional(&state.db).await { Ok(Some(j)) => Json(j).into_response(), Ok(None) => err(StatusCode::NOT_FOUND, "Job not found"), Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()) }
}
async fn get_results(State(state): State<AppState>, Path(id): Path<Uuid>, Query(page): Query<Page>) -> Response {
    let limit = page.limit.unwrap_or(100).clamp(1, 500); let offset = page.offset.unwrap_or(0).max(0);
    let exists = sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM jobs WHERE id=$1)").bind(id).fetch_one(&state.db).await;
    if !matches!(exists, Ok(true)) { return err(StatusCode::NOT_FOUND, "Job not found"); }
    match sqlx::query_as::<_, ResultRow>("SELECT row_number,original_email,normalized_email,syntax_valid,domain,domain_status,mx_status,mx_host,disposable,smtp_status,result_status,reason FROM email_results WHERE job_id=$1 ORDER BY row_number LIMIT $2 OFFSET $3").bind(id).bind(limit).bind(offset).fetch_all(&state.db).await { Ok(rows) => Json(rows).into_response(), Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()) }
}
async fn export_csv(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    let rows = match sqlx::query_as::<_, ResultRow>("SELECT row_number,original_email,normalized_email,syntax_valid,domain,domain_status,mx_status,mx_host,disposable,smtp_status,result_status,reason FROM email_results WHERE job_id=$1 ORDER BY row_number").bind(id).fetch_all(&state.db).await { Ok(r) => r, Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()) };
    if rows.is_empty() { let exists = sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM jobs WHERE id=$1)").bind(id).fetch_one(&state.db).await.unwrap_or(false); if !exists { return err(StatusCode::NOT_FOUND, "Job not found"); } }
    let mut w = csv::Writer::from_writer(vec![]);
    let _ = w.write_record(["row_number","original_email","normalized_email","syntax_valid","domain","domain_status","mx_status","mx_host","disposable","smtp_status","result_status","reason"]);
    for r in rows { let _ = w.serialize((r.row_number,r.original_email,r.normalized_email,r.syntax_valid,r.domain.unwrap_or_default(),r.domain_status,r.mx_status,r.mx_host.unwrap_or_default(),r.disposable,r.smtp_status,r.result_status,r.reason)); }
    let body = String::from_utf8(w.into_inner().unwrap_or_default()).unwrap_or_default();
    Response::builder().status(StatusCode::OK).header(header::CONTENT_TYPE,"text/csv; charset=utf-8").header(header::CONTENT_DISPOSITION,format!("attachment; filename=\"email-validation-{id}.csv\"")).body(Body::from(body)).unwrap_or_else(|_| err(StatusCode::INTERNAL_SERVER_ERROR,"Could not build CSV"))
}

#[derive(Validate)] struct SyntaxCheck { #[validate(email)] email: String }
fn syntax_valid(email: &str) -> bool { SyntaxCheck { email: email.to_string() }.validate().is_ok() }
fn disposable_domain(domain: &str) -> bool {
    // Starter sample list only. Maintain a current, curated list for production.
    const DOMAINS: &[&str] = &["mailinator.com","guerrillamail.com","10minutemail.com","tempmail.com","yopmail.com","throwawaymail.com","getnada.com","temp-mail.org"];
    DOMAINS.contains(&domain)
}
async fn smtp_greeting(mx: &str) -> String {
    // Host reachability only: do not issue VRFY/EXPN/RCPT TO.
    let address = format!("{mx}:25");
    let stream = match timeout(Duration::from_secs(4), TcpStream::connect(address)).await { Ok(Ok(s)) => s, Ok(Err(_)) => return "unreachable".into(), Err(_) => return "timeout".into() };
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read); let mut line = String::new();
    let greeting = match timeout(Duration::from_secs(4), reader.read_line(&mut line)).await { Ok(Ok(n)) if n > 0 => line.trim().to_string(), Ok(_) => "no_greeting".into(), Err(_) => "timeout".into() };
    let _ = timeout(Duration::from_secs(1), write.write_all(b"QUIT\r\n")).await;
    if greeting.starts_with("220") { "reachable".into() } else if greeting == "timeout" { greeting } else { "inconclusive".into() }
}
async fn validate_one(state: &AppState, job_id: Uuid, row_number: i32, original: String) {
    let normalized = original.trim().to_lowercase();
    let syntax = syntax_valid(&normalized);
    let (domain, domain_status, mx_status, mx_host, disposable, smtp_status, result_status, reason) = if !syntax {
        (None, "not_checked".to_string(), "not_checked".to_string(), None, false, "not_checked".to_string(), "invalid".to_string(), "Invalid email syntax".to_string())
    } else {
        let domain = normalized.rsplit_once('@').map(|(_, d)| d.to_string()).unwrap_or_default();
        let disp = disposable_domain(&domain);
        let lookup = timeout(Duration::from_secs(5), state.resolver.mx_lookup(domain.as_str())).await;
        let (dstatus, mxstatus, mx) = match lookup {
            Ok(Ok(records)) => { let host = records.iter().next().map(|r| r.exchange().to_utf8().trim_end_matches('.').to_string()); ("exists".to_string(), "present".to_string(), host) }
            Ok(Err(_)) => {
                // A/AAAA fallback can be used for domains without MX records.
                let fallback = timeout(Duration::from_secs(3), state.resolver.lookup_ip(domain.as_str())).await;
                match fallback { Ok(Ok(ips)) if ips.iter().next().is_some() => ("exists".to_string(), "no_mx_a_fallback".to_string(), None), Ok(Err(_)) => ("nonexistent_or_no_mail".to_string(), "absent".to_string(), None), _ => ("unknown".to_string(), "unknown".to_string(), None) }
            }
            Err(_) => ("unknown".to_string(), "unknown".to_string(), None),
        };
        let smtp = if state.smtp_enabled { if let Some(host) = mx.as_deref() { smtp_greeting(host).await } else { "not_checked".into() } } else { "disabled".into() };
        let status = if disp { "risky" } else if dstatus == "unknown" || mxstatus == "unknown" { "unknown" } else if mxstatus == "absent" { "invalid" } else { "valid_syntax_domain" };
        let reason = if disp { "Disposable email domain matched" } else if dstatus == "unknown" { "DNS result inconclusive" } else if mxstatus == "absent" { "No MX or A/AAAA fallback found" } else if mxstatus == "no_mx_a_fallback" { "No MX record; domain has address fallback" } else if smtp == "unreachable" || smtp == "timeout" { "Mail host reachability inconclusive; connection failed or timed out" } else { "Syntax and domain checks completed; mailbox not confirmed" };
        (Some(domain), dstatus, mxstatus, mx, disp, smtp, status.to_string(), reason.to_string())
    };
    let mut tx = match state.db.begin().await { Ok(t) => t, Err(e) => { tracing::error!("begin result transaction: {e}"); return; } };
    let save = sqlx::query("UPDATE email_results SET normalized_email=$3,syntax_valid=$4,domain=$5,domain_status=$6,mx_status=$7,mx_host=$8,disposable=$9,smtp_status=$10,result_status=$11,reason=$12 WHERE job_id=$1 AND row_number=$2")
        .bind(job_id).bind(row_number).bind(normalized).bind(syntax).bind(domain).bind(domain_status).bind(mx_status).bind(mx_host).bind(disposable).bind(smtp_status).bind(result_status).bind(reason).execute(&mut *tx).await;
    if let Err(e) = save { tracing::error!("save result: {e}"); return; }
    let _ = sqlx::query("UPDATE jobs SET processed_count=processed_count+1, valid_count=valid_count+CASE WHEN $2='valid_syntax_domain' THEN 1 ELSE 0 END, invalid_count=invalid_count+CASE WHEN $2='invalid' THEN 1 ELSE 0 END, risky_count=risky_count+CASE WHEN $2='risky' THEN 1 ELSE 0 END, unknown_count=unknown_count+CASE WHEN $2='unknown' THEN 1 ELSE 0 END WHERE id=$1")
        .bind(job_id).bind(&result_status).execute(&mut *tx).await;
    let _ = tx.commit().await;
}
async fn worker_loop(state: AppState) {
    loop {
        let claimed = sqlx::query_as::<_, (Uuid,)>("WITH candidate AS (SELECT id FROM jobs WHERE status='queued' ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED) UPDATE jobs SET status='running', started_at=COALESCE(started_at,NOW()) WHERE id=(SELECT id FROM candidate) RETURNING id").fetch_optional(&state.db).await;
        match claimed {
            Ok(Some((job_id,))) => {
                let rows = sqlx::query_as::<_, (i32,String)>("SELECT row_number, original_email FROM email_results WHERE job_id=$1 ORDER BY row_number").bind(job_id).fetch_all(&state.db).await.unwrap_or_default();
                // Bounded parallelism avoids hammering recursive DNS resolvers or mail hosts.
                let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(20));
                let mut tasks = tokio::task::JoinSet::new();
                for (row, email) in rows {
                    let permit = semaphore.clone().acquire_owned().await;
                    if let Ok(permit) = permit { let st = state.clone(); tasks.spawn(async move { let _permit = permit; validate_one(&st, job_id, row, email).await; }); }
                }
                while tasks.join_next().await.is_some() {}
                let _ = sqlx::query("UPDATE jobs SET status='completed', finished_at=NOW() WHERE id=$1").bind(job_id).execute(&state.db).await;
            }
            Ok(None) => tokio::time::sleep(Duration::from_millis(800)).await,
            Err(e) => { tracing::error!("worker queue error: {e}"); tokio::time::sleep(Duration::from_secs(2)).await; }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into())).init();
    let database_url = env::var("DATABASE_URL")?;
    let db = PgPoolOptions::new().max_connections(10).connect(&database_url).await?;
    sqlx::migrate!("./migrations").run(&db).await?;
    // Recover interrupted jobs by resetting their rows and counters before restarting them.
    sqlx::query("UPDATE email_results SET syntax_valid=FALSE,domain=NULL,domain_status='pending',mx_status='pending',mx_host=NULL,disposable=FALSE,smtp_status='not_checked',result_status='pending',reason='Queued for validation' WHERE job_id IN (SELECT id FROM jobs WHERE status='running')").execute(&db).await?;
    sqlx::query("UPDATE jobs SET status='queued',processed_count=0,valid_count=0,invalid_count=0,risky_count=0,unknown_count=0,finished_at=NULL WHERE status='running'").execute(&db).await?;
    let resolver = TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default());
    let state = AppState { db, api_key: env::var("APP_API_KEY")?, max_bytes: env::var("MAX_UPLOAD_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(10*1024*1024), max_emails: env::var("MAX_EMAILS_PER_JOB").ok().and_then(|v| v.parse().ok()).unwrap_or(10_000), smtp_enabled: env::var("SMTP_REACHABILITY_ENABLED").map(|v| v == "true").unwrap_or(false), resolver };
    tokio::spawn(worker_loop(state.clone()));
    let protected = Router::new().route("/api/jobs", post(create_job).get(list_jobs)).route("/api/jobs/:id", get(get_job)).route("/api/jobs/:id/results", get(get_results)).route("/api/jobs/:id/export.csv", get(export_csv)).route_layer(middleware::from_fn_with_state(state.clone(), auth));
    let app = Router::new().route("/health", get(health)).merge(protected).layer(DefaultBodyLimit::max(state.max_bytes + 1024*1024)).layer(CorsLayer::permissive()).layer(TraceLayer::new_for_http()).with_state(state);
    let addr: SocketAddr = env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into()).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("API listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
