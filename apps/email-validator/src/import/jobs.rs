use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use axum::http::StatusCode;
use redis::{Client, aio::ConnectionManager};
use serde_json::{Map, Value};
use sqlx::PgPool;
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::{debug, error, info, warn};

use crate::{
    common::{
        AppError,
        file_validation::{RowSource, SheetFormat, StoredUpload, open_rows},
    },
    import::{dto::ImportStatusResponse, mapping::map_row},
    validation::{
        model::{EmailStatus, ValidationOutcome},
        service::ValidationService,
    },
};

const VALIDATION_CONCURRENCY: usize = 64;
const CHUNK_SIZE: usize = 200;
const QUEUE_KEY: &str = "import:queue";
const PROGRESS_TTL_SECS: i64 = 24 * 60 * 60;
const DEFAULT_WORKERS: usize = 8;

type Row = (usize, Vec<String>);

fn db_error(error: sqlx::Error) -> AppError {
    tracing::error!(%error, "Database error");
    AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
}

fn redis_error(error: redis::RedisError) -> AppError {
    tracing::error!(%error, "Redis error");
    AppError::new(StatusCode::SERVICE_UNAVAILABLE, "Import queue unavailable")
}

fn progress_key(batch_id: i64) -> String {
    format!("import:progress:{batch_id}")
}

// Without an SMTP probe the pipeline ends in Unknown/SMTP_PROBE_NOT_CONFIGURED
// once syntax and domain checks pass; that still counts as an importable email.
fn is_importable(outcome: &ValidationOutcome) -> bool {
    match outcome.status {
        EmailStatus::Valid | EmailStatus::Risky => true,
        EmailStatus::Unknown => outcome.reason_code.as_deref() == Some("SMTP_PROBE_NOT_CONFIGURED"),
        EmailStatus::Invalid => false,
    }
}

#[derive(Clone, Default)]
struct Counters {
    total: usize,
    valid: usize,
    invalid: usize,
    duplicate: usize,
}

fn build_status(
    import_id: i64,
    filename: String,
    status: String,
    c: Counters,
    error_message: Option<String>,
) -> ImportStatusResponse {
    let processed_rows = c.valid + c.invalid + c.duplicate;
    let progress_percent = if status == "completed" {
        100.0
    } else if c.total == 0 {
        0.0
    } else {
        (processed_rows as f64 / c.total as f64 * 1000.0).round() / 10.0
    };
    ImportStatusResponse {
        import_id,
        filename,
        finished: matches!(status.as_str(), "completed" | "failed"),
        status,
        total_rows: c.total,
        processed_rows,
        valid_rows: c.valid,
        invalid_rows: c.invalid,
        duplicate_rows: c.duplicate,
        progress_percent,
        error_message,
    }
}

/// Redis-backed job queue and progress store for background imports.
#[derive(Clone)]
pub struct ImportQueue {
    client: Client,
    redis: ConnectionManager,
    upload_dir: PathBuf,
}

impl ImportQueue {
    pub async fn connect(redis_url: &str) -> Result<Self, redis::RedisError> {
        let client = Client::open(redis_url)?;
        let redis = client.get_connection_manager().await?;
        let upload_dir = std::env::var("UPLOAD_DIR").unwrap_or_else(|_| "./data/uploads".into());
        Ok(Self {
            client,
            redis,
            upload_dir: PathBuf::from(upload_dir),
        })
    }

    pub fn upload_dir(&self) -> &Path {
        &self.upload_dir
    }

    async fn enqueue(&self, batch_id: i64) -> Result<(), redis::RedisError> {
        redis::cmd("RPUSH")
            .arg(QUEUE_KEY)
            .arg(batch_id)
            .query_async::<()>(&mut self.redis.clone())
            .await
    }

    async fn publish(
        &self,
        batch_id: i64,
        status: &str,
        filename: &str,
        c: &Counters,
        error: &str,
    ) {
        let key = progress_key(batch_id);
        let result = redis::pipe()
            .hset_multiple(
                &key,
                &[
                    ("status", status.to_string()),
                    ("filename", filename.to_string()),
                    ("total", c.total.to_string()),
                    ("valid", c.valid.to_string()),
                    ("invalid", c.invalid.to_string()),
                    ("duplicate", c.duplicate.to_string()),
                    ("error", error.to_string()),
                ],
            )
            .ignore()
            .expire(&key, PROGRESS_TTL_SECS)
            .ignore()
            .query_async::<()>(&mut self.redis.clone())
            .await;
        if let Err(error) = result {
            tracing::warn!(%error, batch_id, "Could not publish import progress");
        }
    }

    async fn cached_status(&self, batch_id: i64) -> Option<ImportStatusResponse> {
        let fields: HashMap<String, String> = redis::cmd("HGETALL")
            .arg(progress_key(batch_id))
            .query_async(&mut self.redis.clone())
            .await
            .ok()?;
        let number = |name: &str| fields.get(name).and_then(|v| v.parse().ok()).unwrap_or(0);
        let error = fields.get("error").filter(|e| !e.is_empty()).cloned();

        Some(build_status(
            batch_id,
            fields.get("filename")?.clone(),
            fields.get("status")?.clone(),
            Counters {
                total: number("total"),
                valid: number("valid"),
                invalid: number("invalid"),
                duplicate: number("duplicate"),
            },
            error,
        ))
    }

    /// Starts the worker pool and re-queues batches left unfinished by a restart.
    pub fn spawn_workers(&self, pool: PgPool, validator: ValidationService) {
        let workers = std::env::var("IMPORT_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_WORKERS);

        let queue = self.clone();
        let recover_pool = pool.clone();
        tokio::spawn(async move {
            // Unfinished batches are re-run from scratch; their partial rows are cleared first.
            let ids: Result<Vec<i64>, _> = sqlx::query_scalar(
                "SELECT id FROM import_batches WHERE status IN ('queued','validating') ORDER BY id",
            )
            .fetch_all(&recover_pool)
            .await;
            // Drop stale queue entries so recovered ids are not queued twice.
            let _ = redis::cmd("DEL")
                .arg(QUEUE_KEY)
                .query_async::<()>(&mut queue.redis.clone())
                .await;
            for id in ids.unwrap_or_default() {
                if let Err(error) = queue.enqueue(id).await {
                    tracing::error!(%error, id, "Could not re-queue import");
                }
            }
        });

        for worker in 0..workers {
            let queue = self.clone();
            let pool = pool.clone();
            let validator = validator.clone();
            tokio::spawn(async move { queue.worker_loop(worker, pool, validator).await });
        }
        tracing::info!(workers, "Import workers started");
    }

    async fn worker_loop(&self, worker: usize, pool: PgPool, validator: ValidationService) {
        let mut conn = self.redis.clone();
        loop {
            let popped: redis::RedisResult<Option<(String, String)>> = redis::cmd("BLPOP")
                .arg(QUEUE_KEY)
                .arg(5)
                .query_async(&mut conn)
                .await;

            match popped {
                Ok(Some((_, batch_id))) => {
                    let batch_id: i64 = match batch_id.parse() {
                        Ok(id) => id,
                        Err(error) => {
                            tracing::error!(%error, "Invalid batch ID from Redis");
                            continue;
                        }
                    };

                    tracing::info!(worker, batch_id, "Import started");

                    if let Err(error) = self.process_batch(&pool, &validator, batch_id).await {
                        let detail = error.detail();
                        tracing::error!(batch_id, %detail, "Import failed");
                        self.fail_batch(&pool, batch_id, &detail).await;
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(%error, worker, "Queue pop failed");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    break;
                }
            }
        }
    }

    async fn fail_batch(&self, pool: &PgPool, batch_id: i64, message: &str) {
        let row: Result<Option<(String, i32, i32, i32, i32)>, _> = sqlx::query_as(
            "UPDATE import_batches
             SET status = 'failed', error_message = $2, finished_at = now()
             WHERE id = $1
             RETURNING filename, total_rows, valid_rows, invalid_rows, duplicate_rows",
        )
        .bind(batch_id)
        .bind(message)
        .fetch_optional(pool)
        .await;

        if let Ok(Some((filename, total, valid, invalid, duplicate))) = row {
            let counters = Counters {
                total: total as usize,
                valid: valid as usize,
                invalid: invalid as usize,
                duplicate: duplicate as usize,
            };
            self.publish(batch_id, "failed", &filename, &counters, message)
                .await;
        }
    }

    async fn process_batch(
        &self,
        pool: &PgPool,
        validator: &ValidationService,
        batch_id: i64,
    ) -> Result<(), AppError> {
        let (filename, storage_path): (String, Option<String>) = sqlx::query_as(
            "UPDATE import_batches
             SET status = 'validating', started_at = now(), error_message = NULL,
                 valid_rows = 0, invalid_rows = 0, duplicate_rows = 0
             WHERE id = $1 AND status IN ('queued','validating')
             RETURNING filename, storage_path",
        )
        .bind(batch_id)
        .fetch_optional(pool)
        .await
        .map_err(db_error)?
        .ok_or_else(|| AppError::new(StatusCode::NOT_FOUND, "Import is not runnable"))?;

        let path = PathBuf::from(
            storage_path
                .ok_or_else(|| AppError::new(StatusCode::NOT_FOUND, "Stored file missing"))?,
        );
        let format = SheetFormat::from_filename(&path.to_string_lossy())?;

        sqlx::query("DELETE FROM contact_sources WHERE batch_id = $1")
            .bind(batch_id)
            .execute(pool)
            .await
            .map_err(db_error)?;
        sqlx::query("DELETE FROM invalid_emails WHERE batch_id = $1")
            .bind(batch_id)
            .execute(pool)
            .await
            .map_err(db_error)?;

        let mut counters = Counters::default();
        self.publish(batch_id, "validating", &filename, &counters, "")
            .await;

        // First pass counts rows so progress has a denominator.
        let count_path = path.clone();
        counters.total = tokio::task::spawn_blocking(move || -> Result<usize, AppError> {
            let source = open_rows(&count_path, format)?;
            let mut total = 0;
            for row in source.rows {
                row?;
                total += 1;
            }
            Ok(total)
        })
        .await
        .map_err(|_| AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "Import worker crashed"))??;

        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Result<Vec<Row>, AppError>>(2);
        let (meta_sender, meta_receiver) = tokio::sync::oneshot::channel();
        let reader_path = path.clone();
        tokio::task::spawn_blocking(move || {
            let RowSource {
                headers,
                email_index,
                rows,
            } = match open_rows(&reader_path, format) {
                Ok(source) => source,
                Err(error) => {
                    let _ = meta_sender.send(Err(error));
                    return;
                }
            };
            let _ = meta_sender.send(Ok((headers, email_index)));

            let mut chunk = Vec::with_capacity(CHUNK_SIZE);
            for row in rows {
                match row {
                    Ok(row) => chunk.push(row),
                    Err(error) => {
                        let _ = sender.blocking_send(Err(error));
                        return;
                    }
                }
                if chunk.len() >= CHUNK_SIZE {
                    let full = std::mem::replace(&mut chunk, Vec::with_capacity(CHUNK_SIZE));
                    if sender.blocking_send(Ok(full)).is_err() {
                        return;
                    }
                }
            }
            if !chunk.is_empty() {
                let _ = sender.blocking_send(Ok(chunk));
            }
        });

        let (headers, email_index): (Vec<String>, usize) =
            meta_receiver.await.map_err(|_| {
                AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "Import reader crashed")
            })??;

        sqlx::query(
            "UPDATE import_batches SET original_headers = $2, total_rows = $3 WHERE id = $1",
        )
        .bind(batch_id)
        .bind(&headers)
        .bind(counters.total as i32)
        .execute(pool)
        .await
        .map_err(db_error)?;

        while let Some(chunk) = receiver.recv().await {
            let chunk = chunk?;
            let outcomes = validate_chunk(validator, &chunk, email_index).await;
            insert_chunk(
                pool,
                batch_id,
                &filename,
                &headers,
                email_index,
                chunk,
                outcomes,
                &mut counters,
            )
            .await?;
            self.publish(batch_id, "validating", &filename, &counters, "")
                .await;
        }

        sqlx::query(
            "UPDATE import_batches SET status = 'completed', finished_at = now() WHERE id = $1",
        )
        .bind(batch_id)
        .execute(pool)
        .await
        .map_err(db_error)?;
        self.publish(batch_id, "completed", &filename, &counters, "")
            .await;

        if let Err(error) = tokio::fs::remove_file(&path).await {
            tracing::warn!(%error, "Could not delete processed upload");
        }
        tracing::info!(batch_id, total = counters.total, "Import completed");
        Ok(())
    }
}

async fn validate_chunk(
    service: &ValidationService,
    chunk: &[Row],
    email_index: usize,
) -> Vec<Option<ValidationOutcome>> {
    let started_at = Instant::now();
    let chunk_size = chunk.len();

    let semaphore = Arc::new(Semaphore::new(VALIDATION_CONCURRENCY));
    let mut tasks = JoinSet::new();

    let mut submitted = 0usize;
    let mut skipped_empty = 0usize;
    let mut skipped_invalid = 0usize;

    info!(
        chunk_size,
        email_index,
        concurrency = VALIDATION_CONCURRENCY,
        "Email validation chunk started"
    );

    for (position, (_, row)) in chunk.iter().enumerate() {
        let email = match row.get(email_index) {
            Some(value) => value.trim().to_string(),
            None => {
                warn!(
                    position,
                    email_index, "Skipping row: email column index out of bounds"
                );
                skipped_invalid += 1;
                continue;
            }
        };

        if email.is_empty() {
            skipped_empty += 1;
            debug!(position, "Skipping row: empty email");
            continue;
        }

        let service = service.clone();
        let semaphore = Arc::clone(&semaphore);

        submitted += 1;

        tasks.spawn(async move {
            let permit = match semaphore.acquire_owned().await {
                Ok(permit) => permit,
                Err(error) => {
                    return (position, None, Some(error.to_string()), 0u128);
                }
            };

            let validation_started = Instant::now();
            let outcome = service.validate(&email).await;
            let elapsed_ms = validation_started.elapsed().as_millis();

            drop(permit);

            (position, Some(outcome), None, elapsed_ms)
        });
    }

    let mut outcomes = vec![None; chunk_size];
    let mut completed = 0usize;
    let mut failed_tasks = 0usize;

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((position, Some(outcome), None, elapsed_ms)) => {
                debug!(position, elapsed_ms, "Email validation completed");

                outcomes[position] = Some(outcome);
                completed += 1;
            }
            Ok((position, None, Some(detail), _)) => {
                error!(
                    position,
                    %detail,
                    "Failed to acquire validation semaphore"
                );
                failed_tasks += 1;
            }
            Ok(_) => {
                error!("Unexpected validation task result");
                failed_tasks += 1;
            }
            Err(join_error) => {
                error!(
                    %join_error,
                    "Email validation task panicked or was cancelled"
                );
                failed_tasks += 1;
            }
        }
    }

    info!(
        chunk_size,
        submitted,
        completed,
        skipped_empty,
        skipped_invalid,
        failed_tasks,
        elapsed_ms = started_at.elapsed().as_millis(),
        "Email validation chunk finished"
    );

    outcomes
}

#[allow(clippy::too_many_arguments)]
async fn insert_chunk(
    pool: &PgPool,
    batch_id: i64,
    filename: &str,
    headers: &[String],
    email_index: usize,
    chunk: Vec<Row>,
    outcomes: Vec<Option<ValidationOutcome>>,
    counters: &mut Counters,
) -> Result<(), AppError> {
    let mut tx = pool.begin().await.map_err(db_error)?;
    let mut local = Counters::default();

    for ((row_number, row), outcome) in chunk.iter().zip(outcomes) {
        let raw_email = row[email_index].trim();
        let accepted = outcome.as_ref().filter(|o| is_importable(o));

        let Some(outcome) = accepted else {
            let reason = outcome
                .and_then(|o| o.reason_detail)
                .unwrap_or_else(|| "Email is empty".to_string());
            let raw_data: Map<String, Value> = headers
                .iter()
                .zip(row)
                .map(|(header, cell)| (header.clone(), Value::String(cell.clone())))
                .collect();

            sqlx::query(
                "INSERT INTO invalid_emails (batch_id, row_number, raw_email, raw_data, reason_detail)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(batch_id)
            .bind(*row_number as i32)
            .bind(raw_email)
            .bind(Value::Object(raw_data))
            .bind(reason)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
            local.invalid += 1;
            continue;
        };

        let (columns, attributes) = map_row(headers, email_index, row);
        let (contact_id, inserted): (i64, bool) = sqlx::query_as(
            "INSERT INTO contacts
                (email, first_name, last_name, full_name, company, job_title, industry,
                 country, city, phone, source_filename, attributes, mx_valid,
                 is_disposable, is_role_account, validated_at, first_batch_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,TRUE,$13,$14,now(),$15)
             ON CONFLICT (email) DO UPDATE SET email = EXCLUDED.email
             RETURNING id, (xmax = 0)",
        )
        .bind(&outcome.email)
        .bind(columns.first_name)
        .bind(columns.last_name)
        .bind(columns.full_name)
        .bind(columns.company)
        .bind(columns.job_title)
        .bind(columns.industry)
        .bind(columns.country)
        .bind(columns.city)
        .bind(columns.phone)
        .bind(filename)
        .bind(attributes)
        .bind(outcome.is_disposable)
        .bind(outcome.is_role)
        .bind(batch_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db_error)?;

        sqlx::query(
            "INSERT INTO contact_sources (contact_id, batch_id, row_number) VALUES ($1, $2, $3)",
        )
        .bind(contact_id)
        .bind(batch_id)
        .bind(*row_number as i32)
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;

        if inserted {
            local.valid += 1;
        } else {
            local.duplicate += 1;
        }
    }

    sqlx::query(
        "UPDATE import_batches SET valid_rows = $2, invalid_rows = $3, duplicate_rows = $4 WHERE id = $1",
    )
    .bind(batch_id)
    .bind((counters.valid + local.valid) as i32)
    .bind((counters.invalid + local.invalid) as i32)
    .bind((counters.duplicate + local.duplicate) as i32)
    .execute(&mut *tx)
    .await
    .map_err(db_error)?;
    tx.commit().await.map_err(db_error)?;

    counters.valid += local.valid;
    counters.invalid += local.invalid;
    counters.duplicate += local.duplicate;
    Ok(())
}

/// Registers an uploaded file as a queued batch and hands it to the workers.
pub async fn submit_import(
    pool: &PgPool,
    queue: &ImportQueue,
    upload: StoredUpload,
) -> Result<ImportStatusResponse, AppError> {
    let result = register(pool, queue, &upload).await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&upload.path).await;
    }
    result
}

async fn register(
    pool: &PgPool,
    queue: &ImportQueue,
    upload: &StoredUpload,
) -> Result<ImportStatusResponse, AppError> {
    let existing: Option<i64> =
        sqlx::query_scalar("SELECT id FROM import_batches WHERE file_hash = $1")
            .bind(&upload.sha256)
            .fetch_optional(pool)
            .await
            .map_err(db_error)?;
    if let Some(id) = existing {
        return Err(AppError::new(
            StatusCode::CONFLICT,
            format!("This file was already imported (import id {id})"),
        ));
    }

    // CSV headers are cheap to check up front; Excel files are checked by the worker.
    let mut headers: Vec<String> = Vec::new();
    if upload.format == SheetFormat::Csv {
        let (path, format) = (upload.path.clone(), upload.format);
        headers = tokio::task::spawn_blocking(move || open_rows(&path, format).map(|s| s.headers))
            .await
            .map_err(|_| {
                AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "Header check failed")
            })??;
    }

    let extension = match upload.format {
        SheetFormat::Csv => "csv",
        SheetFormat::Xlsx => "xlsx",
        SheetFormat::Xls => "xls",
    };
    let final_path = queue
        .upload_dir()
        .join(format!("{}.{extension}", upload.sha256));
    tokio::fs::rename(&upload.path, &final_path)
        .await
        .map_err(|error| {
            tracing::error!(%error, "Could not store upload");
            AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
        })?;

    let inserted: Option<i64> = sqlx::query_scalar(
        "INSERT INTO import_batches (filename, file_hash, storage_path, original_headers, status)
         VALUES ($1, $2, $3, $4, 'queued')
         ON CONFLICT (file_hash) DO NOTHING RETURNING id",
    )
    .bind(&upload.filename)
    .bind(&upload.sha256)
    .bind(final_path.to_string_lossy().as_ref())
    .bind(&headers)
    .fetch_optional(pool)
    .await
    .map_err(db_error)?;

    let Some(batch_id) = inserted else {
        // A concurrent upload of the same file won the race; keep its stored copy.
        return Err(AppError::new(
            StatusCode::CONFLICT,
            "This file was already imported",
        ));
    };

    if let Err(error) = queue.enqueue(batch_id).await {
        let _ = sqlx::query("DELETE FROM import_batches WHERE id = $1")
            .bind(batch_id)
            .execute(pool)
            .await;
        let _ = tokio::fs::remove_file(&final_path).await;
        return Err(redis_error(error));
    }

    let counters = Counters::default();
    queue
        .publish(batch_id, "queued", &upload.filename, &counters, "")
        .await;
    Ok(build_status(
        batch_id,
        upload.filename.clone(),
        "queued".into(),
        counters,
        None,
    ))
}

/// Progress of a batch: Redis first (cheap to poll), the database as fallback.
pub async fn import_status(
    pool: &PgPool,
    queue: &ImportQueue,
    batch_id: i64,
) -> Result<ImportStatusResponse, AppError> {
    if let Some(status) = queue.cached_status(batch_id).await {
        return Ok(status);
    }

    let row: Option<(String, String, i32, i32, i32, i32, Option<String>)> = sqlx::query_as(
        "SELECT filename, status, total_rows, valid_rows, invalid_rows, duplicate_rows, error_message
         FROM import_batches WHERE id = $1",
    )
    .bind(batch_id)
    .fetch_optional(pool)
    .await
    .map_err(db_error)?;
    let (filename, status, total, valid, invalid, duplicate, error) =
        row.ok_or_else(|| AppError::new(StatusCode::NOT_FOUND, "Import not found"))?;

    Ok(build_status(
        batch_id,
        filename,
        status,
        Counters {
            total: total as usize,
            valid: valid as usize,
            invalid: invalid as usize,
            duplicate: duplicate as usize,
        },
        error,
    ))
}
