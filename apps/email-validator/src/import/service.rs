use std::{collections::HashMap, sync::Arc};

use csv::Writer;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::{sync::Semaphore, task::JoinSet};

use crate::{
    common::{
        AppError,
        file_validation::{ParsedSheet, UploadedFile, parse_sheet},
    },
    import::{dto::ImportEmailResponse, mapping::map_row},
    validation::{
        model::{EmailStatus, ValidationOutcome},
        service::ValidationService,
    },
};

const VALIDATION_CONCURRENCY: usize = 32;
const REASON_COLUMN: &str = "invalid_reason";

fn db_error(error: sqlx::Error) -> AppError {
    tracing::error!(%error, "Database error");
    AppError::new(
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "Internal server error",
    )
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

async fn validate_all(
    service: &ValidationService,
    sheet: &ParsedSheet,
) -> Vec<Option<ValidationOutcome>> {
    let semaphore = Arc::new(Semaphore::new(VALIDATION_CONCURRENCY));
    let mut tasks = JoinSet::new();

    for (position, (_, row)) in sheet.rows.iter().enumerate() {
        let email = row[sheet.email_index].trim().to_string();
        if email.is_empty() {
            continue;
        }
        let service = service.clone();
        let semaphore = semaphore.clone();
        tasks.spawn(async move {
            let _permit = semaphore.acquire_owned().await;
            (position, service.validate(&email).await)
        });
    }

    let mut outcomes = vec![None; sheet.rows.len()];
    while let Some(result) = tasks.join_next().await {
        if let Ok((position, outcome)) = result {
            outcomes[position] = Some(outcome);
        }
    }
    outcomes
}

pub async fn import_file(
    pool: &PgPool,
    service: &ValidationService,
    file: UploadedFile,
) -> Result<ImportEmailResponse, AppError> {
    let sheet = parse_sheet(&file)?;
    let file_hash = Sha256::digest(&file.bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    let existing: Option<i64> =
        sqlx::query_scalar("SELECT id FROM import_batches WHERE file_hash = $1")
            .bind(&file_hash)
            .fetch_optional(pool)
            .await
            .map_err(db_error)?;
    if let Some(id) = existing {
        return Err(AppError::new(
            axum::http::StatusCode::CONFLICT,
            format!("This file was already imported (import id {id})"),
        ));
    }

    let outcomes = validate_all(service, &sheet).await;

    let mut tx = pool.begin().await.map_err(db_error)?;
    let batch_id: i64 = sqlx::query_scalar(
        "INSERT INTO import_batches (filename, file_hash, original_headers, status, total_rows, started_at)
         VALUES ($1, $2, $3, 'validating', $4, now()) RETURNING id",
    )
    .bind(&file.filename)
    .bind(&file_hash)
    .bind(&sheet.headers)
    .bind(sheet.rows.len() as i32)
    .fetch_one(&mut *tx)
    .await
    .map_err(db_error)?;

    let (mut valid, mut invalid, mut duplicate) = (0usize, 0usize, 0usize);

    for ((row_number, row), outcome) in sheet.rows.iter().zip(outcomes) {
        let raw_email = row[sheet.email_index].trim();

        let accepted = outcome.as_ref().filter(|o| is_importable(o));

        let Some(outcome) = accepted else {
            let reason = outcome
                .and_then(|o| o.reason_detail)
                .unwrap_or_else(|| "Email is empty".to_string());
            let raw_data: Map<String, Value> = sheet
                .headers
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
            invalid += 1;
            continue;
        };

        let (columns, attributes) = map_row(&sheet.headers, sheet.email_index, row);
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
        .bind(&file.filename)
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
            valid += 1;
        } else {
            duplicate += 1;
        }
    }

    sqlx::query(
        "UPDATE import_batches
         SET status = 'completed', valid_rows = $2, invalid_rows = $3,
             duplicate_rows = $4, finished_at = now()
         WHERE id = $1",
    )
    .bind(batch_id)
    .bind(valid as i32)
    .bind(invalid as i32)
    .bind(duplicate as i32)
    .execute(&mut *tx)
    .await
    .map_err(db_error)?;
    tx.commit().await.map_err(db_error)?;

    Ok(ImportEmailResponse {
        import_id: batch_id,
        filename: file.filename,
        total_rows: sheet.rows.len(),
        valid_rows: valid,
        invalid_rows: invalid,
        duplicate_rows: duplicate,
    })
}

/// Builds a CSV of the batch's invalid rows: original columns plus the reason.
pub async fn invalid_rows_csv(pool: &PgPool, import_id: i64) -> Result<Vec<u8>, AppError> {
    let headers: Option<Vec<String>> =
        sqlx::query_scalar("SELECT original_headers FROM import_batches WHERE id = $1")
            .bind(import_id)
            .fetch_optional(pool)
            .await
            .map_err(db_error)?;
    let headers = headers
        .ok_or_else(|| AppError::new(axum::http::StatusCode::NOT_FOUND, "Import not found"))?;

    let rows: Vec<(Value, Option<String>)> = sqlx::query_as(
        "SELECT raw_data, reason_detail FROM invalid_emails WHERE batch_id = $1 ORDER BY row_number",
    )
    .bind(import_id)
    .fetch_all(pool)
    .await
    .map_err(db_error)?;

    let mut writer = Writer::from_writer(Vec::new());
    let csv_error = |_| {
        AppError::new(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Could not build CSV",
        )
    };

    let mut header_row = headers.clone();
    header_row.push(REASON_COLUMN.to_string());
    writer.write_record(&header_row).map_err(csv_error)?;

    for (raw_data, reason) in rows {
        let cells: HashMap<&str, &str> = raw_data
            .as_object()
            .map(|object| {
                object
                    .iter()
                    .filter_map(|(key, value)| Some((key.as_str(), value.as_str()?)))
                    .collect()
            })
            .unwrap_or_default();

        let mut record: Vec<&str> = headers
            .iter()
            .map(|header| cells.get(header.as_str()).copied().unwrap_or(""))
            .collect();
        record.push(reason.as_deref().unwrap_or(""));
        writer.write_record(&record).map_err(csv_error)?;
    }

    writer.into_inner().map_err(|_| {
        AppError::new(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Could not build CSV",
        )
    })
}
