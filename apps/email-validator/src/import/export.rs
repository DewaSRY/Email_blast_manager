use std::collections::HashMap;

use axum::http::StatusCode;
use csv::Writer;
use serde_json::Value;
use sqlx::PgPool;

use crate::common::AppError;

const REASON_COLUMN: &str = "invalid_reason";

fn db_error(error: sqlx::Error) -> AppError {
    tracing::error!(%error, "Database error");
    AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
}

/// Builds a CSV of the batch's invalid rows: original columns plus the reason.
pub async fn invalid_rows_csv(pool: &PgPool, import_id: i64) -> Result<Vec<u8>, AppError> {
    let headers: Option<Vec<String>> =
        sqlx::query_scalar("SELECT original_headers FROM import_batches WHERE id = $1")
            .bind(import_id)
            .fetch_optional(pool)
            .await
            .map_err(db_error)?;
    let headers = headers.ok_or_else(|| AppError::new(StatusCode::NOT_FOUND, "Import not found"))?;

    let rows: Vec<(Value, Option<String>)> = sqlx::query_as(
        "SELECT raw_data, reason_detail FROM invalid_emails WHERE batch_id = $1 ORDER BY row_number",
    )
    .bind(import_id)
    .fetch_all(pool)
    .await
    .map_err(db_error)?;

    let mut writer = Writer::from_writer(Vec::new());
    let csv_error = |_| AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "Could not build CSV");

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

    writer
        .into_inner()
        .map_err(|_| AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "Could not build CSV"))
}
