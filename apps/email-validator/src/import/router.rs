use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    routing::{get, post},
};

use crate::{
    app::AppState,
    common::{
        ApiErrorPayload, ApiResponse, AppError,
        file_validation::{MAX_UPLOAD_BYTES, read_upload},
    },
    import::{
        dto::{ImportEmailForm, ImportStatusResponse},
        export::{import_result_csv, invalid_rows_csv},
        jobs::{import_status, submit_import},
    },
};

#[utoipa::path(
    post,
    path = "/import",
    tag = "Import",
    request_body(
        content = ImportEmailForm,
        content_type = "multipart/form-data",
        description = "Multipart form with a `file` field (CSV, XLSX or XLS) that has an `email` column"
    ),
    responses(
        (status = 202, description = "Import queued; poll `/import/{import_id}/status`", body = ApiResponse<ImportStatusResponse>),
        (status = 400, description = "Invalid upload or file format", body = ApiResponse<ApiErrorPayload>),
        (status = 409, description = "File already imported", body = ApiResponse<ApiErrorPayload>)
    )
)]
pub async fn import_emails(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<ApiResponse<ImportStatusResponse>>), AppError> {
    let file = read_upload(
        &mut multipart,
        "file",
        MAX_UPLOAD_BYTES,
        state.imports.upload_dir(),
    )
    .await?;
    let result = submit_import(&state.db, &state.imports, file).await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(ApiResponse::with_status(
            Some(result),
            StatusCode::ACCEPTED,
            "File accepted for processing",
        )),
    ))
}

#[utoipa::path(
    get,
    path = "/import/{import_id}/status",
    tag = "Import",
    params(("import_id" = i64, Path, description = "Import id returned by the upload")),
    responses(
        (status = 200, description = "Import progress", body = ApiResponse<ImportStatusResponse>),
        (status = 404, description = "Import not found", body = ApiResponse<ApiErrorPayload>)
    )
)]
pub async fn get_import_status(
    State(state): State<AppState>,
    Path(import_id): Path<i64>,
) -> Result<Json<ApiResponse<ImportStatusResponse>>, AppError> {
    let status = import_status(&state.db, &state.imports, import_id).await?;
    Ok(Json(ApiResponse::ok(status, "Import status")))
}

#[utoipa::path(
    get,
    path = "/import/{import_id}/invalid",
    tag = "Import", 
    params(("import_id" = i64, Path, description = "Import id returned by the upload")),
    responses(
        (status = 200, description = "CSV of invalid rows", content_type = "text/csv"),
        (status = 404, description = "Import not found", body = ApiResponse<ApiErrorPayload>)
    )
)]
pub async fn download_invalid(
    State(state): State<AppState>,
    Path(import_id): Path<i64>,
) -> Result<(HeaderMap, Vec<u8>), AppError> {
    let body = invalid_rows_csv(&state.db, import_id).await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/csv; charset=utf-8"),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "attachment; filename=\"import-{import_id}-invalid.csv\""
        ))
        .expect("ascii header value"),
    );
    Ok((headers, body))
}

#[utoipa::path(
    get,
    path = "/import/{import_id}/result",
    tag = "Import",
    params(("import_id" = i64, Path, description = "Import id returned by the upload")),
    responses(
        (status = 200, description = "CSV of accepted rows only, in original row order", content_type = "text/csv"),
        (status = 404, description = "Import not found", body = ApiResponse<ApiErrorPayload>),
        (status = 409, description = "Import results are not ready", body = ApiResponse<ApiErrorPayload>)
    )
)]
pub async fn download_result(
    State(state): State<AppState>,
    Path(import_id): Path<i64>,
) -> Result<(HeaderMap, Vec<u8>), AppError> {
    let body = import_result_csv(&state.db, import_id).await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/csv; charset=utf-8"),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "attachment; filename=\"import-{import_id}-results.csv\""
        ))
        .expect("ascii header value"),
    );
    Ok((headers, body))
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", post(import_emails))
        .route("/{import_id}/status", get(get_import_status))
        .route("/{import_id}/invalid", get(download_invalid))
        .route("/{import_id}/result", get(download_result))
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES + 1024 * 1024))
}

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;
    use sqlx::postgres::PgPoolOptions;

    use super::*;
    use crate::{import::jobs::ImportQueue, validation::service::ValidationService};

    #[tokio::test]
    #[ignore = "requires local Postgres and Redis; uses temporary tables only"]
    async fn downloads_only_batch_contacts_and_checks_readiness() {
        let config = crate::config::Config::from_env().unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&config.database_url)
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TEMP TABLE import_batches (
                id BIGINT, original_headers TEXT[], status TEXT
             );
             CREATE TEMP TABLE contacts (
                id BIGINT, email TEXT, first_name TEXT, last_name TEXT, full_name TEXT,
                company TEXT, job_title TEXT, industry TEXT, country TEXT, city TEXT,
                phone TEXT, attributes JSONB
             );
             CREATE TEMP TABLE contact_sources (
                contact_id BIGINT, batch_id BIGINT, row_number INT
             );
             CREATE TEMP TABLE invalid_emails (
                batch_id BIGINT, row_number INT, raw_data JSONB, reason_detail TEXT
             );
             INSERT INTO import_batches VALUES
                (10, ARRAY['Email', 'First Name', 'Note'], 'completed'),
                (11, ARRAY['Email'], 'completed'),
                (12, ARRAY['Email'], 'queued'),
                (13, ARRAY['Email'], 'validating'),
                (14, ARRAY['Email'], 'failed'),
                (15, ARRAY['Email'], 'completed'),
                (16, ARRAY['Name'], 'completed');
             INSERT INTO contacts (id, email, first_name, attributes) VALUES
                (1, 'a@example.com', 'Ann', '{\"Note\":\"hello, world\"}'),
                (2, 'b@example.com', 'Bob', '{}'),
                (3, 'other@example.com', 'Other', '{}');
             INSERT INTO contact_sources VALUES
                (1, 10, 4), (2, 10, 2), (1, 10, 5), (3, 11, 1);
             INSERT INTO invalid_emails VALUES
                (10, 3, '{\"Email\":\"bad\",\"First Name\":\"Invalid\"}', 'Bad syntax');",
        )
        .execute(&pool)
        .await
        .unwrap();
        let state = AppState {
            db: pool.clone(),
            imports: ImportQueue::connect(&config.redis_url).await.unwrap(),
            validation_service: ValidationService::from_env().unwrap(),
            mailer: None,
        };

        let (headers, body) = download_result(State(state.clone()), Path(10))
            .await
            .unwrap();
        assert_eq!(headers[header::CONTENT_TYPE], "text/csv; charset=utf-8");
        assert_eq!(
            headers[header::CONTENT_DISPOSITION],
            "attachment; filename=\"import-10-results.csv\""
        );
        assert_eq!(
            String::from_utf8(body).unwrap(),
            "Email,First Name,Note\nb@example.com,Bob,\na@example.com,Ann,\"hello, world\"\na@example.com,Ann,\"hello, world\"\n"
        );

        let (_, body) = download_result(State(state.clone()), Path(15))
            .await
            .unwrap();
        assert_eq!(String::from_utf8(body).unwrap(), "Email\n");
        for (id, expected) in [
            (999, StatusCode::NOT_FOUND),
            (12, StatusCode::CONFLICT),
            (13, StatusCode::CONFLICT),
            (14, StatusCode::CONFLICT),
            (16, StatusCode::INTERNAL_SERVER_ERROR),
        ] {
            let response = download_result(State(state.clone()), Path(id))
                .await
                .unwrap_err()
                .into_response();
            assert_eq!(response.status(), expected);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(json["code"], expected.as_u16());
            assert!(json["data"]["errors"].is_array());
        }

        let (_, body) = download_invalid(State(state), Path(10)).await.unwrap();
        assert_eq!(
            String::from_utf8(body).unwrap(),
            "Email,First Name,Note,invalid_reason\nbad,Invalid,,Bad syntax\n"
        );
        pool.close().await;
    }
}
