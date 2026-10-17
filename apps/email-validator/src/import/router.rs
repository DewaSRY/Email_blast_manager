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
        export::invalid_rows_csv,
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

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", post(import_emails))
        .route("/{import_id}/status", get(get_import_status))
        .route("/{import_id}/invalid", get(download_invalid))
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES + 1024 * 1024))
}
