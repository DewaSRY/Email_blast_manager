use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::{HeaderMap, HeaderValue, header},
    routing::{get, post},
};

use crate::{
    app::AppState,
    common::{
        ApiErrorPayload, ApiResponse, AppError,
        file_validation::{MAX_UPLOAD_BYTES, read_upload},
    },
    import::{
        dto::ImportEmailResponse,
        service::{import_file, invalid_rows_csv},
    },
};

#[utoipa::path(
    post,
    path = "/import",
    tag = "Import",
    request_body(
        content_type = "multipart/form-data",
        description = "Multipart form with a `file` field (CSV, XLSX or XLS) that has an `email` column"
    ),
    responses(
        (status = 200, description = "File imported", body = ApiResponse<ImportEmailResponse>),
        (status = 400, description = "Invalid upload or file format", body = ApiResponse<ApiErrorPayload>),
        (status = 409, description = "File already imported", body = ApiResponse<ApiErrorPayload>)
    )
)]
pub async fn import_emails(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<ApiResponse<ImportEmailResponse>>, AppError> {
    let file = read_upload(&mut multipart, "file", MAX_UPLOAD_BYTES).await?;
    let result = import_file(&state.db, &state.validation_service, file).await?;

    Ok(Json(ApiResponse::ok(result, "File imported successfully")))
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
        .route("/{import_id}/invalid", get(download_invalid))
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES + 1024 * 1024))
}
