use axum::{
    Json, Router,
    extract::{Multipart, State},
    routing::post,
};

use crate::{
    app::AppState,
    common::{
        ApiErrorPayload, ApiResponse, AppError,
        file_validation::{MAX_UPLOAD_BYTES, count_emails, read_upload},
    },
    import::dto::ImportEmailResponse,
};

#[utoipa::path(
    post,
    path = "/validator/import",
    tag = "Validator",
    request_body(
        content_type = "multipart/form-data",
        description = "Multipart form with a `file` field (CSV, XLSX or XLS)"
    ),
    responses(
        (status = 200, description = "File parsed successfully", body = ApiResponse<ImportEmailResponse>),
        (status = 400, description = "Invalid upload or file format", body = ApiResponse<ApiErrorPayload>)
    )
)]
pub async fn import_emails(
    State(_state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<ApiResponse<ImportEmailResponse>>, AppError> {
    let file = read_upload(&mut multipart, "file", MAX_UPLOAD_BYTES).await?;
    let stats = count_emails(&file)?;

    Ok(Json(ApiResponse::ok(
        ImportEmailResponse {
            filename: file.filename,
            total_rows: stats.total_rows,
            total_emails: stats.total_emails,
            invalid_rows: stats.total_rows - stats.total_emails,
        },
        "File parsed successfully",
    )))
}

pub fn router() -> Router<AppState> {
    Router::new().route("/", post(import_emails))
}
