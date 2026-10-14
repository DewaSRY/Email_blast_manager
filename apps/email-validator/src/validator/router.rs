use axum::{
    Json, Router,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
    routing::post,
};

use crate::{
    app::AppState,
    common::{ApiResponse, AppError},
    validator::dto::{ValidateEmailRequest, ValidateEmailResponse},
};

#[utoipa::path(
    post,
    path = "/validator/email",
    tag = "Validator",
    request_body = ValidateEmailRequest,
    responses(
        (status = 200, description = "Email validation result", body = ApiResponse<ValidateEmailResponse>),
        (status = 400, description = "Malformed request", body = ApiResponse<crate::common::ApiErrorPayload>)
    )
)]
async fn validate_email(
    State(state): State<AppState>,
    payload: Result<Json<ValidateEmailRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ValidateEmailResponse>>, AppError> {
    let Json(request) =
        payload.map_err(|_| AppError::new(StatusCode::BAD_REQUEST, "Invalid request body"))?;
    let outcome = state.validation_service.validate(&request.email).await;

    Ok(Json(ApiResponse::ok(outcome, "OK")))
}

pub fn router() -> Router<AppState> {
    Router::new().route("/", post(validate_email))
}
