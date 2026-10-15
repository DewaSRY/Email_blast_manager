use axum::{
    Json, Router,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
    routing::post,
};

use crate::{
    app::AppState,
    common::{ApiResponse, AppError},
    mailer::dto::{SendEmailRequest, SendEmailResponse},
};

#[utoipa::path(
    post,
    path = "/mailer/send",
    tag = "Mailer",
    request_body = SendEmailRequest,
    responses(
        (status = 200, description = "Message queued in KumoMTA", body = ApiResponse<SendEmailResponse>),
        (status = 502, description = "KumoMTA rejected or is unreachable", body = ApiResponse<crate::common::ApiErrorPayload>),
        (status = 503, description = "Mailer not configured", body = ApiResponse<crate::common::ApiErrorPayload>)
    )
)]
async fn send_email(
    State(state): State<AppState>,
    payload: Result<Json<SendEmailRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<SendEmailResponse>>, AppError> {
    let Json(request) =
        payload.map_err(|_| AppError::bad_request("body", "Malformed or invalid request body"))?;
    let mailer = state.mailer.as_ref().ok_or_else(|| {
        AppError::new(StatusCode::SERVICE_UNAVAILABLE, "Mailer not configured (set KUMO_SMTP_PASSWORD)")
    })?;

    let smtp_response = mailer
        .send(&request.to, &request.subject, &request.body)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "KumoMTA submission failed");
            AppError::new(StatusCode::BAD_GATEWAY, format!("KumoMTA submission failed: {e}"))
        })?;

    Ok(Json(ApiResponse::ok(
        SendEmailResponse { accepted: true, smtp_response },
        "Queued",
    )))
}

pub fn router() -> Router<AppState> {
    Router::new().route("/send", post(send_email))
}
