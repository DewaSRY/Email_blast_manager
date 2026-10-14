use axum::{routing::post, Json, Router};
use utoipa::path;

use crate::validator::{
    dto::{ValidateEmailRequest, ValidateEmailResponse},
    model::EmailValidation,
};

#[utoipa::path(
    post,
    path = "/",
    tag = "Validator",
    request_body = ValidateEmailRequest,
    responses(
        (status = 200, description = "Email validation result", body = ValidateEmailResponse)
    )
)]
async fn validate_email(
    Json(request): Json<ValidateEmailRequest>,
) -> Json<ValidateEmailResponse> {
    let model = EmailValidation::new(request.email);
    Json(ValidateEmailResponse::from(model))
}

pub fn router() -> Router {
    Router::new().route("/", post(validate_email))
}