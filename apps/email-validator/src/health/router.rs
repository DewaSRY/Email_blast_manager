use axum::{routing::get, Json, Router};
use utoipa::path;

use crate::health::{
    dto::HealthResponse,
    model::HealthStatus,
};

#[utoipa::path(
    get,
    path = "/health",
    tag = "Health",
    responses(
        (status = 200, description = "Service health", body = HealthResponse)
    )
)]
async fn get_health() -> Json<HealthResponse> {
    let model = HealthStatus::new();
    Json(HealthResponse::from(model))
}

pub fn router() -> Router {
    Router::new().route("/", get(get_health))
}