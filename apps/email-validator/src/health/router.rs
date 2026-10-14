use axum::{Json, Router, routing::get};

use crate::{
    app::AppState,
    common::ApiResponse,
    health::{dto::HealthResponse, model::HealthStatus},
};

#[utoipa::path(
    get,
    path = "/health",
    tag = "Health",
    responses(
        (status = 200, description = "Service health", body = ApiResponse<HealthResponse>)
    )
)]
async fn get_health() -> Json<ApiResponse<HealthResponse>> {
    let model = HealthStatus::new();
    Json(ApiResponse::ok(HealthResponse::from(model), "OK"))
}

pub fn router() -> Router<AppState> {
    Router::new().route("/", get(get_health))
}
