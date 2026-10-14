use axum::{
    http::StatusCode,
    response::IntoResponse,
    Json, Router,
};
use tower_http::trace::TraceLayer;
use tracing::info;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    common::ApiResponse,
    health,
    validation::service::ValidationService,
    validator,
    config::Config,
};

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub validation_service: ValidationService,
}

impl AppState {
    fn new(config: Config) -> Result<Self, hickory_resolver::error::ResolveError> {
        Ok(Self {
            validation_service: ValidationService::system()?,
            config,
        })
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        health::router::get_health,
        validator::router::validate_email
    ),
    components(schemas(
        health::dto::HealthResponse,
        validator::dto::ValidateEmailRequest,
        validator::dto::ValidateEmailResponse,
        ApiResponse<health::dto::HealthResponse>,
        ApiResponse<validator::dto::ValidateEmailResponse>
    )),
    tags(
        (name = "Health", description = "Health endpoints"),
        (name = "Validator", description = "Validation endpoints")
    )
)]
struct ApiDoc;

pub fn create_app(config: Config) -> Result<Router, hickory_resolver::error::ResolveError> {
    info!("Initializing application state");

    let state = AppState::new(config)?;

    info!("Application state initialized successfully");

    Ok(
        Router::<AppState>::new()
            .nest("/health", health::router::router())
            .nest("/validator/email", validator::router::router())
            .merge(
                SwaggerUi::new("/swagger-ui")
                    .url("/api-docs/openapi.json", ApiDoc::openapi()),
            )
            .fallback(not_found)
            .method_not_allowed_fallback(method_not_allowed)
            .layer(TraceLayer::new_for_http())
            .with_state(state),
    )
}

async fn not_found() -> impl IntoResponse {
    let status = StatusCode::NOT_FOUND;

    tracing::warn!(status = %status, "Route not found");

    (
        status,
        Json(ApiResponse::<String>::with_status(
            None,
            status,
            "Not found",
        )),
    )
}

async fn method_not_allowed() -> impl IntoResponse {
    let status = StatusCode::METHOD_NOT_ALLOWED;

    tracing::warn!(status = %status, "Method not allowed");

    (
        status,
        Json(ApiResponse::<String>::with_status(
            None,
            status,
            "Method not allowed",
        )),
    )
}
