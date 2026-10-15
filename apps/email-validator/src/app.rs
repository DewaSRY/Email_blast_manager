use axum::{Router, http::StatusCode, response::IntoResponse};
use sqlx::PgPool;
use tower_http::trace::TraceLayer;
use tracing::info;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    common::{ApiResponse, AppError},
    health, import, mailer,
    validation::service::ValidationService,
    validator,
};

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub validation_service: ValidationService,
    pub mailer: Option<mailer::service::MailerService>,
}

impl AppState {
    fn new(db: PgPool) -> Result<Self, hickory_resolver::error::ResolveError> {
        Ok(Self {
            db,
            validation_service: ValidationService::from_env()?,
            mailer: mailer::service::MailerService::from_env().unwrap_or_else(|error| {
                tracing::error!(%error, "Mailer disabled");
                None
            }),
        })
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        health::router::get_health,
        validator::router::validate_email,
        import::router::import_emails,
        import::router::download_invalid,
        mailer::router::send_email
    ),
    components(schemas(
        health::dto::HealthResponse,
        validator::dto::ValidateEmailRequest,
        validator::dto::ValidateEmailResponse,
        ApiResponse<health::dto::HealthResponse>,
        ApiResponse<validator::dto::ValidateEmailResponse>,
        import::dto::ImportEmailResponse,
        ApiResponse<import::dto::ImportEmailResponse>,
        mailer::dto::SendEmailRequest,
        mailer::dto::SendEmailResponse,
        ApiResponse<mailer::dto::SendEmailResponse>
    )),
    tags(
        (name = "Health", description = "Health endpoints"),
        (name = "Validator", description = "Validation endpoints"),
        (name = "Import", description = "Import endpoints"),
        (name = "Mailer", description = "Send mail through KumoMTA")
    )
)]
struct ApiDoc;

pub fn create_app(
    db: PgPool,
) -> Result<Router, hickory_resolver::error::ResolveError> {
    info!("Initializing application state");

    let state = AppState::new( db)?;

    info!("Application state initialized successfully");

    Ok(Router::<AppState>::new()
        .nest("/health", health::router::router())
        .nest("/validator/email", validator::router::router())
        .nest("/import", import::router::router())
        .nest("/mailer", mailer::router::router())
        .route("/import/", axum::routing::post(import::router::import_emails))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(TraceLayer::new_for_http())
        .with_state(state))
}

async fn not_found() -> impl IntoResponse {
    let status = StatusCode::NOT_FOUND;

    tracing::warn!(status = %status, "Route not found");

    AppError::new(status, "Not found").into_response()
}

async fn method_not_allowed() -> impl IntoResponse {
    let status = StatusCode::METHOD_NOT_ALLOWED;

    tracing::warn!(status = %status, "Method not allowed");

    AppError::new(status, "Method not allowed").into_response()
}
