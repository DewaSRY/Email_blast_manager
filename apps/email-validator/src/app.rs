use axum::Router;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    health,
    validator,
};

#[derive(OpenApi)]
#[openapi(
    paths(
        health::router::get_health,
        validator::router::validate_email
    ),
    components(schemas(
        health::dto::HealthResponse,
        validator::dto::ValidateEmailRequest,
        validator::dto::ValidateEmailResponse
    )),
    tags(
        (name = "Health", description = "Health endpoints"),
        (name = "Validator", description = "Validation endpoints")
    )
)]
struct ApiDoc;

pub fn create_app() -> Router {
    Router::new()
        .nest("/health", health::router::router())
        .nest("/validator/email", validator::router::router())
        .merge(
            SwaggerUi::new("/swagger-ui")
                .url("/api-docs/openapi.json", ApiDoc::openapi()),
        )
}