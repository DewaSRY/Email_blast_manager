use axum::{
    extract::Json,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
struct ValidateEmailRequest {
    #[validate(email)]
    email: String,
}

#[derive(Serialize)]
struct ValidateEmailResponse {
    email: String,
    syntax_valid: bool,
    message: String,
}

async fn health() -> &'static str {
    "Email validation service is running"
}

async fn validate_email(
    Json(payload): Json<ValidateEmailRequest>,
) -> (StatusCode, Json<ValidateEmailResponse>) {
    let valid = payload.validate().is_ok();

    let response = ValidateEmailResponse {
        email: payload.email,
        syntax_valid: valid,
        message: if valid {
            "Email format is valid".to_string()
        } else {
            "Email format is invalid".to_string()
        },
    };

    let status = if valid {
        StatusCode::OK
    } else {
        StatusCode::BAD_REQUEST
    };

    (status, Json(response))
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/health", get(health))
        .route("/validate", post(validate_email));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("Failed to bind port 3000");

    println!("Server running at http://127.0.0.1:3000");

    axum::serve(listener, app)
        .await
        .expect("Server failed");
}
