use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Deserialize, ToSchema)]
pub struct SendEmailRequest {
    pub to: String,
    pub subject: String,
    pub body: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SendEmailResponse {
    pub accepted: bool,
    pub smtp_response: String,
}
