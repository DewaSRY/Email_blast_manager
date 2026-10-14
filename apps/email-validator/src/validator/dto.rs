use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::validator::model::EmailValidation;

#[derive(Deserialize, ToSchema)]
pub struct ValidateEmailRequest {
    pub email: String,
}

#[derive(Serialize, ToSchema)]
pub struct ValidateEmailResponse {
    pub email: String,
    pub is_valid: bool,
}

impl From<EmailValidation> for ValidateEmailResponse {
    fn from(model: EmailValidation) -> Self {
        Self {
            email: model.email,
            is_valid: model.is_valid,
        }
    }
}