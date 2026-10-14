use serde::Deserialize;
use utoipa::ToSchema;

use crate::validator::model::ValidationOutcome;

#[derive(Deserialize, ToSchema)]
pub struct ValidateEmailRequest {
    pub email: String,
}

pub type ValidateEmailResponse = ValidationOutcome;
