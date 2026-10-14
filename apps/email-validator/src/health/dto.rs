use serde::Serialize;
use utoipa::ToSchema;

use crate::health::model::HealthStatus;

#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
}

impl From<HealthStatus> for HealthResponse {
    fn from(model: HealthStatus) -> Self {
        Self {
            status: if model.healthy {
                "ok".to_string()
            } else {
                "unhealthy".to_string()
            },
        }
    }
}