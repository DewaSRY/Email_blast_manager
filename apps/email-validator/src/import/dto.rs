use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Serialize, ToSchema)]
pub struct ImportEmailResponse {
    pub filename: String,
    pub total_rows: usize,
    pub total_emails: usize,
    pub invalid_rows: usize,
}
