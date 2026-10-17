use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Serialize, ToSchema)]
pub struct ImportStatusResponse {
    pub import_id: i64,
    pub filename: String,
    /// `queued`, `validating`, `completed` or `failed`
    pub status: String,
    /// True once the batch is `completed` or `failed`
    pub finished: bool,
    pub total_rows: usize,
    pub processed_rows: usize,
    pub valid_rows: usize,
    pub invalid_rows: usize,
    pub duplicate_rows: usize,
    pub progress_percent: f64,
    pub error_message: Option<String>,
}



/// Documentation-only schema so Swagger UI renders a file picker.
/// The handler still reads the upload through `Multipart`.
#[derive(ToSchema)]
#[allow(dead_code)]
pub struct ImportEmailForm {
    /// CSV, XLSX or XLS file with an `email` column
    #[schema(value_type = String, format = Binary)]
    file: Vec<u8>,
}