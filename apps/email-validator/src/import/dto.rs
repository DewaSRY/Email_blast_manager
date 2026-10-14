use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Serialize, ToSchema)]
pub struct ImportEmailResponse {
    pub import_id: i64,
    pub filename: String,
    pub total_rows: usize,
    pub valid_rows: usize,
    pub invalid_rows: usize,
    pub duplicate_rows: usize,
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