use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::extract::Multipart;
use calamine::{Reader, open_workbook_auto_from_rs};
use csv::ReaderBuilder;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::common::AppError;

pub const MAX_UPLOAD_BYTES: usize = 2 * 1024 * 1024 * 1024;
const EMAIL_HEADER: &str = "email";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SheetFormat {
    Csv,
    Xlsx,
    Xls,
}

impl SheetFormat {
    pub fn from_filename(filename: &str) -> Result<Self, AppError> {
        let extension = filename
            .rsplit_once('.')
            .map(|(_, extension)| extension.to_ascii_lowercase());

        match extension.as_deref() {
            Some("csv") => Ok(Self::Csv),
            Some("xlsx") => Ok(Self::Xlsx),
            Some("xls") => Ok(Self::Xls),
            _ => Err(AppError::bad_request(
                "file",
                "Only CSV, XLSX, and XLS files are supported",
            )),
        }
    }
}

/// An upload streamed to disk, so large files never sit in memory.
#[derive(Debug)]
pub struct StoredUpload {
    pub filename: String,
    pub format: SheetFormat,
    pub path: PathBuf,
    pub sha256: String,
}

/// Header row plus a lazy iterator over data rows (fully blank rows dropped).
pub struct RowSource {
    pub headers: Vec<String>,
    pub email_index: usize,
    /// `(1-based data row number, cells padded to the header width)`
    pub rows: Box<dyn Iterator<Item = Result<(usize, Vec<String>), AppError>> + Send>,
}

fn io_error(error: std::io::Error) -> AppError {
    tracing::error!(%error, "Upload storage error");
    AppError::new(
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "Internal server error",
    )
}

/// Streams the named multipart file field to `dir`, hashing as it goes and
/// enforcing the size limit, plus the non-empty and supported-extension checks.
pub async fn read_upload(
    multipart: &mut Multipart,
    field_name: &str,
    max_bytes: usize,
    dir: &Path,
) -> Result<StoredUpload, AppError> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|_| AppError::bad_request(field_name, "Invalid multipart form"))?
    {
        if field.name() != Some(field_name) {
            continue;
        }

        let filename = field.file_name().unwrap_or("upload").to_string();
        let format = SheetFormat::from_filename(&filename)?;

        tokio::fs::create_dir_all(dir).await.map_err(io_error)?;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let path = dir.join(format!(
            "upload-{nanos}-{}.part",
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));

        let result = stream_field(&mut field, &path, field_name, max_bytes).await;
        let sha256 = match result {
            Ok(hash) => hash,
            Err(error) => {
                let _ = tokio::fs::remove_file(&path).await;
                return Err(error);
            }
        };

        return Ok(StoredUpload {
            filename,
            format,
            path,
            sha256,
        });
    }

    Err(AppError::bad_request(field_name, "Missing file field"))
}

async fn stream_field(
    field: &mut axum::extract::multipart::Field<'_>,
    path: &Path,
    field_name: &str,
    max_bytes: usize,
) -> Result<String, AppError> {
    let mut file = tokio::fs::File::create(path).await.map_err(io_error)?;
    let mut hasher = Sha256::new();
    let mut size = 0usize;

    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|_| AppError::bad_request(field_name, "Could not read uploaded file"))?
    {
        size += chunk.len();
        if size > max_bytes {
            return Err(AppError::bad_request(
                field_name,
                format!("Maximum upload size is {} MB", max_bytes / (1024 * 1024)),
            ));
        }
        hasher.update(&chunk);
        file.write_all(&chunk).await.map_err(io_error)?;
    }
    file.flush().await.map_err(io_error)?;

    if size == 0 {
        return Err(AppError::bad_request(field_name, "Uploaded file is empty"));
    }

    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// Opens the stored file; the sheet must contain a header named `email`.
/// CSV rows are read lazily; Excel workbooks must be loaded whole by the parser.
pub fn open_rows(path: &Path, format: SheetFormat) -> Result<RowSource, AppError> {
    let mut records: Box<dyn Iterator<Item = Result<Vec<String>, AppError>> + Send> = match format
    {
        SheetFormat::Csv => Box::new(read_csv(path)?),
        SheetFormat::Xlsx | SheetFormat::Xls => Box::new(read_workbook(path)?.into_iter().map(Ok)),
    };

    let headers: Vec<String> = records
        .next()
        .ok_or_else(|| AppError::bad_request("file", "File has no header row"))??
        .into_iter()
        .map(|header| header.trim().trim_start_matches('\u{feff}').to_string())
        .collect();

    let email_index = headers
        .iter()
        .position(|header| is_email_header(header))
        .ok_or_else(|| {
            AppError::bad_request(
                "file",
                format!(
                    "File must have an `email` column. Found headers: {}",
                    headers.join(", ")
                ),
            )
        })?;

    let width = headers.len();
    let rows = records
        .enumerate()
        .filter(|(_, row)| match row {
            Ok(row) => row.iter().any(|cell| !cell.trim().is_empty()),
            Err(_) => true,
        })
        .map(move |(index, row)| {
            row.map(|mut row| {
                row.resize(width, String::new());
                (index + 1, row)
            })
        });

    Ok(RowSource {
        headers,
        email_index,
        rows: Box::new(rows),
    })
}

fn detect_delimiter(path: &Path) -> Result<u8, AppError> {
    let mut first_line = Vec::new();
    BufReader::new(File::open(path).map_err(io_error)?)
        .take(64 * 1024)
        .read_until(b'\n', &mut first_line)
        .map_err(io_error)?;

    let commas = first_line.iter().filter(|&&b| b == b',').count();
    let semicolons = first_line.iter().filter(|&&b| b == b';').count();

    Ok(if semicolons > commas { b';' } else { b',' })
}

fn read_csv(
    path: &Path,
) -> Result<impl Iterator<Item = Result<Vec<String>, AppError>> + Send + use<>, AppError> {
    let delimiter = detect_delimiter(path)?;
    let file = File::open(path).map_err(io_error)?;

    Ok(ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .delimiter(delimiter)
        .from_reader(BufReader::new(file))
        .into_records()
        .map(|record| {
            record
                .map(|record| record.iter().map(str::to_string).collect())
                .map_err(|_| AppError::bad_request("file", "Invalid CSV row"))
        }))
}

fn read_workbook(path: &Path) -> Result<Vec<Vec<String>>, AppError> {
    let bytes = std::fs::read(path).map_err(io_error)?;
    let mut workbook = open_workbook_auto_from_rs(std::io::Cursor::new(bytes))
        .map_err(|_| AppError::bad_request("file", "Invalid Excel file"))?;
    let range = workbook
        .worksheet_range_at(0)
        .ok_or_else(|| AppError::bad_request("file", "Workbook has no worksheets"))?
        .map_err(|_| AppError::bad_request("file", "Could not read worksheet"))?;

    Ok(range
        .rows()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect())
        .collect())
}

fn is_email_header(header: &str) -> bool {
    header
        .trim()
        .trim_start_matches('\u{feff}')
        .eq_ignore_ascii_case(EMAIL_HEADER)
}

#[cfg(test)]
mod tests {
    use super::{SheetFormat, open_rows};
    use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};

    fn temp_file(name: &str, content: &[u8]) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "ev-test-{}-{}-{name}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn detects_format_from_extension_case_insensitively() {
        assert_eq!(SheetFormat::from_filename("a.CSV").unwrap(), SheetFormat::Csv);
        assert_eq!(SheetFormat::from_filename("a.xlsx").unwrap(), SheetFormat::Xlsx);
        assert_eq!(SheetFormat::from_filename("a.xls").unwrap(), SheetFormat::Xls);
        assert!(SheetFormat::from_filename("csv").is_err());
        assert!(SheetFormat::from_filename("a.txt").is_err());
    }

    #[test]
    fn parses_rows_using_the_email_header() {
        let path = temp_file("a.csv", b"name,Email\nA,a@example.com\n,\nC,c@example.com\nD\n");
        let source = open_rows(&path, SheetFormat::Csv).unwrap();
        let rows: Vec<_> = source.rows.map(Result::unwrap).collect();
        std::fs::remove_file(path).ok();

        assert_eq!(source.email_index, 1);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1].0, 3);
        assert_eq!(rows[2].1, vec!["D".to_string(), String::new()]);
    }

    #[test]
    fn rejects_files_without_email_header() {
        let path = temp_file("b.csv", b"address\na@example.com\n");
        assert!(open_rows(&path, SheetFormat::Csv).is_err());
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn rejects_invalid_excel_content() {
        let path = temp_file("c.xlsx", b"not a workbook");
        assert!(open_rows(&path, SheetFormat::Xlsx).is_err());
        std::fs::remove_file(path).ok();
    }
}
