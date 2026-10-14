use std::io::Cursor;

use axum::extract::Multipart;
use calamine::{Reader, open_workbook_auto_from_rs};
use csv::ReaderBuilder;

use crate::common::AppError;

pub const MAX_UPLOAD_BYTES: usize = 10 * 1024 * 1024;
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

#[derive(Debug)]
pub struct UploadedFile {
    pub filename: String,
    pub format: SheetFormat,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SheetStats {
    pub total_rows: usize,
    pub total_emails: usize,
}

/// Reads the named multipart file field, enforcing the size limit while
/// streaming, plus the non-empty and supported-extension checks.
pub async fn read_upload(
    multipart: &mut Multipart,
    field_name: &str,
    max_bytes: usize,
) -> Result<UploadedFile, AppError> {
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

        let mut bytes = Vec::new();
        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|_| AppError::bad_request(field_name, "Could not read uploaded file"))?
        {
            if bytes.len() + chunk.len() > max_bytes {
                return Err(AppError::bad_request(
                    field_name,
                    format!("Maximum upload size is {} MB", max_bytes / (1024 * 1024)),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }

        if bytes.is_empty() {
            return Err(AppError::bad_request(field_name, "Uploaded file is empty"));
        }

        return Ok(UploadedFile {
            filename,
            format,
            bytes,
        });
    }

    Err(AppError::bad_request(field_name, "Missing file field"))
}

/// Counts data rows and rows with a non-empty email cell. The email column is
/// the header named `email`, falling back to the first column.
pub fn count_emails(file: &UploadedFile) -> Result<SheetStats, AppError> {
    match file.format {
        SheetFormat::Csv => count_csv(&file.bytes),
        SheetFormat::Xlsx | SheetFormat::Xls => count_workbook(&file.bytes),
    }
}

fn count_csv(bytes: &[u8]) -> Result<SheetStats, AppError> {
    let mut reader = ReaderBuilder::new().flexible(true).from_reader(bytes);

    let email_index = reader
        .headers()
        .map_err(|_| AppError::bad_request("file", "Invalid CSV headers"))?
        .iter()
        .position(is_email_header)
        .unwrap_or(0);

    let mut stats = SheetStats {
        total_rows: 0,
        total_emails: 0,
    };
    for record in reader.records() {
        let record = record.map_err(|_| AppError::bad_request("file", "Invalid CSV row"))?;
        stats.total_rows += 1;
        if record
            .get(email_index)
            .is_some_and(|cell| !cell.trim().is_empty())
        {
            stats.total_emails += 1;
        }
    }

    Ok(stats)
}

fn count_workbook(bytes: &[u8]) -> Result<SheetStats, AppError> {
    let mut workbook = open_workbook_auto_from_rs(Cursor::new(bytes))
        .map_err(|_| AppError::bad_request("file", "Invalid Excel file"))?;
    let range = workbook
        .worksheet_range_at(0)
        .ok_or_else(|| AppError::bad_request("file", "Workbook has no worksheets"))?
        .map_err(|_| AppError::bad_request("file", "Could not read worksheet"))?;

    let mut rows = range.rows();
    let email_index = rows
        .next()
        .and_then(|header| {
            header
                .iter()
                .position(|cell| is_email_header(&cell.to_string()))
        })
        .unwrap_or(0);

    let mut stats = SheetStats {
        total_rows: 0,
        total_emails: 0,
    };
    for row in rows {
        stats.total_rows += 1;
        if row
            .get(email_index)
            .is_some_and(|cell| !cell.to_string().trim().is_empty())
        {
            stats.total_emails += 1;
        }
    }

    Ok(stats)
}

fn is_email_header(header: &str) -> bool {
    header.trim().eq_ignore_ascii_case(EMAIL_HEADER)
}

#[cfg(test)]
mod tests {
    use super::{SheetFormat, SheetStats, UploadedFile, count_emails};

    fn csv(content: &str) -> UploadedFile {
        UploadedFile {
            filename: "list.csv".to_string(),
            format: SheetFormat::Csv,
            bytes: content.as_bytes().to_vec(),
        }
    }

    #[test]
    fn detects_format_from_extension_case_insensitively() {
        assert_eq!(
            SheetFormat::from_filename("a.CSV").unwrap(),
            SheetFormat::Csv
        );
        assert_eq!(
            SheetFormat::from_filename("a.xlsx").unwrap(),
            SheetFormat::Xlsx
        );
        assert_eq!(
            SheetFormat::from_filename("a.xls").unwrap(),
            SheetFormat::Xls
        );
        assert!(SheetFormat::from_filename("csv").is_err());
        assert!(SheetFormat::from_filename("a.txt").is_err());
    }

    #[test]
    fn counts_rows_using_the_email_header() {
        let stats =
            count_emails(&csv("name,Email\nA,a@example.com\nB,\nC,c@example.com\n")).unwrap();

        assert_eq!(
            stats,
            SheetStats {
                total_rows: 3,
                total_emails: 2
            }
        );
    }

    #[test]
    fn falls_back_to_first_column_without_email_header() {
        let stats = count_emails(&csv("address\na@example.com\n\n")).unwrap();

        assert_eq!(stats.total_emails, 1);
    }

    #[test]
    fn rejects_invalid_excel_content() {
        let file = UploadedFile {
            filename: "a.xlsx".to_string(),
            format: SheetFormat::Xlsx,
            bytes: b"not a workbook".to_vec(),
        };

        assert!(count_emails(&file).is_err());
    }
}
