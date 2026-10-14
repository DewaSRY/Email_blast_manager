use std::io::Cursor;

use axum::extract::Multipart;
use calamine::{Reader, open_workbook_auto_from_rs};
use csv::ReaderBuilder;

use crate::common::AppError;

pub const MAX_UPLOAD_BYTES: usize = 10* 10000 * 1024 * 1024;
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

/// Header row plus data rows (fully blank rows dropped) of an uploaded sheet.
#[derive(Debug)]
pub struct ParsedSheet {
    pub headers: Vec<String>,
    pub email_index: usize,
    /// `(1-based data row number, cells padded to the header width)`
    pub rows: Vec<(usize, Vec<String>)>,
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

/// Parses the file; the sheet must contain a header named `email`.
pub fn parse_sheet(file: &UploadedFile) -> Result<ParsedSheet, AppError> {
    let mut records = match file.format {
        SheetFormat::Csv => read_csv(&file.bytes)?,
        SheetFormat::Xlsx | SheetFormat::Xls => read_workbook(&file.bytes)?,
    }
    .into_iter();

    let headers: Vec<String> = records
        .next()
        .ok_or_else(|| AppError::bad_request("file", "File has no header row"))?
        .into_iter()
        .map(|header| header.trim().trim_start_matches('\u{feff}').to_string())
        .collect();

    let email_index = headers
        .iter()
        .position(|header| is_email_header(header))
        .ok_or_else(|| AppError::bad_request("file", &format!("File must have an `email` column. Found headers: {}", headers.join(",test "))))?;

    let width = headers.len();
    let rows = records
        .enumerate()
        .filter(|(_, row)| row.iter().any(|cell| !cell.trim().is_empty()))
        .map(|(index, mut row)| {
            row.resize(width, String::new());
            (index + 1, row)
        })
        .collect();

    Ok(ParsedSheet {
        headers,
        email_index,
        rows,
    })
}

fn detect_delimiter(bytes: &[u8]) -> u8 {
    let first_line = bytes
        .split(|&b| b == b'\n')
        .next()
        .unwrap_or_default();

    let commas = first_line.iter().filter(|&&b| b == b',').count();
    let semicolons = first_line.iter().filter(|&&b| b == b';').count();

    if semicolons > commas {
        b';'
    } else {
        b','
    }
}

fn read_csv(bytes: &[u8]) -> Result<Vec<Vec<String>>, AppError> {
    let delimiter = detect_delimiter(bytes);
    
    ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .delimiter(delimiter)
        .from_reader(bytes)
        .records()
        .map(|record| {
            record
                .map(|record| record.iter().map(str::to_string).collect())
                .map_err(|_| AppError::bad_request("file", "Invalid CSV row"))
        })
        .collect()
}

fn read_workbook(bytes: &[u8]) -> Result<Vec<Vec<String>>, AppError> {
    let mut workbook = open_workbook_auto_from_rs(Cursor::new(bytes))
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
    use super::{SheetFormat, UploadedFile, parse_sheet};

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
    fn parses_rows_using_the_email_header() {
        let sheet =
            parse_sheet(&csv("name,Email\nA,a@example.com\n,\nC,c@example.com\nD\n")).unwrap();

        assert_eq!(sheet.email_index, 1);
        assert_eq!(sheet.rows.len(), 3);
        assert_eq!(sheet.rows[1].0, 3);
        assert_eq!(sheet.rows[2].1, vec!["D".to_string(), String::new()]);
    }

    #[test]
    fn rejects_files_without_email_header() {
        assert!(parse_sheet(&csv("address\na@example.com\n")).is_err());
    }

    #[test]
    fn rejects_invalid_excel_content() {
        let file = UploadedFile {
            filename: "a.xlsx".to_string(),
            format: SheetFormat::Xlsx,
            bytes: b"not a workbook".to_vec(),
        };

        assert!(parse_sheet(&file).is_err());
    }
}
