use std::collections::HashMap;

use axum::http::StatusCode;
use csv::Writer;
use serde_json::Value;
use sqlx::PgPool;

use crate::{
    common::AppError,
    import::mapping::{ContactField, field_for_header},
};

const REASON_COLUMN: &str = "invalid_reason";

fn db_error(error: sqlx::Error) -> AppError {
    tracing::error!(%error, "Database error");
    AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
}

/// Builds a CSV of the batch's invalid rows: original columns plus the reason.
pub async fn invalid_rows_csv(pool: &PgPool, import_id: i64) -> Result<Vec<u8>, AppError> {
    let headers: Option<Vec<String>> =
        sqlx::query_scalar("SELECT original_headers FROM import_batches WHERE id = $1")
            .bind(import_id)
            .fetch_optional(pool)
            .await
            .map_err(db_error)?;
    let headers =
        headers.ok_or_else(|| AppError::new(StatusCode::NOT_FOUND, "Import not found"))?;

    let rows: Vec<(Value, Option<String>)> = sqlx::query_as(
        "SELECT raw_data, reason_detail FROM invalid_emails WHERE batch_id = $1 ORDER BY row_number",
    )
    .bind(import_id)
    .fetch_all(pool)
    .await
    .map_err(db_error)?;

    let mut writer = Writer::from_writer(Vec::new());
    let csv_error = |_| AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "Could not build CSV");

    let mut header_row = headers.clone();
    header_row.push(REASON_COLUMN.to_string());
    writer.write_record(&header_row).map_err(csv_error)?;

    for (raw_data, reason) in rows {
        let cells: HashMap<&str, &str> = raw_data
            .as_object()
            .map(|object| {
                object
                    .iter()
                    .filter_map(|(key, value)| Some((key.as_str(), value.as_str()?)))
                    .collect()
            })
            .unwrap_or_default();

        let mut record: Vec<&str> = headers
            .iter()
            .map(|header| cells.get(header.as_str()).copied().unwrap_or(""))
            .collect();
        record.push(reason.as_deref().unwrap_or(""));
        writer.write_record(&record).map_err(csv_error)?;
    }

    writer
        .into_inner()
        .map_err(|_| AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "Could not build CSV"))
}

#[derive(sqlx::FromRow)]
struct ExportContact {
    email: String,
    first_name: Option<String>,
    last_name: Option<String>,
    full_name: Option<String>,
    company: Option<String>,
    job_title: Option<String>,
    industry: Option<String>,
    country: Option<String>,
    city: Option<String>,
    phone: Option<String>,
    attributes: Value,
}

/// Builds a CSV containing only accepted contacts from a completed import.
pub async fn import_result_csv(pool: &PgPool, import_id: i64) -> Result<Vec<u8>, AppError> {
    let batch: Option<(Vec<String>, String)> =
        sqlx::query_as("SELECT original_headers, status FROM import_batches WHERE id = $1")
            .bind(import_id)
            .fetch_optional(pool)
            .await
            .map_err(db_error)?;
    let (headers, status) =
        batch.ok_or_else(|| AppError::new(StatusCode::NOT_FOUND, "Import not found"))?;
    if status != "completed" {
        return Err(AppError::new(
            StatusCode::CONFLICT,
            "Import results are not ready",
        ));
    }

    let email_index = headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case("email"))
        .ok_or_else(|| {
            tracing::error!(import_id, "Completed import has no email header");
            AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "Could not build CSV")
        })?;

    let contacts: Vec<ExportContact> = sqlx::query_as(
        "SELECT c.email, c.first_name, c.last_name, c.full_name,
                c.company, c.job_title, c.industry, c.country, c.city, c.phone, c.attributes
         FROM contact_sources cs
         JOIN contacts c ON c.id = cs.contact_id
         WHERE cs.batch_id = $1
         ORDER BY cs.row_number",
    )
    .bind(import_id)
    .fetch_all(pool)
    .await
    .map_err(db_error)?;

    contacts_csv(&headers, email_index, contacts)
}

fn contacts_csv(
    headers: &[String],
    email_index: usize,
    contacts: Vec<ExportContact>,
) -> Result<Vec<u8>, AppError> {
    let mut writer = Writer::from_writer(Vec::new());
    writer.write_record(headers).map_err(result_csv_error)?;

    for contact in contacts {
        let record = headers
            .iter()
            .enumerate()
            .map(|(index, header)| {
                if index == email_index {
                    return contact.email.clone();
                }

                let value = match field_for_header(header) {
                    Some(ContactField::FirstName) => contact.first_name.as_deref(),
                    Some(ContactField::LastName) => contact.last_name.as_deref(),
                    Some(ContactField::FullName) => contact.full_name.as_deref(),
                    Some(ContactField::Company) => contact.company.as_deref(),
                    Some(ContactField::JobTitle) => contact.job_title.as_deref(),
                    Some(ContactField::Industry) => contact.industry.as_deref(),
                    Some(ContactField::Country) => contact.country.as_deref(),
                    Some(ContactField::City) => contact.city.as_deref(),
                    Some(ContactField::Phone) => contact.phone.as_deref(),
                    None => contact.attributes.get(header).and_then(Value::as_str),
                };
                value.unwrap_or_default().to_string()
            })
            .collect::<Vec<_>>();
        writer.write_record(&record).map_err(result_csv_error)?;
    }

    writer
        .into_inner()
        .map_err(|error| result_csv_error(error.into_error().into()))
}

fn result_csv_error(error: csv::Error) -> AppError {
    tracing::error!(%error, "Could not build import result CSV");
    AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "Could not build CSV")
}

#[cfg(test)]
mod tests {
    use super::{ExportContact, contacts_csv};
    use serde_json::json;

    fn contact(email: &str) -> ExportContact {
        ExportContact {
            email: email.into(),
            first_name: None,
            last_name: None,
            full_name: None,
            company: None,
            job_title: None,
            industry: None,
            country: None,
            city: None,
            phone: None,
            attributes: json!({}),
        }
    }

    #[test]
    fn exports_original_headers_mapped_fields_and_custom_attributes() {
        let headers = ["First Name", "EMAIL", "company_name", "Note", "phone"].map(String::from);
        let mut row = contact("person@example.com");
        row.first_name = Some("Ann".into());
        row.company = Some("Acme, \"Inc\"".into());
        row.attributes = json!({"Note": "first line\nsecond line"});

        let body = contacts_csv(&headers, 1, vec![row]).unwrap();
        let mut reader = csv::Reader::from_reader(body.as_slice());
        assert_eq!(
            reader.headers().unwrap(),
            &csv::StringRecord::from(headers.to_vec())
        );
        let records = reader.records().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0],
            csv::StringRecord::from(vec![
                "Ann",
                "person@example.com",
                "Acme, \"Inc\"",
                "first line\nsecond line",
                ""
            ])
        );
    }

    #[test]
    fn empty_results_still_include_headers() {
        let headers = ["email", "First Name"].map(String::from);
        let body = contacts_csv(&headers, 0, vec![]).unwrap();
        assert_eq!(String::from_utf8(body).unwrap(), "email,First Name\n");
    }

    #[test]
    fn preserves_order_and_accepted_duplicate_rows() {
        let headers = ["email"].map(String::from);
        let body = contacts_csv(
            &headers,
            0,
            vec![
                contact("b@example.com"),
                contact("a@example.com"),
                contact("b@example.com"),
            ],
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(body).unwrap(),
            "email\nb@example.com\na@example.com\nb@example.com\n"
        );
    }
}
