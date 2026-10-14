use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContactField {
    FirstName,
    LastName,
    FullName,
    Company,
    JobTitle,
    Industry,
    Country,
    City,
    Phone,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ContactColumns {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub full_name: Option<String>,
    pub company: Option<String>,
    pub job_title: Option<String>,
    pub industry: Option<String>,
    pub country: Option<String>,
    pub city: Option<String>,
    pub phone: Option<String>,
}

/// Maps a file header to a `contacts` column, ignoring case and separators.
pub fn field_for_header(header: &str) -> Option<ContactField> {
    let key: String = header
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect();

    Some(match key.as_str() {
        "firstname" | "givenname" | "fname" => ContactField::FirstName,
        "lastname" | "surname" | "familyname" | "lname" => ContactField::LastName,
        "fullname" | "name" | "contactname" => ContactField::FullName,
        "company" | "companyname" | "organization" | "organisation" => ContactField::Company,
        "jobtitle" | "title" | "position" | "role" => ContactField::JobTitle,
        "industry" => ContactField::Industry,
        "country" => ContactField::Country,
        "city" => ContactField::City,
        "phone" | "phonenumber" | "mobile" | "tel" | "telephone" => ContactField::Phone,
        _ => return None,
    })
}

/// Splits a row into known contact columns and everything else (JSON attributes).
pub fn map_row(headers: &[String], email_index: usize, row: &[String]) -> (ContactColumns, Value) {
    let mut columns = ContactColumns::default();
    let mut attributes = Map::new();

    for (index, (header, cell)) in headers.iter().zip(row).enumerate() {
        let cell = cell.trim();
        if index == email_index || cell.is_empty() {
            continue;
        }

        let slot = match field_for_header(header) {
            Some(ContactField::FirstName) => &mut columns.first_name,
            Some(ContactField::LastName) => &mut columns.last_name,
            Some(ContactField::FullName) => &mut columns.full_name,
            Some(ContactField::Company) => &mut columns.company,
            Some(ContactField::JobTitle) => &mut columns.job_title,
            Some(ContactField::Industry) => &mut columns.industry,
            Some(ContactField::Country) => &mut columns.country,
            Some(ContactField::City) => &mut columns.city,
            Some(ContactField::Phone) => &mut columns.phone,
            None => {
                if !header.is_empty() {
                    attributes.insert(header.clone(), Value::String(cell.to_string()));
                }
                continue;
            }
        };
        // First column wins when two headers map to the same field.
        if slot.is_none() {
            *slot = Some(cell.to_string());
        }
    }

    (columns, Value::Object(attributes))
}

#[cfg(test)]
mod tests {
    use super::map_row;

    #[test]
    fn maps_known_headers_and_keeps_the_rest_as_attributes() {
        let headers = ["Email", "First Name", "company_name", "Favourite Color"]
            .map(String::from)
            .to_vec();
        let row = ["a@example.com", "Ann", "Acme", "blue"]
            .map(String::from)
            .to_vec();

        let (columns, attributes) = map_row(&headers, 0, &row);

        assert_eq!(columns.first_name.as_deref(), Some("Ann"));
        assert_eq!(columns.company.as_deref(), Some("Acme"));
        assert_eq!(attributes["Favourite Color"], "blue");
        assert!(attributes.get("Email").is_none());
    }
}
