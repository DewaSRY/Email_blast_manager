use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum EmailStatus {
    #[allow(dead_code)]
    Valid,
    Invalid,
    #[allow(dead_code)]
    Risky,
    Unknown,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ValidationOutcome {
    pub email: String,
    pub status: EmailStatus,
    pub reason_code: Option<String>,
    pub reason_detail: Option<String>,
    pub stage_reached: String,
    pub mx_hosts: Vec<String>,
    pub is_disposable: bool,
    pub is_role: bool,
    pub is_free_provider: bool,
}

impl ValidationOutcome {
    pub fn new(email: String, status: EmailStatus, stage_reached: &str) -> Self {
        Self {
            email,
            status,
            reason_code: None,
            reason_detail: None,
            stage_reached: stage_reached.to_string(),
            mx_hosts: Vec::new(),
            is_disposable: false,
            is_role: false,
            is_free_provider: false,
        }
    }

    pub fn with_reason(mut self, code: &str, detail: &str) -> Self {
        self.reason_code = Some(code.to_string());
        self.reason_detail = Some(detail.to_string());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{EmailStatus, ValidationOutcome};

    #[test]
    fn supports_all_documented_verdicts() {
        let valid = ValidationOutcome::new(
            "person@example.com".to_string(),
            EmailStatus::Valid,
            "smtp_probe",
        );
        assert_eq!(valid.status, EmailStatus::Valid);
    }
}
