use async_trait::async_trait;
use email_address::EmailAddress;
use idna::domain_to_ascii;
use unicode_normalization::UnicodeNormalization;

use crate::validation::model::{EmailStatus, ValidationOutcome};

pub struct ValidationContext {
    pub input: String,
    pub normalized: Option<NormalizedEmail>,
    pub outcome: Option<ValidationOutcome>,
}

impl ValidationContext {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.to_string(),
            normalized: None,
            outcome: None,
        }
    }
}

pub enum StageOutcome {
    Continue,
    Finish(ValidationOutcome),
}

#[async_trait]
pub trait Stage: Send + Sync {
    async fn run(&self, context: &mut ValidationContext) -> StageOutcome;
}

pub struct SyntaxStage;

#[async_trait]
impl Stage for SyntaxStage {
    async fn run(&self, context: &mut ValidationContext) -> StageOutcome {
        match normalize_and_validate_email(&context.input) {
            Ok(email) => {
                context.normalized = Some(email);
                StageOutcome::Continue
            }
            Err(reason) => StageOutcome::Finish(
                ValidationOutcome::new(
                    context.input.trim().to_string(),
                    EmailStatus::Invalid,
                    "syntax",
                )
                .with_reason("INVALID_SYNTAX", reason),
            ),
        }
    }
}

pub struct NormalizedEmail {
    pub value: String,
    pub local_part: String,
    pub domain: String,
}

pub fn normalize_and_validate_email(input: &str) -> Result<NormalizedEmail, &'static str> {
    let email = input.trim();
    let Some((local_part, domain)) = email.rsplit_once('@') else {
        return Err("Email must contain a local part and domain");
    };

    let normalized_local_part = local_part.nfc().collect::<String>();
    if normalized_local_part.is_empty() || domain.is_empty() || normalized_local_part.len() > 64 {
        return Err("Email local part or domain has an invalid length");
    }

    let domain = domain_to_ascii(domain).map_err(|_| "Email domain is not a valid IDN")?;
    let normalized_domain = domain.to_ascii_lowercase();
    let normalized_email = format!("{normalized_local_part}@{normalized_domain}");

    if normalized_email.len() > 254 || !EmailAddress::is_valid(&normalized_email) {
        return Err("Email address syntax is invalid");
    }

    Ok(NormalizedEmail {
        value: normalized_email,
        local_part: normalized_local_part,
        domain: normalized_domain,
    })
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Classification {
    pub is_disposable: bool,
    pub is_role: bool,
    pub is_free_provider: bool,
}

pub fn classify_email(local_part: &str, domain: &str) -> Classification {
    const DISPOSABLE_DOMAINS: &[&str] = &[
        "10minutemail.com",
        "guerrillamail.com",
        "mailinator.com",
        "temp-mail.org",
        "tempmail.com",
    ];
    const FREE_PROVIDERS: &[&str] = &[
        "gmail.com",
        "googlemail.com",
        "hotmail.com",
        "icloud.com",
        "live.com",
        "outlook.com",
        "yahoo.com",
    ];
    const ROLE_ADDRESSES: &[&str] = &[
        "abuse",
        "admin",
        "billing",
        "contact",
        "info",
        "noreply",
        "postmaster",
        "sales",
        "security",
        "support",
        "webmaster",
    ];

    Classification {
        is_disposable: DISPOSABLE_DOMAINS.contains(&domain),
        is_role: ROLE_ADDRESSES.contains(&local_part.to_ascii_lowercase().as_str()),
        is_free_provider: FREE_PROVIDERS.contains(&domain),
    }
}

pub struct ClassifyStage;

#[async_trait]
impl Stage for ClassifyStage {
    async fn run(&self, context: &mut ValidationContext) -> StageOutcome {
        let (Some(email), Some(mut outcome)) =
            (context.normalized.as_ref(), context.outcome.take())
        else {
            return StageOutcome::Finish(
                ValidationOutcome::new(
                    context.input.trim().to_string(),
                    EmailStatus::Unknown,
                    "classify",
                )
                .with_reason(
                    "PIPELINE_STATE_INVALID",
                    "A required validation stage was skipped",
                ),
            );
        };

        let classification = classify_email(&email.local_part, &email.domain);
        outcome.is_disposable = classification.is_disposable;
        outcome.is_role = classification.is_role;
        outcome.is_free_provider = classification.is_free_provider;
        outcome.stage_reached = "classify".to_string();

        if classification.is_disposable {
            outcome.status = EmailStatus::Risky;
            return StageOutcome::Finish(outcome.with_reason(
                "DISPOSABLE",
                "The address uses a known disposable email provider",
            ));
        }
        if classification.is_role {
            outcome.status = EmailStatus::Risky;
            return StageOutcome::Finish(outcome.with_reason(
                "ROLE_ACCOUNT",
                "The address appears to be a role-based account",
            ));
        }

        outcome.stage_reached = "smtp_probe".to_string();
        StageOutcome::Finish(outcome.with_reason(
            "SMTP_PROBE_NOT_CONFIGURED",
            "Domain checks passed, but mailbox existence has not been verified",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{classify_email, normalize_and_validate_email};

    #[test]
    fn normalizes_idn_domain_without_changing_local_part() {
        let email = normalize_and_validate_email("  Person@bücher.de ").unwrap();

        assert_eq!(email.value, "Person@xn--bcher-kva.de");
        assert_eq!(email.local_part, "Person");
        assert_eq!(email.domain, "xn--bcher-kva.de");
    }

    #[test]
    fn normalizes_local_part_to_unicode_nfc() {
        let email = normalize_and_validate_email("e\u{301}@example.com").unwrap();

        assert_eq!(email.value, "é@example.com");
        assert_eq!(email.local_part, "é");
    }

    #[test]
    fn rejects_invalid_syntax_and_lengths() {
        assert!(normalize_and_validate_email("not-an-email").is_err());
        assert!(normalize_and_validate_email("a..b@example.com").is_err());
        assert!(normalize_and_validate_email(&format!("{}@example.com", "a".repeat(65))).is_err());
    }

    #[test]
    fn classifies_disposable_role_and_free_addresses() {
        let disposable = classify_email("person", "mailinator.com");
        assert!(disposable.is_disposable);

        let role = classify_email("Support", "example.org");
        assert!(role.is_role);

        let free = classify_email("person", "gmail.com");
        assert!(free.is_free_provider);
        assert!(!free.is_disposable);
    }
}
