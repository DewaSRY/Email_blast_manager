use std::sync::Arc;

use async_trait::async_trait;
use hickory_resolver::{TokioAsyncResolver, error::ResolveErrorKind};

use crate::validation::{
    model::{EmailStatus, ValidationOutcome},
    stages::{ClassifyStage, Stage, StageOutcome, SyntaxStage, ValidationContext},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MxHost {
    pub preference: u16,
    pub exchange: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainLookup {
    Mx(Vec<MxHost>),
    AddressFallback,
    NoMx,
    NullMx,
    NotFound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DnsLookupError {
    Temporary,
}

#[async_trait]
pub trait DnsResolver: Send + Sync {
    async fn lookup_domain(&self, domain: &str) -> Result<DomainLookup, DnsLookupError>;
}

pub struct SystemDnsResolver {
    resolver: TokioAsyncResolver,
}

impl SystemDnsResolver {
    pub fn new() -> Result<Self, hickory_resolver::error::ResolveError> {
        Ok(Self {
            resolver: TokioAsyncResolver::tokio_from_system_conf()?,
        })
    }
}

#[async_trait]
impl DnsResolver for SystemDnsResolver {
    async fn lookup_domain(&self, domain: &str) -> Result<DomainLookup, DnsLookupError> {
        let fully_qualified_domain = format!("{domain}.");
        match self.resolver.mx_lookup(&fully_qualified_domain).await {
            Ok(records) => {
                let mut mx_hosts = records
                    .iter()
                    .map(|record| MxHost {
                        preference: record.preference(),
                        exchange: record
                            .exchange()
                            .to_utf8()
                            .trim_end_matches('.')
                            .to_string(),
                    })
                    .collect::<Vec<_>>();

                if mx_hosts.iter().any(|host| host.exchange.is_empty()) {
                    return Ok(DomainLookup::NullMx);
                }
                if !mx_hosts.is_empty() {
                    mx_hosts.sort_by_key(|host| host.preference);
                    return Ok(DomainLookup::Mx(mx_hosts));
                }
            }
            Err(error) if is_nx_domain(error.kind()) => return Ok(DomainLookup::NotFound),
            Err(error) if is_no_records(error.kind()) => {}
            Err(_) => return Err(DnsLookupError::Temporary),
        }

        match self.resolver.lookup_ip(&fully_qualified_domain).await {
            Ok(records) if records.iter().next().is_some() => Ok(DomainLookup::AddressFallback),
            Ok(_) => Ok(DomainLookup::NoMx),
            Err(error) if is_nx_domain(error.kind()) => Ok(DomainLookup::NotFound),
            Err(error) if is_no_records(error.kind()) => Ok(DomainLookup::NoMx),
            Err(_) => Err(DnsLookupError::Temporary),
        }
    }
}

fn is_nx_domain(error: &ResolveErrorKind) -> bool {
    matches!(
        error,
        ResolveErrorKind::NoRecordsFound {
            response_code: hickory_resolver::proto::op::ResponseCode::NXDomain,
            ..
        }
    )
}

fn is_no_records(error: &ResolveErrorKind) -> bool {
    matches!(
        error,
        ResolveErrorKind::NoRecordsFound {
            response_code: hickory_resolver::proto::op::ResponseCode::NoError,
            ..
        }
    )
}

struct DomainStage {
    dns: Arc<dyn DnsResolver>,
}

#[async_trait]
impl Stage for DomainStage {
    async fn run(&self, context: &mut ValidationContext) -> StageOutcome {
        let Some(email) = context.normalized.as_ref() else {
            return StageOutcome::Finish(
                ValidationOutcome::new(
                    context.input.trim().to_string(),
                    EmailStatus::Unknown,
                    "domain",
                )
                .with_reason("PIPELINE_STATE_INVALID", "The syntax stage did not run"),
            );
        };

        let mut outcome =
            ValidationOutcome::new(email.value.clone(), EmailStatus::Unknown, "domain");

        match self.dns.lookup_domain(&email.domain).await {
            Ok(DomainLookup::NotFound) => {
                outcome.status = EmailStatus::Invalid;
                return StageOutcome::Finish(
                    outcome.with_reason("DOMAIN_NOT_FOUND", "The email domain does not exist"),
                );
            }
            Ok(DomainLookup::NoMx | DomainLookup::NullMx) => {
                outcome.status = EmailStatus::Invalid;
                return StageOutcome::Finish(
                    outcome.with_reason("NO_MX_RECORD", "The domain does not accept email"),
                );
            }
            Ok(DomainLookup::Mx(records)) => {
                outcome.mx_hosts = records.into_iter().map(|mx| mx.exchange).collect();
            }
            Ok(DomainLookup::AddressFallback) => {}
            Err(DnsLookupError::Temporary) => {
                return StageOutcome::Finish(outcome.with_reason(
                    "DNS_LOOKUP_FAILED",
                    "The domain DNS lookup could not be completed",
                ));
            }
        }

        context.outcome = Some(outcome);
        StageOutcome::Continue
    }
}

#[derive(Clone)]
pub struct ValidationService {
    stages: Vec<Arc<dyn Stage>>,
}

impl ValidationService {
    pub fn new(dns: Arc<dyn DnsResolver>) -> Self {
        Self {
            stages: vec![
                Arc::new(SyntaxStage),
                Arc::new(DomainStage { dns }),
                Arc::new(ClassifyStage),
            ],
        }
    }

    pub fn system() -> Result<Self, hickory_resolver::error::ResolveError> {
        Ok(Self::new(Arc::new(SystemDnsResolver::new()?)))
    }

    pub async fn validate(&self, input: &str) -> ValidationOutcome {
        let mut context = ValidationContext::new(input);
        for stage in &self.stages {
            match stage.run(&mut context).await {
                StageOutcome::Continue => {}
                StageOutcome::Finish(outcome) => return outcome,
            }
        }

        ValidationOutcome::new(input.trim().to_string(), EmailStatus::Unknown, "pipeline")
            .with_reason(
                "PIPELINE_INCOMPLETE",
                "Validation stages did not return an outcome",
            )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::{DnsLookupError, DnsResolver, DomainLookup, MxHost, ValidationService};
    use crate::validation::model::EmailStatus;

    struct FakeDns(DomainLookup);

    #[async_trait]
    impl DnsResolver for FakeDns {
        async fn lookup_domain(&self, _domain: &str) -> Result<DomainLookup, DnsLookupError> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn rejects_addresses_when_the_domain_does_not_exist() {
        let service = ValidationService::new(Arc::new(FakeDns(DomainLookup::NotFound)));
        let result = service.validate("person@example.test").await;

        assert_eq!(result.status, EmailStatus::Invalid);
        assert_eq!(result.reason_code.as_deref(), Some("DOMAIN_NOT_FOUND"));
    }

    #[tokio::test]
    async fn flags_disposable_addresses_as_risky() {
        let service = ValidationService::new(Arc::new(FakeDns(DomainLookup::Mx(vec![MxHost {
            preference: 10,
            exchange: "mail.mailinator.com".to_string(),
        }]))));
        let result = service.validate("person@mailinator.com").await;

        assert_eq!(result.status, EmailStatus::Risky);
        assert!(result.is_disposable);
        assert_eq!(result.reason_code.as_deref(), Some("DISPOSABLE"));
    }

    #[tokio::test]
    async fn never_reports_mailbox_as_valid_without_smtp_probe() {
        let service = ValidationService::new(Arc::new(FakeDns(DomainLookup::Mx(vec![MxHost {
            preference: 10,
            exchange: "mx.example.com".to_string(),
        }]))));
        let result = service.validate("person@example.com").await;

        assert_eq!(result.status, EmailStatus::Unknown);
        assert_eq!(
            result.reason_code.as_deref(),
            Some("SMTP_PROBE_NOT_CONFIGURED")
        );
    }
}
