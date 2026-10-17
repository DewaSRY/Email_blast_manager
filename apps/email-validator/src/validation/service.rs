mod dns;
mod smtp;

pub use dns::{DnsLookupError, DnsResolver, DomainLookup, MxHost, SystemDnsResolver};
pub use smtp::{SmtpProbeResult, SmtpProber, TcpSmtpProber};

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use hickory_resolver::config::ResolverConfig;
use tracing::{debug, info, warn};

use crate::validation::{
    model::{EmailStatus, ValidationOutcome},
    stages::{ClassifyStage, Stage, StageOutcome, SyntaxStage, ValidationContext},
};

use self::smtp::smtp_recipient_probe_at;

struct DomainStage {
    dns: Arc<dyn DnsResolver>,
}

#[async_trait]
impl Stage for DomainStage {
    fn name(&self) -> &'static str {
        "domain"
    }

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

        let lookup_started = Instant::now();
        let lookup = self.dns.lookup_domain(&email.domain).await;
        debug!(
            domain = %email.domain,
            lookup_ms = lookup_started.elapsed().as_millis() as u64,
            result = match &lookup {
                Ok(DomainLookup::Mx(records)) => format!("mx({})", records.len()),
                Ok(DomainLookup::AddressFallback) => "address_fallback".to_string(),
                Ok(DomainLookup::NullMx) => "null_mx".to_string(),
                Ok(DomainLookup::NoMx) => "no_mx".to_string(),
                Ok(DomainLookup::NotFound) => "nxdomain".to_string(),
                Err(_) => "temporary_failure".to_string(),
            },
            "DNS lookup finished"
        );

        match lookup {
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
            Ok(DomainLookup::AddressFallback) => {
                outcome.mx_hosts.push(email.domain.clone());
            }
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

struct SmtpProbeStage {
    prober: Arc<dyn SmtpProber>,
}

#[async_trait]
impl Stage for SmtpProbeStage {
    fn name(&self) -> &'static str {
        "smtp_probe"
    }

    async fn run(&self, context: &mut ValidationContext) -> StageOutcome {
        let (Some(email), Some(mut outcome)) =
            (context.normalized.as_ref(), context.outcome.take())
        else {
            return StageOutcome::Finish(
                ValidationOutcome::new(
                    context.input.trim().to_string(),
                    EmailStatus::Unknown,
                    "smtp_probe",
                )
                .with_reason(
                    "PIPELINE_STATE_INVALID",
                    "A required validation stage was skipped",
                ),
            );
        };

        outcome.stage_reached = "smtp_probe".to_string();
        let domain = email.domain.as_str();
        let total_hosts = outcome.mx_hosts.len();
        for (index, mx_host) in outcome.mx_hosts.iter().enumerate() {
            debug!(%domain, %mx_host, attempt = index + 1, total_hosts, "Probing mail server");
            match self.prober.probe(mx_host, &email.value).await {
                SmtpProbeResult::Accepted { response } => {
                    info!(%domain, %mx_host, "Mail server accepted recipient");
                    outcome.status = EmailStatus::Valid;
                    return StageOutcome::Finish(outcome.with_reason(
                        "SMTP_ACCEPTED",
                        &format!(
                            "The mail server accepted the recipient at RCPT TO ({response}); actual delivery is not guaranteed"
                        ),
                    ));
                }
                SmtpProbeResult::Rejected { code, response } => {
                    info!(%domain, %mx_host, code, "Mail server rejected recipient");
                    outcome.status = EmailStatus::Invalid;
                    return StageOutcome::Finish(outcome.with_reason(
                        "SMTP_RECIPIENT_REJECTED",
                        &format!("The mail server rejected the recipient ({code} {response})"),
                    ));
                }
                SmtpProbeResult::Indeterminate {
                    reason_code,
                    detail,
                } => {
                    debug!(%domain, %mx_host, %reason_code, "Probe inconclusive, trying next host");
                    outcome.reason_code = Some(reason_code);
                    outcome.reason_detail = Some(detail);
                }
            }
        }

        outcome.status = EmailStatus::Unknown;
        if outcome.mx_hosts.is_empty() {
            return StageOutcome::Finish(outcome.with_reason(
                "SMTP_HOST_NOT_FOUND",
                "The domain has no mail server to probe",
            ));
        }
        StageOutcome::Finish(outcome)
    }
}

#[derive(Clone)]
pub struct ValidationService {
    stages: Vec<Arc<dyn Stage>>,
}

impl ValidationService {
    pub fn new(dns: Arc<dyn DnsResolver>) -> Self {
        Self::new_with_prober(dns, Arc::new(TcpSmtpProber::from_env()))
    }

    pub fn new_with_prober(dns: Arc<dyn DnsResolver>, prober: Arc<dyn SmtpProber>) -> Self {
        Self {
            stages: vec![
                Arc::new(SyntaxStage),
                Arc::new(DomainStage { dns }),
                Arc::new(ClassifyStage),
                Arc::new(SmtpProbeStage { prober }),
            ],
        }
    }


    pub fn from_env() -> Result<Self, hickory_resolver::error::ResolveError> {
        let resolver = match std::env::var("DNS_PROVIDER").as_deref() {
            Ok("google") => SystemDnsResolver::with_config(ResolverConfig::google()),
            Ok("cloudflare") => SystemDnsResolver::with_config(ResolverConfig::cloudflare()),
            _ => SystemDnsResolver::new()?, // default: system DNS
        };
        Ok(Self::new_with_prober(
            Arc::new(resolver),
            Arc::new(TcpSmtpProber::from_env()),
        ))
    }

    pub async fn validate(&self, input: &str) -> ValidationOutcome {
        let started = Instant::now();
        let mut context = ValidationContext::new(input);
        for stage in &self.stages {
            let stage_started = Instant::now();
            let result = stage.run(&mut context).await;
            let stage_ms = stage_started.elapsed().as_millis() as u64;
            match result {
                StageOutcome::Continue => {
                    debug!(stage = stage.name(), stage_ms, "Stage passed");
                }
                StageOutcome::Finish(outcome) => {
                    info!(
                        stage = stage.name(),
                        stage_ms,
                        total_ms = started.elapsed().as_millis() as u64,
                        status = ?outcome.status,
                        reason_code = outcome.reason_code.as_deref().unwrap_or("-"),
                        stage_reached = %outcome.stage_reached,
                        mx_hosts = outcome.mx_hosts.len(),
                        "Validation finished"
                    );
                    return outcome;
                }
            }
        }

        warn!("Validation pipeline ended without an outcome");
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
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::TcpListener,
    };

    use super::{
        DnsLookupError, DnsResolver, DomainLookup, MxHost, SmtpProbeResult, SmtpProber,
        ValidationService, smtp_recipient_probe_at,
    };
    use crate::validation::model::EmailStatus;

    struct FakeDns(DomainLookup);

    #[async_trait]
    impl DnsResolver for FakeDns {
        async fn lookup_domain(&self, _domain: &str) -> Result<DomainLookup, DnsLookupError> {
            Ok(self.0.clone())
        }
    }

    struct FakeSmtpProber(SmtpProbeResult);

    #[async_trait]
    impl SmtpProber for FakeSmtpProber {
        async fn probe(&self, _mx_host: &str, _recipient: &str) -> SmtpProbeResult {
            self.0.clone()
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
    async fn classification_flags_do_not_override_an_accepted_recipient() {
        let service = ValidationService::new_with_prober(
            Arc::new(FakeDns(DomainLookup::Mx(vec![MxHost {
                preference: 10,
                exchange: "mail.mailinator.com".to_string(),
            }]))),
            Arc::new(FakeSmtpProber(SmtpProbeResult::Accepted {
                response: "250 recipient accepted".to_string(),
            })),
        );
        let result = service.validate("person@mailinator.com").await;

        assert_eq!(result.status, EmailStatus::Valid);
        assert!(result.is_disposable);
        assert_eq!(result.reason_code.as_deref(), Some("SMTP_ACCEPTED"));
    }

    #[tokio::test]
    async fn role_address_is_not_rejected_when_the_recipient_is_accepted() {
        let service = ValidationService::new_with_prober(
            Arc::new(FakeDns(DomainLookup::Mx(vec![MxHost {
                preference: 10,
                exchange: "mx.example.com".to_string(),
            }]))),
            Arc::new(FakeSmtpProber(SmtpProbeResult::Accepted {
                response: "250 recipient accepted".to_string(),
            })),
        );
        let result = service.validate("support@example.com").await;

        assert_eq!(result.status, EmailStatus::Valid);
        assert!(result.is_role);
    }

    #[tokio::test]
    async fn reports_mailbox_rejection_as_invalid() {
        let service = ValidationService::new_with_prober(
            Arc::new(FakeDns(DomainLookup::Mx(vec![MxHost {
                preference: 10,
                exchange: "mx.example.com".to_string(),
            }]))),
            Arc::new(FakeSmtpProber(SmtpProbeResult::Rejected {
                code: 550,
                response: "550 no such user".to_string(),
            })),
        );
        let result = service.validate("person@example.com").await;

        assert_eq!(result.status, EmailStatus::Invalid);
        assert_eq!(
            result.reason_code.as_deref(),
            Some("SMTP_RECIPIENT_REJECTED")
        );
    }

    #[tokio::test]
    async fn keeps_inconclusive_smtp_results_unknown() {
        let service = ValidationService::new_with_prober(
            Arc::new(FakeDns(DomainLookup::Mx(vec![MxHost {
                preference: 10,
                exchange: "mx.example.com".to_string(),
            }]))),
            Arc::new(FakeSmtpProber(SmtpProbeResult::Indeterminate {
                reason_code: "SMTP_TEMPORARY_FAILURE".to_string(),
                detail: "451 try again later".to_string(),
            })),
        );
        let result = service.validate("person@example.com").await;

        assert_eq!(result.status, EmailStatus::Unknown);
        assert_eq!(
            result.reason_code.as_deref(),
            Some("SMTP_TEMPORARY_FAILURE")
        );
    }

    #[tokio::test]
    async fn smtp_probe_sends_recipient_command_without_sending_data() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            stream
                .get_mut()
                .write_all(b"220 mx.example.com ready\r\n")
                .await
                .unwrap();

            let expected = [
                (
                    "EHLO validator.example.com",
                    b"250-mx.example.com\r\n250 OK\r\n".as_slice(),
                ),
                (
                    "MAIL FROM:<probe@example.com>",
                    b"250 sender ok\r\n".as_slice(),
                ),
                (
                    "RCPT TO:<person@example.com>",
                    b"250 recipient ok\r\n".as_slice(),
                ),
                ("QUIT", b"221 bye\r\n".as_slice()),
            ];
            for (command, response) in expected {
                let mut received = String::new();
                stream.read_line(&mut received).await.unwrap();
                assert_eq!(received.trim_end(), command);
                stream.get_mut().write_all(response).await.unwrap();
            }
        });

        let result = smtp_recipient_probe_at(
            "127.0.0.1",
            address.port(),
            "person@example.com",
            "validator.example.com",
            "probe@example.com",
        )
        .await
        .unwrap();

        assert_eq!(result.code, 250);
        assert!(result.is_recipient_reply);
        server.await.unwrap();
    }
}
