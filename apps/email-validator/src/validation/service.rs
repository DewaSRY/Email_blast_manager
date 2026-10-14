use std::{io, sync::Arc, time::Duration};

use async_trait::async_trait;
use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::{TokioAsyncResolver, error::ResolveErrorKind};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::timeout,
};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SmtpProbeResult {
    Accepted { response: String },
    Rejected { code: u16, response: String },
    Indeterminate { reason_code: String, detail: String },
}

#[async_trait]
pub trait SmtpProber: Send + Sync {
    async fn probe(&self, mx_host: &str, recipient: &str) -> SmtpProbeResult;
}

#[derive(Clone)]
enum SmtpIdentity {
    Configured {
        ehlo_host: String,
        mail_from: String,
    },
    Missing,
    Invalid,
}

#[derive(Clone)]
pub struct TcpSmtpProber {
    identity: SmtpIdentity,
}

impl TcpSmtpProber {
    pub fn from_env() -> Self {
        let identity = match (
            std::env::var("SMTP_EHLO_HOST"),
            std::env::var("SMTP_MAIL_FROM"),
        ) {
            (Ok(ehlo_host), Ok(mail_from))
                if valid_ehlo_host(&ehlo_host) && valid_mail_from(&mail_from) =>
            {
                SmtpIdentity::Configured {
                    ehlo_host,
                    mail_from,
                }
            }
            (Err(std::env::VarError::NotPresent), Err(std::env::VarError::NotPresent)) => {
                SmtpIdentity::Missing
            }
            _ => SmtpIdentity::Invalid,
        };
        Self { identity }
    }

    async fn probe_configured(
        &self,
        mx_host: &str,
        recipient: &str,
        ehlo_host: &str,
        mail_from: &str,
    ) -> SmtpProbeResult {
        match timeout(
            Duration::from_secs(10),
            smtp_recipient_probe(mx_host, recipient, ehlo_host, mail_from),
        )
        .await
        {
            Err(_) => SmtpProbeResult::Indeterminate {
                reason_code: "SMTP_TIMEOUT".to_string(),
                detail: "The mail server did not respond before the probe timed out".to_string(),
            },
            Ok(Err(error)) => SmtpProbeResult::Indeterminate {
                reason_code: "SMTP_CONNECTION_FAILED".to_string(),
                detail: format!("Could not complete the SMTP probe: {error}"),
            },
            Ok(Ok(SmtpReply {
                code: 200..=299,
                response,
                is_recipient_reply: true,
            })) => SmtpProbeResult::Accepted { response },
            Ok(Ok(SmtpReply {
                code: code @ (550 | 551 | 553),
                response,
                is_recipient_reply: true,
            })) => SmtpProbeResult::Rejected { code, response },
            Ok(Ok(SmtpReply { code, response, .. })) => SmtpProbeResult::Indeterminate {
                reason_code: if (400..500).contains(&code) {
                    "SMTP_TEMPORARY_FAILURE"
                } else {
                    "SMTP_SERVER_REJECTED"
                }
                .to_string(),
                detail: format!("SMTP server replied {code}: {response}"),
            },
        }
    }
}

#[async_trait]
impl SmtpProber for TcpSmtpProber {
    async fn probe(&self, mx_host: &str, recipient: &str) -> SmtpProbeResult {
        match &self.identity {
            SmtpIdentity::Configured {
                ehlo_host,
                mail_from,
            } => {
                if !recipient.is_ascii() {
                    return SmtpProbeResult::Indeterminate {
                        reason_code: "SMTPUTF8_NOT_SUPPORTED".to_string(),
                        detail:
                            "Internationalized mailbox names cannot be probed without SMTPUTF8 support"
                                .to_string(),
                    };
                }
                self.probe_configured(mx_host, recipient, ehlo_host, mail_from)
                    .await
            }
            SmtpIdentity::Missing => SmtpProbeResult::Indeterminate {
                reason_code: "SMTP_PROBE_NOT_CONFIGURED".to_string(),
                detail: "Set SMTP_EHLO_HOST and SMTP_MAIL_FROM to enable mailbox probing"
                    .to_string(),
            },
            SmtpIdentity::Invalid => SmtpProbeResult::Indeterminate {
                reason_code: "SMTP_IDENTITY_INVALID".to_string(),
                detail: "SMTP_EHLO_HOST or SMTP_MAIL_FROM is not a valid SMTP identity".to_string(),
            },
        }
    }
}

fn valid_ehlo_host(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 253
        && host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
}

fn valid_mail_from(address: &str) -> bool {
    !address.is_empty()
        && address.len() <= 254
        && address.is_ascii()
        && !address.contains(['\r', '\n', '<', '>'])
        && email_address::EmailAddress::is_valid(address)
}

struct SmtpReply {
    code: u16,
    response: String,
    is_recipient_reply: bool,
}

async fn smtp_recipient_probe(
    mx_host: &str,
    recipient: &str,
    ehlo_host: &str,
    mail_from: &str,
) -> io::Result<SmtpReply> {
    smtp_recipient_probe_at(mx_host, 25, recipient, ehlo_host, mail_from).await
}

async fn smtp_recipient_probe_at(
    mx_host: &str,
    port: u16,
    recipient: &str,
    ehlo_host: &str,
    mail_from: &str,
) -> io::Result<SmtpReply> {
    let stream = TcpStream::connect((mx_host, port)).await?;
    let mut stream = BufReader::new(stream);

    let (code, response) = read_smtp_response(&mut stream).await?;
    if code != 220 {
        return Ok(SmtpReply {
            code,
            response,
            is_recipient_reply: false,
        });
    }

    let (code, response) = smtp_command(&mut stream, &format!("EHLO {ehlo_host}")).await?;
    if code != 250 {
        return Ok(SmtpReply {
            code,
            response,
            is_recipient_reply: false,
        });
    }

    let (code, response) = smtp_command(&mut stream, &format!("MAIL FROM:<{mail_from}>")).await?;
    if code != 250 {
        return Ok(SmtpReply {
            code,
            response,
            is_recipient_reply: false,
        });
    }

    let (code, response) = smtp_command(&mut stream, &format!("RCPT TO:<{recipient}>")).await?;
    let _ = stream.get_mut().write_all(b"QUIT\r\n").await;
    Ok(SmtpReply {
        code,
        response,
        is_recipient_reply: true,
    })
}

async fn smtp_command(
    stream: &mut BufReader<TcpStream>,
    command: &str,
) -> io::Result<(u16, String)> {
    stream.get_mut().write_all(command.as_bytes()).await?;
    stream.get_mut().write_all(b"\r\n").await?;
    stream.get_mut().flush().await?;
    read_smtp_response(stream).await
}

async fn read_smtp_response(stream: &mut BufReader<TcpStream>) -> io::Result<(u16, String)> {
    let mut response = String::new();
    let mut expected_code = None;

    loop {
        let mut line = String::new();
        if stream.read_line(&mut line).await? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "SMTP server closed the connection",
            ));
        }
        if response.len() + line.len() > 4096 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "SMTP response exceeded 4096 bytes",
            ));
        }

        let code = line
            .get(..3)
            .and_then(|value| value.parse::<u16>().ok())
            .filter(|value| (100..=599).contains(value))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid SMTP response"))?;
        if expected_code.is_some_and(|expected| expected != code) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "SMTP multiline response used inconsistent status codes",
            ));
        }
        expected_code = Some(code);
        response.push_str(line.trim_end());

        match line.as_bytes().get(3) {
            Some(b'-') => response.push(' '),
            Some(b' ') | None => return Ok((code, response)),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid SMTP response separator",
                ));
            }
        }
    }
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

    pub fn with_config(config: ResolverConfig) -> Self {
        let mut opts = ResolverOpts::default();
        opts.timeout = std::time::Duration::from_secs(3);
        opts.attempts = 2;
        Self {
            resolver: TokioAsyncResolver::tokio(config, opts),
        }
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
        for mx_host in &outcome.mx_hosts {
            match self.prober.probe(mx_host, &email.value).await {
                SmtpProbeResult::Accepted { response } => {
                    outcome.status = EmailStatus::Valid;
                    return StageOutcome::Finish(outcome.with_reason(
                        "SMTP_ACCEPTED",
                        &format!(
                            "The mail server accepted the recipient at RCPT TO ({response}); actual delivery is not guaranteed"
                        ),
                    ));
                }
                SmtpProbeResult::Rejected { code, response } => {
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

    // pub fn system() -> Result<Self, hickory_resolver::error::ResolveError> {
    //     Ok(Self::new(Arc::new(SystemDnsResolver::new()?)))
    // }

    pub fn from_env() -> Result<Self, hickory_resolver::error::ResolveError> {
        let resolver = match std::env::var("DNS_PROVIDER").as_deref() {
            Ok("google") => SystemDnsResolver::with_config(ResolverConfig::google()),
            Ok("cloudflare") => SystemDnsResolver::with_config(ResolverConfig::cloudflare()),
            _ => SystemDnsResolver::new()?, // default: system DNS
        };
        Ok(Self::new(Arc::new(resolver)))
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
