use std::{
    io,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::timeout,
};
use tracing::{debug, info, warn};

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
        match &identity {
            SmtpIdentity::Configured {
                ehlo_host,
                mail_from,
            } => {
                let sender_domain = mail_from.rsplit_once('@').map_or("", |(_, domain)| domain);
                info!(%ehlo_host, %sender_domain, "SMTP probing enabled");
            }
            SmtpIdentity::Missing => warn!(
                "SMTP probing disabled: SMTP_EHLO_HOST and SMTP_MAIL_FROM are not set; \
                 deliverable addresses will stay unknown"
            ),
            SmtpIdentity::Invalid => warn!(
                "SMTP probing disabled: SMTP_EHLO_HOST or SMTP_MAIL_FROM is invalid or only one is set"
            ),
        }
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
            Err(_) => {
                warn!(%mx_host, timeout_secs = 10, "SMTP probe timed out");
                SmtpProbeResult::Indeterminate {
                    reason_code: "SMTP_TIMEOUT".to_string(),
                    detail: "The mail server did not respond before the probe timed out"
                        .to_string(),
                }
            }
            Ok(Err(error)) => {
                warn!(%mx_host, %error, "SMTP probe connection failed");
                SmtpProbeResult::Indeterminate {
                    reason_code: "SMTP_CONNECTION_FAILED".to_string(),
                    detail: format!("Could not complete the SMTP probe: {error}"),
                }
            }
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
            Ok(Ok(SmtpReply {
                code,
                response,
                is_recipient_reply,
            })) => {
                warn!(%mx_host, code, is_recipient_reply, %response, "SMTP probe was inconclusive");
                SmtpProbeResult::Indeterminate {
                    reason_code: if (400..500).contains(&code) {
                        "SMTP_TEMPORARY_FAILURE"
                    } else {
                        "SMTP_SERVER_REJECTED"
                    }
                    .to_string(),
                    detail: format!("SMTP server replied {code}: {response}"),
                }
            }
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
                    debug!(%mx_host, "Skipping SMTP probe: internationalized mailbox name");
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
            SmtpIdentity::Missing => {
                debug!(%mx_host, "Skipping SMTP probe: identity not configured");
                SmtpProbeResult::Indeterminate {
                    reason_code: "SMTP_PROBE_NOT_CONFIGURED".to_string(),
                    detail: "Set SMTP_EHLO_HOST and SMTP_MAIL_FROM to enable mailbox probing"
                        .to_string(),
                }
            }
            SmtpIdentity::Invalid => {
                debug!(%mx_host, "Skipping SMTP probe: identity invalid");
                SmtpProbeResult::Indeterminate {
                    reason_code: "SMTP_IDENTITY_INVALID".to_string(),
                    detail: "SMTP_EHLO_HOST or SMTP_MAIL_FROM is not a valid SMTP identity"
                        .to_string(),
                }
            }
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

pub(super) struct SmtpReply {
    pub(super) code: u16,
    response: String,
    pub(super) is_recipient_reply: bool,
}

async fn smtp_recipient_probe(
    mx_host: &str,
    recipient: &str,
    ehlo_host: &str,
    mail_from: &str,
) -> io::Result<SmtpReply> {
    smtp_recipient_probe_at(mx_host, 25, recipient, ehlo_host, mail_from).await
}

pub(super) async fn smtp_recipient_probe_at(
    mx_host: &str,
    port: u16,
    recipient: &str,
    ehlo_host: &str,
    mail_from: &str,
) -> io::Result<SmtpReply> {
    let connect_started = Instant::now();
    let stream = TcpStream::connect((mx_host, port)).await?;
    debug!(
        %mx_host,
        port,
        elapsed_ms = connect_started.elapsed().as_millis() as u64,
        "SMTP connection established"
    );
    let mut stream = BufReader::new(stream);

    let (code, response) = read_smtp_response(&mut stream).await?;
    debug!(%mx_host, code, step = "greeting", "SMTP reply");
    if code != 220 {
        return Ok(SmtpReply {
            code,
            response,
            is_recipient_reply: false,
        });
    }

    let (code, response) = smtp_command(&mut stream, &format!("EHLO {ehlo_host}")).await?;
    debug!(%mx_host, code, step = "ehlo", "SMTP reply");
    if code != 250 {
        return Ok(SmtpReply {
            code,
            response,
            is_recipient_reply: false,
        });
    }

    let (code, response) = smtp_command(&mut stream, &format!("MAIL FROM:<{mail_from}>")).await?;
    debug!(%mx_host, code, step = "mail_from", "SMTP reply");
    if code != 250 {
        return Ok(SmtpReply {
            code,
            response,
            is_recipient_reply: false,
        });
    }

    let (code, response) = smtp_command(&mut stream, &format!("RCPT TO:<{recipient}>")).await?;
    debug!(%mx_host, code, step = "rcpt_to", "SMTP reply");
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
