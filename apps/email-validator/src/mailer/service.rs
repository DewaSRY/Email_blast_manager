use std::env;

use lettre::{
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
    message::{Mailbox, header::ContentType},
    transport::smtp::{
        authentication::{Credentials, Mechanism},
        client::{Tls, TlsParameters},
    },
};

#[derive(Clone)]
pub struct MailerService {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: Mailbox,
}

impl MailerService {
    /// Returns `Ok(None)` when `KUMO_SMTP_PASSWORD` is unset (mailer disabled).
    pub fn from_env() -> Result<Option<Self>, String> {
        let Ok(password) = env::var("KUMO_SMTP_PASSWORD") else {
            return Ok(None);
        };
        let host = env::var("KUMO_SMTP_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        let port: u16 = env::var("KUMO_SMTP_PORT")
            .unwrap_or_else(|_| "2525".into())
            .parse()
            .map_err(|e| format!("KUMO_SMTP_PORT invalid: {e}"))?;
        let username = env::var("KUMO_SMTP_USERNAME").unwrap_or_else(|_| "devapp".into());
        let from = env::var("KUMO_MAIL_FROM").unwrap_or_else(|_| "noreply@send.example.com".into());
        let accept_invalid_certs = env::var("KUMO_SMTP_ACCEPT_INVALID_CERTS")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let tls = TlsParameters::builder(host.clone())
            .dangerous_accept_invalid_certs(accept_invalid_certs)
            .dangerous_accept_invalid_hostnames(accept_invalid_certs)
            .build()
            .map_err(|e| format!("TLS setup failed: {e}"))?;

        // KumoMTA only offers AUTH after STARTTLS, so STARTTLS is mandatory.
        let transport = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(host)
            .port(port)
            .tls(Tls::Required(tls))
            .credentials(Credentials::new(username, password))
            .authentication(vec![Mechanism::Plain])
            .build();

        Ok(Some(Self {
            transport,
            from: from.parse().map_err(|e| format!("KUMO_MAIL_FROM invalid: {e}"))?,
        }))
    }

    pub async fn send(&self, to: &str, subject: &str, body: &str) -> Result<String, String> {
        let message = Message::builder()
            .from(self.from.clone())
            .to(to.parse().map_err(|e| format!("invalid recipient: {e}"))?)
            .subject(subject)
            .header(ContentType::TEXT_PLAIN)
            .body(body.to_string())
            .map_err(|e| e.to_string())?;

        let response = self.transport.send(message).await.map_err(|e| e.to_string())?;
        Ok(response.message().collect::<Vec<_>>().join(" "))
    }
}
