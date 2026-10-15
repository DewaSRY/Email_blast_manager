use async_trait::async_trait;
use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::{TokioAsyncResolver, error::ResolveErrorKind};
use tracing::{debug, warn};

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
            Err(error) if is_no_records(error.kind()) => {
                debug!(%domain, "No MX records, trying A/AAAA fallback");
            }
            Err(error) => {
                warn!(%domain, %error, "MX lookup failed");
                return Err(DnsLookupError::Temporary);
            }
        }

        match self.resolver.lookup_ip(&fully_qualified_domain).await {
            Ok(records) if records.iter().next().is_some() => Ok(DomainLookup::AddressFallback),
            Ok(_) => Ok(DomainLookup::NoMx),
            Err(error) if is_nx_domain(error.kind()) => Ok(DomainLookup::NotFound),
            Err(error) if is_no_records(error.kind()) => Ok(DomainLookup::NoMx),
            Err(error) => {
                warn!(%domain, %error, "Address fallback lookup failed");
                Err(DnsLookupError::Temporary)
            }
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
