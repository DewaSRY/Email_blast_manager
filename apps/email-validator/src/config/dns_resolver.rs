use hickory_resolver::config::{ResolverConfig, ResolverOpts};

impl SystemDnsResolver {
    // existing: uses your computer's DNS settings
    pub fn new() -> Result<Self, hickory_resolver::error::ResolveError> {
        Ok(Self {
            resolver: TokioAsyncResolver::tokio_from_system_conf()?,
        })
    }

    // new: uses a DNS server you choose
    pub fn with_config(config: ResolverConfig) -> Self {
        let mut opts = ResolverOpts::default();
        opts.timeout = std::time::Duration::from_secs(3);
        opts.attempts = 2;
        Self {
            resolver: TokioAsyncResolver::tokio(config, opts),
        }
    }
}