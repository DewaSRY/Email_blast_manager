use std::{env, io, net::SocketAddr};

#[derive(Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub redis_url: String,
}

impl Config {
    pub fn from_env() -> io::Result<Self> {
        let bind_addr = match env::var("BIND_ADDR") {
            Ok(value) => value.parse().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("BIND_ADDR must be a socket address: {error}"),
                )
            })?,
            Err(env::VarError::NotPresent) => SocketAddr::from(([0, 0, 0, 0], 3000)),
            Err(error @ env::VarError::NotUnicode(_)) => {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, error));
            }
        };

        let database_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://user:password@localhost:5442/mydatabase".to_string());

        let redis_url =
            env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

        Ok(Self {
            bind_addr,
            database_url,
            redis_url,
        })
    }
}
