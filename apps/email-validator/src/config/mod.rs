use std::{env, io, net::SocketAddr};

pub struct Config {
    pub bind_addr: SocketAddr,
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

        Ok(Self { bind_addr })
    }
}
