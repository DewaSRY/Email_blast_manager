mod app;
mod common;
mod config;
mod db;
mod health;
mod import;
mod validation;
mod validator;

use tokio::net::TcpListener;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = config::Config::from_env()?;

    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,tower_http=info")),
        )
        .with(fmt::layer())
        .init();

    let db = db::connect(&config.database_url).await?;
    let app = app::create_app(config.clone(), db)?;

    let listener = TcpListener::bind(&config.bind_addr).await?;
    tracing::info!("Server listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}
