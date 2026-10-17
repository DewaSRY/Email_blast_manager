mod app;
mod common;
mod config;
mod db;
mod health;
mod import;
mod mailer;
mod validation;
mod validator;

use tokio::net::TcpListener;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match dotenvy::dotenv() {
        Ok(path) => println!("Loaded .env from: {}", path.display()),
        Err(err) => println!("Could not load .env: {}", err),
    }

    println!(
        "SMTP_EHLO_HOST exists: {}",
        std::env::var("SMTP_EHLO_HOST").is_ok()
    );
    println!(
        "SMTP_MAIL_FROM exists: {}",
        std::env::var("SMTP_MAIL_FROM").is_ok()
    );
    // dotenvy::dotenv().ok();
    let config = config::Config::from_env()?;

    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,tower_http=info")),
        )
        .with(fmt::layer())
        .init();

    let db = db::connect(&config.database_url).await?;
    let imports = import::jobs::ImportQueue::connect(&config.redis_url).await?;
    let app = app::create_app(db, imports)?;

    let listener = TcpListener::bind(&config.bind_addr).await?;
    tracing::info!("Server listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}
