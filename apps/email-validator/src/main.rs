mod app;
mod common;
mod config;
mod health;
mod validation;
mod validator;

use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = app::create_app()?;
    let config = config::Config::from_env()?;
    let listener = TcpListener::bind(config.bind_addr).await?;

    Ok(axum::serve(listener, app).await?)
}
