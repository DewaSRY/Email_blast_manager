mod app;
mod common;
mod health;
mod validator;

use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let app = app::create_app();

    let listener = TcpListener::bind("127.0.0.1:3000").await?;

    axum::Server::from_tcp(listener)?
        .serve(app.into_make_service())
        .await
}