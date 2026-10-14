use sqlx::{migrate::Migrator, postgres::PgPoolOptions, PgPool};

// Embedded at compile time from ./migrate (files named `<version>_<name>.sql`).
static MIGRATOR: Migrator = sqlx::migrate!("./migrate");

pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await?;

    MIGRATOR.run(&pool).await?;
    tracing::info!("Database migrations applied");
    Ok(pool)
}
