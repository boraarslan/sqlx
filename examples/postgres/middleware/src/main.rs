//! Using Tower middleware with SQLx.
//!
//! Run with `DATABASE_URL=postgres://... cargo run`.

use std::time::{Duration, Instant};

use sqlx::middleware::trace::{QuerySummary, TraceLayer};
use sqlx::middleware::{self, Next, Request, RequestKind};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, Postgres};
use tower::limit::GlobalConcurrencyLimitLayer;
use tower::timeout::TimeoutLayer;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

/// Queries slower than this are logged by the `slow_query_log` middleware.
const SLOW_QUERY: Duration = Duration::from_millis(100);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Print each span when it closes, with its fields and duration.
    // In production, you'd export the spans with `tracing-opentelemetry` instead.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,sqlx::query=info".into()),
        )
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let options: PgConnectOptions = std::env::var("DATABASE_URL")
        .expect("Env var DATABASE_URL is required for this example.")
        .parse()?;

    // Middleware added to the connect options applies to every statement executed on every
    // connection of the pool. Layers are applied in order: the first one sees requests first.
    let options = options
        // A `tracing` span for each statement, following the OpenTelemetry conventions.
        .layer(TraceLayer::new())
        // Middleware can be written as an async closure.
        .layer(middleware::from_fn(slow_query_log))
        // Generic Tower middleware works too. This limits the time to the first row.
        .layer(TimeoutLayer::new(Duration::from_secs(5)));

    let pool = PgPoolOptions::new()
        .max_connections(5)
        // Middleware added to the pool options sees the queries executed on `&pool`,
        // including the time spent waiting for a connection.
        .layer(GlobalConcurrencyLimitLayer::new(3))
        .connect_with(options)
        .await?;

    let one: i32 = sqlx::query_scalar("SELECT 1")
        .extension(QuerySummary::new("SELECT one"))
        .fetch_one(&pool)
        .await?;
    println!("SELECT 1 returned {one}");

    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_sleep(0.2)")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    let rows: Vec<i32> = sqlx::query_scalar("SELECT generate_series(1, $1)")
        .bind(5i32)
        .fetch_all(&pool)
        .await?;
    println!("generate_series returned {rows:?}");

    Ok(())
}

/// Logs statements which take longer than [`SLOW_QUERY`] to start responding.
async fn slow_query_log(
    request: Request<Postgres>,
    next: Next<Postgres>,
) -> sqlx::Result<sqlx::middleware::Response<Postgres>> {
    let kind = request.kind();
    let sql = request.sql().clone();
    let start = Instant::now();

    let response = next.run(request).await;

    let elapsed = start.elapsed();

    if elapsed > SLOW_QUERY && kind != RequestKind::Prepare {
        tracing::warn!(?elapsed, sql = sql.as_str(), "slow statement");
    }

    response
}
