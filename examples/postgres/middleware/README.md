# Middleware

Shows how to add [Tower](https://docs.rs/tower) middleware to SQLx connections and pools:

* `sqlx::middleware::trace::TraceLayer` creates a `tracing` span for each statement, with fields following the
  [OpenTelemetry semantic conventions for database client spans](https://opentelemetry.io/docs/specs/semconv/database/database-spans/)
  (`db.system.name`, `db.query.text`, `db.namespace`, `server.address`, ...), exportable with
  `tracing-opentelemetry`. Datadog identifies SQL spans by the attribute names used before these conventions were
  stabilized (`db.system`, `db.statement`); `TraceLayer::new().legacy_attributes(true)` records those too.
* `sqlx::middleware::from_fn()` turns an async function into middleware; here, to log slow statements.
* `tower::timeout::TimeoutLayer` and `tower::limit::GlobalConcurrencyLimitLayer` are generic Tower middleware.
* `connect_layer()` adds middleware around opening connections; here, a span and a timeout for each attempt to
  connect.

## Usage

Start a Postgres server and run:

```
DATABASE_URL=postgres://postgres:password@localhost/postgres cargo run
```
