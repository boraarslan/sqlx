//! Middleware which creates a [`tracing`] span for each request, following the
//! [OpenTelemetry semantic conventions for database client spans][otel].
//!
//! See [`TraceLayer`] for details.
//!
//! [otel]: https://opentelemetry.io/docs/specs/semconv/database/database-spans/

use std::borrow::Cow;
use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

use either::Either;
use futures_core::future::BoxFuture;
use futures_core::Stream;
use futures_util::StreamExt;
use tracing::field::Empty;
use tracing::{Instrument, Span};

use crate::database::Database;
use crate::error::{BoxDynError, Error};
use crate::middleware::{
    into_sqlx_error, ConnectRequest, ConnectionInfo, Layer, Request, Response, ResultItem,
    ResultStream, Service,
};

/// A [`Layer`] which creates a [`tracing`] span for each request, following the
/// [OpenTelemetry semantic conventions for database client spans][otel].
///
/// The span covers the whole execution of the request: for queries, it is entered whenever the
/// result stream is polled, and is closed when the stream is exhausted or dropped.
///
/// Spans are created at the `INFO` level with the target `sqlx::query` and the name
/// `sqlx.query`, and have the following fields:
///
/// | Field | Value |
/// |---|---|
/// | `otel.name` | The [`QuerySummary`] of the request if set, otherwise `db.system.name` |
/// | `otel.kind` | `client` |
/// | `otel.status_code` | `ERROR` if the request failed |
/// | `db.system.name` | `postgresql`, `mysql`, `sqlite`, or the lowercase name of the database |
/// | `db.namespace` | The name of the database (or the path of an SQLite database file) |
/// | `db.query.text` | The SQL of the request, unless disabled with [`query_text(false)`][Self::query_text] |
/// | `db.query.summary` | The [`QuerySummary`] of the request, if set |
/// | `db.response.returned_rows` | The number of rows returned |
/// | `db.response.status_code` | The error code returned by the database, if any (e.g. `SQLSTATE`) |
/// | `error.type` | The error code returned by the database, or a short description of the error kind |
/// | `server.address` | The host name, IP address or socket path of the server, if any |
/// | `server.port` | The port of the server, if connecting over TCP |
/// | `sqlx.request.kind` | The [`RequestKind`][super::RequestKind] of the request, e.g. `fetch` or `begin` |
///
/// The `otel.*` fields are special fields interpreted by `tracing-opentelemetry`.
///
/// # Legacy Attributes and Datadog
/// The names above are those of the stable OpenTelemetry conventions. Some backends still expect
/// the names used before they were stabilized: with
/// [`legacy_attributes(true)`][Self::legacy_attributes], spans also have the fields
/// `db.system` (same as `db.system.name`), `db.statement` (same as `db.query.text`) and
/// `db.name` (same as `db.namespace`), like the `database/dup` mode of other OpenTelemetry
/// instrumentations.
///
/// Datadog [documents][datadog] `db.system` and `db.statement` as the attributes which make an
/// OpenTelemetry span a SQL span (`span.type = sql`) with the normalized query as its resource
/// name, so enable `legacy_attributes` when exporting to Datadog.
///
/// # Connecting
/// `TraceLayer` can also be added with [`ConnectOptions::connect_layer()`] to create a span for
/// each attempt to open a connection. These spans have the target `sqlx::connect`, the name
/// `sqlx.connect`, and the fields above that apply to connecting: `otel.name` (which is
/// `connect`), `otel.kind`, `otel.status_code`, `db.system.name`, `db.namespace`,
/// `db.response.status_code`, `error.type`, `server.address` and `server.port`.
///
/// [`ConnectOptions::connect_layer()`]: crate::connection::ConnectOptions::connect_layer
///
/// # Example
/// ```rust,no_run
/// use sqlx::middleware::trace::TraceLayer;
/// use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
///
/// # async fn example() -> sqlx::Result<()> {
/// let pool = PgPoolOptions::new()
///     .connect_with(
///         "postgres://localhost/mydb"
///             .parse::<PgConnectOptions>()?
///             .layer(TraceLayer::new()),
///     )
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// [otel]: https://opentelemetry.io/docs/specs/semconv/database/database-spans/
/// [datadog]: https://docs.datadoghq.com/opentelemetry/correlate/dbm_and_traces/
#[derive(Clone, Debug)]
pub struct TraceLayer {
    query_text: bool,
    legacy_attributes: bool,
}

impl TraceLayer {
    /// Create a layer with the default configuration.
    pub fn new() -> Self {
        Self {
            query_text: true,
            legacy_attributes: false,
        }
    }

    /// Set whether to record the SQL of each request in the `db.query.text` field
    /// (and `db.statement`, with [`legacy_attributes(true)`][Self::legacy_attributes]).
    ///
    /// Defaults to `true`, as recommended by the OpenTelemetry conventions for parameterized
    /// queries. Disable this if your queries may contain sensitive literal values.
    pub fn query_text(mut self, enabled: bool) -> Self {
        self.query_text = enabled;
        self
    }

    /// Set whether to also record the attribute names used before the OpenTelemetry database
    /// conventions were stabilized: `db.system`, `db.statement` and `db.name`.
    ///
    /// Defaults to `false`. Enable this when exporting to Datadog; see the
    /// [type-level documentation](Self#legacy-attributes-and-datadog).
    pub fn legacy_attributes(mut self, enabled: bool) -> Self {
        self.legacy_attributes = enabled;
        self
    }
}

impl Default for TraceLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for TraceLayer {
    type Service = Trace<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Trace {
            inner,
            query_text: self.query_text,
            legacy_attributes: self.legacy_attributes,
        }
    }
}

/// A low-cardinality summary of a query, used as the name of the span created by [`TraceLayer`]
/// and recorded in `db.query.summary`.
///
/// Attach it to a query with [`Query::extension()`][crate::query::Query::extension].
///
/// The OpenTelemetry conventions recommend a summary of the form `{operation} {target}`,
/// for example `SELECT users` or `INSERT orders`.
///
/// ```rust,no_run
/// # async fn example(pool: &sqlx::PgPool) -> sqlx::Result<()> {
/// use sqlx::middleware::trace::QuerySummary;
///
/// sqlx::query("SELECT * FROM users WHERE id = $1")
///     .bind(1i64)
///     .extension(QuerySummary::new("SELECT users"))
///     .fetch_optional(pool)
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuerySummary(pub Cow<'static, str>);

impl QuerySummary {
    /// Create a summary.
    pub fn new(summary: impl Into<Cow<'static, str>>) -> Self {
        Self(summary.into())
    }
}

/// The [`Service`] created by [`TraceLayer`].
#[derive(Clone)]
pub struct Trace<S> {
    inner: S,
    query_text: bool,
    legacy_attributes: bool,
}

impl<S> Debug for Trace<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Trace")
            .field("query_text", &self.query_text)
            .field("legacy_attributes", &self.legacy_attributes)
            .finish_non_exhaustive()
    }
}

impl<DB, S> Service<Request<DB>> for Trace<S>
where
    DB: Database,
    S: Service<Request<DB>, Response = Response<DB>>,
    S::Error: Into<BoxDynError> + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<DB>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Response<DB>, Error>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(into_sqlx_error)
    }

    fn call(&mut self, request: Request<DB>) -> Self::Future {
        let span = request_span(&request, self.query_text, self.legacy_attributes);
        let response = self.inner.call(request).instrument(span.clone());

        Box::pin(async move {
            match response.await.map_err(into_sqlx_error) {
                Ok(response) => Ok(response.map_stream(|stream| {
                    ResultStream::new(TracedStream {
                        stream,
                        span,
                        rows: 0,
                        finished: false,
                    })
                })),
                Err(e) => {
                    record_error(&span, &e);
                    Err(e)
                }
            }
        })
    }
}

impl<DB, S> Service<ConnectRequest<DB>> for Trace<S>
where
    DB: Database,
    S: Service<ConnectRequest<DB>, Response = DB::Connection>,
    S::Error: Into<BoxDynError> + 'static,
    S::Future: Send + 'static,
{
    type Response = DB::Connection;
    type Error = Error;
    type Future = BoxFuture<'static, Result<DB::Connection, Error>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(into_sqlx_error)
    }

    fn call(&mut self, request: ConnectRequest<DB>) -> Self::Future {
        let span = connect_span(&request, self.legacy_attributes);
        let response = self.inner.call(request).instrument(span.clone());

        Box::pin(async move {
            response
                .await
                .map_err(into_sqlx_error)
                .inspect_err(|e| record_error(&span, e))
        })
    }
}

fn connect_span<DB: Database>(request: &ConnectRequest<DB>, legacy_attributes: bool) -> Span {
    let system = system_name::<DB>();

    let span = tracing::info_span!(
        target: "sqlx::connect",
        "sqlx.connect",
        otel.name = "connect",
        otel.kind = "client",
        otel.status_code = Empty,
        otel.status_description = Empty,
        db.system.name = &*system,
        db.namespace = Empty,
        db.response.status_code = Empty,
        // Quoted, as `type` is a keyword (which needs tracing 0.1.41 otherwise).
        "error.type" = Empty,
        server.address = Empty,
        server.port = Empty,
        db.system = Empty,
        db.name = Empty,
    );

    if !span.is_disabled() {
        if legacy_attributes {
            span.record("db.system", &*system);
        }

        record_connection_info(&span, &request.connection_info(), legacy_attributes);
    }

    span
}

fn record_connection_info(span: &Span, info: &ConnectionInfo, legacy_attributes: bool) {
    if let Some(database) = &info.database {
        span.record("db.namespace", database.as_str());

        if legacy_attributes {
            span.record("db.name", database.as_str());
        }
    }

    if let Some(address) = &info.server_address {
        span.record("server.address", address.as_str());
    }

    if let Some(port) = info.server_port {
        span.record("server.port", port);
    }
}

fn request_span<DB: Database>(
    request: &Request<DB>,
    query_text: bool,
    legacy_attributes: bool,
) -> Span {
    let system = system_name::<DB>();
    let summary = request
        .extensions()
        .get::<QuerySummary>()
        .map(|summary| &*summary.0);

    let span = tracing::info_span!(
        target: "sqlx::query",
        "sqlx.query",
        otel.name = summary.unwrap_or(&system),
        otel.kind = "client",
        otel.status_code = Empty,
        otel.status_description = Empty,
        db.system.name = &*system,
        db.namespace = Empty,
        db.query.text = Empty,
        db.query.summary = summary,
        db.response.returned_rows = Empty,
        db.response.status_code = Empty,
        // Quoted, as `type` is a keyword (which needs tracing 0.1.41 otherwise).
        "error.type" = Empty,
        server.address = Empty,
        server.port = Empty,
        sqlx.request.kind = request.kind().as_str(),
        db.system = Empty,
        db.statement = Empty,
        db.name = Empty,
    );

    if span.is_disabled() {
        return span;
    }

    if legacy_attributes {
        span.record("db.system", &*system);
    }

    if query_text {
        span.record("db.query.text", request.sql().as_str());

        if legacy_attributes {
            span.record("db.statement", request.sql().as_str());
        }
    }

    if let Some(info) = request.connection_info() {
        record_connection_info(&span, info, legacy_attributes);
    }

    span
}

/// The value of `db.system.name` for the database.
fn system_name<DB: Database>() -> Cow<'static, str> {
    match DB::NAME {
        "PostgreSQL" => "postgresql".into(),
        "MySQL" => "mysql".into(),
        "SQLite" => "sqlite".into(),
        other => other.to_ascii_lowercase().into(),
    }
}

fn record_error(span: &Span, error: &Error) {
    if span.is_disabled() {
        return;
    }

    span.record("otel.status_code", "ERROR");
    span.record("otel.status_description", tracing::field::display(error));

    let code = error.as_database_error().and_then(|e| e.code());

    match code {
        Some(code) => {
            span.record("db.response.status_code", &*code);
            span.record("error.type", &*code);
        }
        None => {
            span.record("error.type", error_type(error));
        }
    }
}

fn error_type(error: &Error) -> &'static str {
    match error {
        Error::Configuration(_) => "configuration",
        Error::InvalidArgument(_) => "invalid_argument",
        Error::Database(_) => "database",
        Error::Io(_) => "io",
        Error::Tls(_) => "tls",
        Error::Protocol(_) => "protocol",
        Error::RowNotFound => "row_not_found",
        Error::TypeNotFound { .. } => "type_not_found",
        Error::ColumnIndexOutOfBounds { .. } => "column_index_out_of_bounds",
        Error::ColumnNotFound(_) => "column_not_found",
        Error::ColumnDecode { .. } => "column_decode",
        Error::Encode(_) => "encode",
        Error::Decode(_) => "decode",
        Error::AnyDriverError(_) => "any_driver",
        Error::PoolTimedOut => "pool_timed_out",
        Error::PoolClosed => "pool_closed",
        Error::WorkerCrashed => "worker_crashed",
        Error::Middleware(_) => "middleware",
        _ => "_OTHER",
    }
}

/// Keeps the span of a request open until its result stream is exhausted or dropped.
struct TracedStream<DB: Database> {
    stream: ResultStream<DB>,
    span: Span,
    rows: u64,
    finished: bool,
}

impl<DB: Database> TracedStream<DB> {
    fn finish(&mut self) {
        if !self.finished {
            self.finished = true;
            self.span.record("db.response.returned_rows", self.rows);
        }
    }
}

impl<DB: Database> Stream for TracedStream<DB> {
    type Item = ResultItem<DB>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let res = {
            let _entered = this.span.enter();
            this.stream.poll_next_unpin(cx)
        };

        match &res {
            Poll::Ready(Some(Ok(Either::Right(_)))) => this.rows += 1,
            Poll::Ready(Some(Err(e))) => record_error(&this.span, e),
            Poll::Ready(None) => this.finish(),
            _ => (),
        }

        res
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<DB: Database> Drop for TracedStream<DB> {
    fn drop(&mut self) {
        self.finish();
    }
}

// We never pin-project into the fields.
impl<DB: Database> Unpin for TracedStream<DB> {}
