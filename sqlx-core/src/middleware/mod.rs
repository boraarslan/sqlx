//! Middleware support using [Tower].
//!
//! Every statement SQLx sends to the database on behalf of the user (queries, statement
//! preparation and transaction control) is represented as a [`Request`] and sent through a
//! stack of [`Service`]s, which answers with a [`Response`].
//! Any [`Layer`] that works with generic Tower services can be added to this stack,
//! including those in the `tower` crate (timeouts, concurrency and rate limits, retries, ...).
//!
//! Middleware can be added in four places:
//!
//! * **Connections**: [`ConnectOptions::layer()`] (and the inherent `layer()` methods of each
//!   driver's connect options) adds a layer to every connection made with those options,
//!   including connections opened by a [`Pool`]. All statements executed on such a connection
//!   pass through the layer, whether they come from `&mut Connection`, a [`PoolConnection`], a
//!   [`Transaction`] or a `Pool` executor. Each connection gets its own instance of the stack.
//! * **Pools**: [`PoolOptions::layer()`] adds a layer that sees queries executed directly on a
//!   `&Pool`, *before* a connection is acquired, so the time spent waiting for a connection is
//!   part of the request. There is one instance of the stack, shared by all users of the pool.
//! * **Ad-hoc executors**: connections, transactions and pools implement
//!   `Service<Request<DB>>`, so any of them can be wrapped in layers, e.g. with
//!   `tower::ServiceBuilder`, and turned back into an [`Executor`] with [`ServiceExecutor`].
//! * **Connecting**: [`ConnectOptions::connect_layer()`] adds a layer around opening
//!   connections; see [Connecting](#connecting).
//!
//! Layers are applied like `tower::ServiceBuilder`: the first layer added is the outermost
//! and sees each request first.
//!
//! # Writing Middleware
//! The easiest way to write middleware is [`from_fn()`], which takes an async function or
//! closure receiving each [`Request`] and a [`Next`] to pass it on:
//!
//! ```rust,no_run
//! use sqlx::middleware::{self, Next, Request};
//! use sqlx::postgres::{PgConnectOptions, PgPoolOptions, Postgres};
//!
//! # async fn example() -> sqlx::Result<()> {
//! let options: PgConnectOptions = "postgres://localhost/mydb".parse()?;
//!
//! let pool = PgPoolOptions::new()
//!     .connect_with(options.layer(middleware::from_fn(
//!         |request: Request<Postgres>, next: Next<Postgres>| async move {
//!             if request.sql().as_str().contains("DROP") {
//!                 return Err(sqlx::Error::InvalidArgument("not allowed".into()));
//!             }
//!
//!             next.run(request).await
//!         },
//!     )))
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! Middleware can also be written like any other Tower middleware, by implementing
//! [`Layer`] and [`Service`] (see the example at the end of this page).
//!
//! # Requests and Responses
//! A [`Request`] owns its SQL, bind arguments and [`Extensions`], and its [`RequestKind`]
//! determines the [`Response`] variant:
//!
//! | [`RequestKind`] | [`Response`] |
//! |---|---|
//! | `Fetch`, `FetchOptional` | `Stream(ResultStream)` |
//! | `Prepare` | `Statement` |
//! | `Begin`, `Commit`, `Rollback` | `Done` |
//!
//! Before a request enters the stack of a connection or pool, the connection sets its
//! [`ConnectionInfo`] (database name, server address, ...) and
//! [transaction depth][Request::transaction_depth]. Values can be attached to requests with
//! [`Query::extension()`][crate::query::Query::extension] or by middleware, through
//! [`Request::extensions_mut()`].
//!
//! Middleware may change the request, for example to rewrite its SQL (e.g. to add a comment
//! with trace context; note that a different SQL string is a different prepared statement, so
//! comments which change with every query defeat the statement cache), or return a response
//! without passing the request on.
//!
//! ## Streaming
//! The future returned by a service completes once the database has started responding
//! (the first row or result was received, or an error). The rest of the rows
//! are streamed through the [`ResultStream`], which is `'static` and keeps the connection reserved
//! until it is exhausted or dropped.
//!
//! This has some consequences for generic Tower middleware, which only sees the response future:
//! * Timeouts (e.g. `tower::timeout::Timeout`) limit the time until the first row is received.
//! * Concurrency limits (e.g. `tower::limit::ConcurrencyLimit`) are released when the first
//!   row is received.
//!
//! Middleware that needs to observe the whole execution of a query, such as tracing,
//! should wrap the stream with [`Response::map_stream()`], like [`trace::TraceLayer`] does.
//!
//! The stream of a response keeps the connection busy, even though it doesn't borrow it: when
//! using a connection as a `Service` directly, drop the stream of a response before sending
//! the next request on the same task, or that request waits forever.
//!
//! ## Cancellation
//! Dropping a response future or stream before it completes does not cancel the statement on
//! the server: the connection discards the rest of the response before executing the next
//! request. In particular, a timeout does not stop a long-running query; the next request
//! on the connection waits for the query to finish. Consider setting a statement timeout on the
//! server (e.g. `statement_timeout` in Postgres) as well.
//!
//! If a request is dropped while waiting for the middleware stack of a connection to be ready,
//! the stack is rebuilt (losing the state of its services), as a service may hold resources it
//! reserved while getting ready (like a permit of a concurrency limit shared with other
//! connections) until it's called or dropped.
//!
//! ## Transactions
//! `BEGIN`, `COMMIT` and `ROLLBACK` requests update the transaction state of the connection
//! when the driver completes them. Middleware must not keep them running after their response
//! future is dropped (e.g. by spawning them): a `BEGIN` that completes after its
//! [`Transaction`] was dropped leaves the connection in a transaction that nothing ends.
//! (A [`Pool`] closes connections returned to it in this state.)
//!
//! Retrying requests inside a transaction is rarely correct, as the failure likely aborted
//! the transaction; see [`Request::in_transaction()`]. In particular, a `COMMIT` that failed
//! (e.g. with a serialization failure) may have ended the transaction: retrying it can then
//! report success for a transaction that was rolled back.
//!
//! # Connecting
//! Each attempt to open a connection, including those made by a [`Pool`], is sent through the
//! layers added with [`ConnectOptions::connect_layer()`] as a [`ConnectRequest`], which holds the
//! connect options. The response is the new connection. Connect middleware can:
//!
//! * change the options, e.g. to use a password that's only valid for a short time (like an
//!   AWS RDS IAM authentication token) or to pick one of several servers;
//! * time out or retry attempts to connect, or limit how often they're made
//!   (e.g. with `tower::timeout`, `tower::retry` or `tower::limit::RateLimit`);
//! * trace them, with [`trace::TraceLayer`].
//!
//! [`connect_fn()`] creates connect middleware from an async function, like [`from_fn()`]:
//!
//! ```rust,no_run
//! use sqlx::middleware::{self, ConnectNext, ConnectRequest};
//! use sqlx::postgres::{PgConnectOptions, Postgres};
//! use sqlx::{Connection, PgConnection};
//!
//! # async fn example() -> sqlx::Result<()> {
//! let options: PgConnectOptions = "postgres://localhost/mydb".parse()?;
//!
//! let options = options.connect_layer(middleware::connect_fn(
//!     |request: ConnectRequest<Postgres>, next: ConnectNext<Postgres>| async move {
//!         let info = request.connection_info();
//!         println!("connecting to {:?}", info.server_address);
//!
//!         next.run(request).await
//!     },
//! ));
//!
//! let conn = PgConnection::connect_with(&options).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Unlike the middleware stack of a connection, there is one instance of the connect middleware
//! stack, shared by all clones of the connect options (e.g. all connections of a pool), so
//! stateful middleware applies to all attempts to connect. See [`ConnectLayers`].
//!
//! The statements that initialize a new connection (e.g. MySQL's `SET` statement and SQLite's
//! `PRAGMA`s) are part of the connect request; they don't pass through the layers added with
//! [`ConnectOptions::layer()`].
//!
//! # Errors
//! Middleware may return any error type that converts into a boxed error (like `tower::BoxError`).
//! Errors are converted back into [`Error`]: if the error is an [`Error`], it's passed through
//! unchanged, otherwise it's wrapped in [`Error::Middleware`]. For example, `tower`'s timeout
//! returns `Error::Middleware` wrapping `tower::timeout::error::Elapsed`.
//!
//! # Notes on Tower Middleware
//! * Middleware services are type-erased with [`BoxService`] (and connect middleware with
//!   [`BoxConnectService`]), which is `Clone`: its clones share the same instance of the
//!   middleware, so middleware which needs to clone its inner service (like
//!   `tower::retry::Retry`) can be used anywhere.
//! * A layer added with `ConnectOptions::layer()` is applied to each connection separately, so
//!   `tower::limit::ConcurrencyLimitLayer` limits concurrency per connection (which is always 1).
//!   Add the layer to the pool to limit concurrency across connections (or use
//!   `tower::limit::GlobalConcurrencyLimitLayer`, which doesn't limit queries waiting for a
//!   connection).
//! * Some middleware in the `tower` crate (e.g. `timeout`, `limit::rate` and `buffer`) requires
//!   a Tokio runtime and panics otherwise.
//! * Retrying requests inside a transaction is rarely correct; see [Transactions](#transactions).
//!
//! # What Doesn't Pass Through Middleware
//! * The statements used to initialize connections (e.g. MySQL's `SET` statement and SQLite's
//!   `PRAGMA`s), which are part of the [connect request](#connecting) instead, and pings
//!   (e.g. `PoolOptions::test_before_acquire`).
//! * Queries the driver makes on its own to handle a request, e.g. resolving custom types in
//!   Postgres. These are part of the request that triggered them.
//! * `COPY` in Postgres, and notifications received by `PgListener` (statements like `LISTEN`
//!   do pass through middleware).
//! * The `ROLLBACK` queued when a [`Transaction`] is dropped without being committed or rolled
//!   back, which is sent with the next request (or when the connection is returned to the pool).
//!
//! # Example: Implementing [`Layer`] and [`Service`]
//! ```rust,no_run
//! use std::task::{Context, Poll};
//! use futures_util::future::BoxFuture;
//! use sqlx::Database;
//! use sqlx::middleware::{Layer, Request, Response, Service};
//!
//! #[derive(Clone)]
//! struct LogLayer;
//!
//! impl<S> Layer<S> for LogLayer {
//!     type Service = Log<S>;
//!
//!     fn layer(&self, inner: S) -> Log<S> {
//!         Log { inner }
//!     }
//! }
//!
//! #[derive(Clone)]
//! struct Log<S> {
//!     inner: S,
//! }
//!
//! impl<DB, S> Service<Request<DB>> for Log<S>
//! where
//!     DB: Database,
//!     S: Service<Request<DB>, Response = Response<DB>, Error = sqlx::Error>,
//!     S::Future: Send + 'static,
//! {
//!     type Response = Response<DB>;
//!     type Error = sqlx::Error;
//!     type Future = BoxFuture<'static, sqlx::Result<Response<DB>>>;
//!
//!     fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<sqlx::Result<()>> {
//!         self.inner.poll_ready(cx)
//!     }
//!
//!     fn call(&mut self, request: Request<DB>) -> Self::Future {
//!         println!("executing {}: {}", request.kind().as_str(), request.sql().as_str());
//!         let response = self.inner.call(request);
//!         Box::pin(response)
//!     }
//! }
//!
//! # async fn example() -> sqlx::Result<()> {
//! use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
//!
//! let pool = PgPoolOptions::new()
//!     .connect_with(
//!         "postgres://localhost/mydb"
//!             .parse::<PgConnectOptions>()?
//!             .layer(LogLayer),
//!     )
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! [Tower]: https://docs.rs/tower
//! [`ConnectOptions::layer()`]: crate::connection::ConnectOptions::layer
//! [`ConnectOptions::connect_layer()`]: crate::connection::ConnectOptions::connect_layer
//! [`PoolOptions::layer()`]: crate::pool::PoolOptions::layer
//! [`Pool`]: crate::pool::Pool
//! [`PoolConnection`]: crate::pool::PoolConnection
//! [`Transaction`]: crate::transaction::Transaction
//! [`Executor`]: crate::executor::Executor
//! [`Error`]: crate::error::Error
//! [`Error::Middleware`]: crate::error::Error::Middleware

mod boxed;
mod connect;
mod executor;
mod extensions;
mod from_fn;
mod info;
mod request;
mod response;
pub mod trace;

#[doc(hidden)]
pub use boxed::ConnectionService;
pub use boxed::{into_sqlx_error, BoxConnectService, BoxService, Layers};
pub use connect::{
    connect_fn, ConnectFn, ConnectFnLayer, ConnectLayers, ConnectNext, ConnectRequest,
};
pub use executor::ServiceExecutor;
pub use extensions::Extensions;
pub use from_fn::{from_fn, FromFn, FromFnLayer, Next};
pub use info::ConnectionInfo;
pub use request::{Request, RequestKind, RequestParts};
pub use response::{Response, ResultItem, ResultStream};

pub use tower_layer::{layer_fn, Identity, Layer, LayerFn, Stack};
pub use tower_service::Service;
