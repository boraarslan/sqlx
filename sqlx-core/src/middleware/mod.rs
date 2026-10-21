//! Middleware support using [Tower].
//!
//! Every statement SQLx sends to the database on behalf of the user (queries, statement
//! preparation and transaction control) is represented as a [`Request`] and sent through a
//! stack of [`Service`]s, which answers with a [`Response`].
//! Any [`Layer`] that works with generic Tower services can be added to this stack,
//! including those in the `tower` crate (timeouts, concurrency and rate limits, retries, ...).
//!
//! Middleware can be added in three places:
//!
//! * **Connections**: [`ConnectOptions::layer()`] (and the inherent `layer()` methods of each
//!   driver's connect options) adds a layer to every connection made with those options,
//!   including connections opened by a [`Pool`]. All statements executed on such a connection
//!   pass through the layer, whether they come from `&mut Connection`, a [`PoolConnection`], a
//!   [`Transaction`] or a `Pool` executor. Each connection gets its own instance of the stack.
//! * **Pools**: [`PoolOptions::layer()`] adds a layer that sees queries executed directly on a
//!   `&Pool`, *before* a connection is acquired. The time spent waiting for a connection is part
//!   of the request. The stack is shared and cloned per request, so its services must be `Clone`.
//! * **Ad-hoc executors**: connections, transactions and pools implement
//!   `Service<Request<DB>>`, so any of them can be wrapped in layers, e.g. with
//!   `tower::ServiceBuilder`, and turned back into an [`Executor`] with [`ServiceExecutor`].
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
//! The future returned by a service completes once the database has started responding
//! (the first row or result was received, or an error). The rest of the rows
//! are streamed through the [`ResultStream`], which is `'static` and keeps the connection reserved
//! until it is exhausted or dropped. Middleware that needs to observe the whole execution of a
//! query, such as tracing, should wrap the stream with [`Response::map_stream()`].
//!
//! # Errors
//! Middleware may return any error type that converts into a boxed error (like `tower::BoxError`).
//! Errors are converted back into [`Error`]: if the error is an [`Error`], it's passed through
//! unchanged, otherwise it's wrapped in [`Error::Middleware`].
//!
//! # Example: Tracing
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
//! [`PoolOptions::layer()`]: crate::pool::PoolOptions::layer
//! [`Pool`]: crate::pool::Pool
//! [`PoolConnection`]: crate::pool::PoolConnection
//! [`Transaction`]: crate::transaction::Transaction
//! [`Executor`]: crate::executor::Executor
//! [`Error`]: crate::error::Error
//! [`Error::Middleware`]: crate::error::Error::Middleware

mod boxed;
mod executor;
mod extensions;
mod from_fn;
mod info;
mod request;
mod response;

#[doc(hidden)]
pub use boxed::ConnectionService;
pub use boxed::{into_sqlx_error, BoxService, Layers};
pub use executor::ServiceExecutor;
pub use extensions::Extensions;
pub use from_fn::{from_fn, FromFn, FromFnLayer, Next};
pub use info::ConnectionInfo;
pub use request::{Request, RequestKind, RequestParts};
pub use response::{Response, ResultItem, ResultStream};

pub use tower_layer::{layer_fn, Identity, Layer, LayerFn, Stack};
pub use tower_service::Service;
