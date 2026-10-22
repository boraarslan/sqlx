use std::fmt::{self, Debug, Formatter};
use std::future::{poll_fn, Future};
use std::marker::PhantomData;
use std::task::{Context, Poll};

use crate::database::Database;
use crate::error::{BoxDynError, Error};
use crate::middleware::{BoxService, Layer, Request, Response, Service};

/// Create middleware from an async function or closure.
///
/// The function receives each [`Request`] and a [`Next`], which sends the request
/// on to the rest of the middleware stack. It may inspect or modify the request, decide not to
/// pass it on, or inspect or modify the [`Response`].
///
/// This is often simpler than implementing [`Layer`] and [`Service`] by hand.
///
/// # Example
/// ```rust,no_run
/// use sqlx::middleware::{self, Next, Request};
/// use sqlx::postgres::{PgConnectOptions, Postgres};
///
/// # fn example() -> sqlx::Result<()> {
/// let options: PgConnectOptions = "postgres://localhost/mydb".parse()?;
///
/// let options = options.layer(middleware::from_fn(
///     |request: Request<Postgres>, next: Next<Postgres>| async move {
///         let sql = request.sql().clone();
///         let start = std::time::Instant::now();
///
///         let response = next.run(request).await;
///
///         println!("{} took {:?} to start responding", sql.as_str(), start.elapsed());
///         response
///     },
/// ));
/// # Ok(())
/// # }
/// ```
///
/// # Note: Readiness
/// The middleware itself is always ready; the readiness of the rest of the stack is awaited
/// by [`Next::run()`]. Middleware that provides backpressure, like a concurrency limit,
/// should therefore be added *before* (outside of) middleware created with this function to
/// limit requests before they are passed to the function.
pub fn from_fn<F, DB>(f: F) -> FromFnLayer<F, DB>
where
    DB: Database,
{
    FromFnLayer {
        f,
        _db: PhantomData,
    }
}

/// A [`Layer`] created by [`from_fn()`].
pub struct FromFnLayer<F, DB> {
    f: F,
    _db: PhantomData<fn() -> DB>,
}

impl<F: Clone, DB> Clone for FromFnLayer<F, DB> {
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            _db: PhantomData,
        }
    }
}

impl<F, DB> Debug for FromFnLayer<F, DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FromFnLayer").finish_non_exhaustive()
    }
}

impl<S, F, DB> Layer<S> for FromFnLayer<F, DB>
where
    DB: Database,
    F: Clone,
    S: Service<Request<DB>, Response = Response<DB>> + Send + 'static,
    S::Error: Into<BoxDynError> + 'static,
    S::Future: Send + 'static,
{
    type Service = FromFn<F, DB>;

    fn layer(&self, inner: S) -> Self::Service {
        FromFn {
            f: self.f.clone(),
            inner: BoxService::new(inner),
        }
    }
}

/// The middleware [`Service`] created by [`FromFnLayer`].
pub struct FromFn<F, DB: Database> {
    f: F,
    inner: BoxService<DB>,
}

impl<F: Clone, DB: Database> Clone for FromFn<F, DB> {
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<F, DB: Database> Debug for FromFn<F, DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FromFn").finish_non_exhaustive()
    }
}

impl<F, Fut, DB> Service<Request<DB>> for FromFn<F, DB>
where
    DB: Database,
    F: FnMut(Request<DB>, Next<DB>) -> Fut,
    Fut: Future<Output = Result<Response<DB>, Error>>,
{
    type Response = Response<DB>;
    type Error = Error;
    type Future = Fut;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // The readiness of the inner service is checked in `Next::run()`.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<DB>) -> Self::Future {
        let next = Next {
            inner: self.inner.clone(),
        };

        (self.f)(request, next)
    }
}

/// The rest of the middleware stack, passed to the function given to [`from_fn()`].
pub struct Next<DB: Database> {
    inner: BoxService<DB>,
}

impl<DB: Database> Next<DB> {
    /// Send `request` to the rest of the middleware stack, and wait for its response.
    ///
    /// `Next` can be cloned to send more than one request, for example to execute another
    /// statement on the same connection before the request. Only one request is sent at a time,
    /// and a [`ResultStream`][super::ResultStream] must be exhausted or dropped before another
    /// request can be executed on the same connection.
    pub async fn run(mut self, request: Request<DB>) -> Result<Response<DB>, Error> {
        poll_fn(|cx| self.inner.poll_ready(cx)).await?;
        self.inner.call(request).await
    }
}

impl<DB: Database> Clone for Next<DB> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<DB: Database> Debug for Next<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Next").finish_non_exhaustive()
    }
}
