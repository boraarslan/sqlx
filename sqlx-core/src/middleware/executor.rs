use std::fmt::{self, Debug, Formatter};
use std::future::poll_fn;
use std::marker::PhantomData;

use futures_core::future::BoxFuture;

use crate::database::Database;
use crate::error::{BoxDynError, Error};
use crate::executor::Executor;
use crate::middleware::{into_sqlx_error, Request, Response, Service};

/// Adapts any [`Service`] which handles [`Request`]s into an [`Executor`].
///
/// This allows applying middleware to a single executor at the call site,
/// for example with [`tower::ServiceBuilder`]:
///
/// ```rust,no_run
/// # async fn example(conn: &mut sqlx::PgConnection) -> sqlx::Result<()> {
/// use sqlx::middleware::ServiceExecutor;
/// use tower::ServiceBuilder;
/// # #[derive(Clone)] struct MyLayer;
/// # impl<S> tower::Layer<S> for MyLayer { type Service = S; fn layer(&self, s: S) -> S { s } }
///
/// // Connections, `PoolConnection`s, transactions and pools all implement `Service<Request<DB>>`.
/// let mut executor = ServiceExecutor::new(ServiceBuilder::new().layer(MyLayer).service(&mut *conn));
///
/// sqlx::query("SELECT 1").execute(&mut executor).await?;
/// # Ok(())
/// # }
/// ```
///
/// A single layer can also be applied directly with [`Layer::layer()`][super::Layer::layer]:
///
/// ```rust,no_run
/// # async fn example(conn: &mut sqlx::PgConnection) -> sqlx::Result<()> {
/// use sqlx::middleware::{trace::TraceLayer, Layer, ServiceExecutor};
///
/// let mut executor = ServiceExecutor::new(TraceLayer::new().layer(&mut *conn));
/// sqlx::query("SELECT 1").execute(&mut executor).await?;
/// # Ok(())
/// # }
/// ```
///
/// [`Executor`] is implemented for `&mut ServiceExecutor`, and also for `&ServiceExecutor`
/// if the service is `Clone + Sync`. In the latter case, the service is cloned for each request,
/// like [`Pool`][crate::pool::Pool].
///
/// [`tower::ServiceBuilder`]: https://docs.rs/tower/latest/tower/builder/struct.ServiceBuilder.html
pub struct ServiceExecutor<S, DB> {
    service: S,
    _db: PhantomData<fn() -> DB>,
}

impl<S, DB> ServiceExecutor<S, DB> {
    /// Wrap `service` in an executor.
    pub fn new(service: S) -> Self {
        Self {
            service,
            _db: PhantomData,
        }
    }

    /// A reference to the wrapped service.
    pub fn get_ref(&self) -> &S {
        &self.service
    }

    /// A mutable reference to the wrapped service.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.service
    }

    /// Unwrap the service.
    pub fn into_inner(self) -> S {
        self.service
    }
}

impl<S: Clone, DB> Clone for ServiceExecutor<S, DB> {
    fn clone(&self) -> Self {
        Self::new(self.service.clone())
    }
}

impl<S, DB> Debug for ServiceExecutor<S, DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceExecutor").finish_non_exhaustive()
    }
}

impl<'c, S, DB> Executor<'c> for &'c mut ServiceExecutor<S, DB>
where
    DB: Database,
    S: Service<Request<DB>, Response = Response<DB>> + Send,
    S::Error: Into<BoxDynError> + 'static,
    S::Future: Send + 'static,
{
    type Database = DB;

    fn dispatch<'e>(self, request: Request<DB>) -> BoxFuture<'e, Result<Response<DB>, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            poll_fn(|cx| self.service.poll_ready(cx))
                .await
                .map_err(into_sqlx_error)?;

            self.service.call(request).await.map_err(into_sqlx_error)
        })
    }
}

impl<'c, S, DB> Executor<'c> for &'c ServiceExecutor<S, DB>
where
    DB: Database,
    S: Service<Request<DB>, Response = Response<DB>> + Clone + Send + Sync,
    S::Error: Into<BoxDynError> + 'static,
    S::Future: Send + 'static,
{
    type Database = DB;

    fn dispatch<'e>(self, request: Request<DB>) -> BoxFuture<'e, Result<Response<DB>, Error>>
    where
        'c: 'e,
    {
        let mut service = self.service.clone();

        Box::pin(async move {
            poll_fn(|cx| service.poll_ready(cx))
                .await
                .map_err(into_sqlx_error)?;

            service.call(request).await.map_err(into_sqlx_error)
        })
    }
}
