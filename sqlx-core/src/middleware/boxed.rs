use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::future::poll_fn;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_core::future::BoxFuture;
use sync_wrapper::SyncWrapper;

use crate::database::Database;
use crate::error::{BoxDynError, Error};
use crate::middleware::{Layer, Request, Response, Service};

/// Convert an error returned by middleware into [`Error`].
///
/// If the error is (or boxes) an [`Error`], it's returned as-is so the original error
/// (e.g. [`Error::Database`]) propagates through any middleware unchanged.
/// Any other error is wrapped in [`Error::Middleware`].
pub fn into_sqlx_error<E>(error: E) -> Error
where
    E: Into<BoxDynError> + 'static,
{
    // Avoid boxing if the error is already the right type.
    let mut error = Some(error);

    if let Some(error) = (&mut error as &mut dyn Any).downcast_mut::<Option<Error>>() {
        return error.take().expect("BUG: error already taken");
    }

    let error: BoxDynError = error.take().expect("BUG: error already taken").into();

    match error.downcast::<Error>() {
        Ok(error) => *error,
        Err(error) => Error::Middleware(error),
    }
}

trait ErasedService<DB: Database>: Send + 'static {
    fn erased_poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>>;

    fn erased_call(
        &mut self,
        request: Request<DB>,
    ) -> BoxFuture<'static, Result<Response<DB>, Error>>;
}

impl<DB, S> ErasedService<DB> for S
where
    DB: Database,
    S: Service<Request<DB>, Response = Response<DB>> + Send + 'static,
    S::Error: Into<BoxDynError> + 'static,
    S::Future: Send + 'static,
{
    #[inline]
    fn erased_poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Service::poll_ready(self, cx).map_err(into_sqlx_error)
    }

    #[inline]
    fn erased_call(
        &mut self,
        request: Request<DB>,
    ) -> BoxFuture<'static, Result<Response<DB>, Error>> {
        let fut = Service::call(self, request);
        Box::pin(async move { fut.await.map_err(into_sqlx_error) })
    }
}

/// A type-erased middleware stack.
///
/// Used as the inner service of layers added with [`ConnectOptions::layer()`]
/// and as the type-erased result of applying them.
///
/// This is `Send + Sync`, but the wrapped service only needs to be `Send`.
///
/// [`ConnectOptions::layer()`]: crate::connection::ConnectOptions::layer
pub struct BoxService<DB: Database> {
    inner: SyncWrapper<Box<dyn ErasedService<DB>>>,
}

impl<DB: Database> BoxService<DB> {
    /// Erase the type of `service`.
    ///
    /// Errors returned by the service are converted with [`into_sqlx_error()`].
    pub fn new<S>(service: S) -> Self
    where
        S: Service<Request<DB>, Response = Response<DB>> + Send + 'static,
        S::Error: Into<BoxDynError> + 'static,
        S::Future: Send + 'static,
    {
        Self {
            inner: SyncWrapper::new(Box::new(service)),
        }
    }
}

impl<DB: Database> Service<Request<DB>> for BoxService<DB> {
    type Response = Response<DB>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Response<DB>, Error>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.get_mut().erased_poll_ready(cx)
    }

    #[inline]
    fn call(&mut self, request: Request<DB>) -> Self::Future {
        self.inner.get_mut().erased_call(request)
    }
}

impl<DB: Database> Debug for BoxService<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxService").finish_non_exhaustive()
    }
}

trait ErasedCloneService<DB: Database>: ErasedService<DB> + Sync {
    fn clone_box(&self) -> Box<dyn ErasedCloneService<DB>>;
}

impl<DB, S> ErasedCloneService<DB> for S
where
    DB: Database,
    S: Service<Request<DB>, Response = Response<DB>> + Clone + Send + Sync + 'static,
    S::Error: Into<BoxDynError> + 'static,
    S::Future: Send + 'static,
{
    fn clone_box(&self) -> Box<dyn ErasedCloneService<DB>> {
        Box::new(self.clone())
    }
}

/// A type-erased middleware stack which can be cloned and shared between threads.
///
/// Used as the inner service of layers added with [`PoolOptions::layer()`],
/// and as the type-erased result of applying them.
///
/// [`PoolOptions::layer()`]: crate::pool::PoolOptions::layer
pub struct BoxCloneService<DB: Database> {
    inner: Box<dyn ErasedCloneService<DB>>,
}

impl<DB: Database> BoxCloneService<DB> {
    /// Erase the type of `service`.
    ///
    /// Errors returned by the service are converted with [`into_sqlx_error()`].
    pub fn new<S>(service: S) -> Self
    where
        S: Service<Request<DB>, Response = Response<DB>> + Clone + Send + Sync + 'static,
        S::Error: Into<BoxDynError> + 'static,
        S::Future: Send + 'static,
    {
        Self {
            inner: Box::new(service),
        }
    }
}

impl<DB: Database> Clone for BoxCloneService<DB> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone_box(),
        }
    }
}

impl<DB: Database> Service<Request<DB>> for BoxCloneService<DB> {
    type Response = Response<DB>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Response<DB>, Error>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.erased_poll_ready(cx)
    }

    #[inline]
    fn call(&mut self, request: Request<DB>) -> Self::Future {
        self.inner.erased_call(request)
    }
}

impl<DB: Database> Debug for BoxCloneService<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxCloneService").finish_non_exhaustive()
    }
}

/// An ordered list of type-erased [`Layer`]s.
///
/// Layers are applied like [`tower::ServiceBuilder`]: the first layer added is the outermost
/// and sees each request first.
///
/// Cloning is cheap, as the list is reference-counted.
///
/// [`tower::ServiceBuilder`]: https://docs.rs/tower/latest/tower/builder/struct.ServiceBuilder.html
pub struct Layers<S> {
    layers: Arc<Vec<Arc<dyn Fn(S) -> S + Send + Sync>>>,
}

impl<S> Layers<S> {
    /// An empty list of layers.
    pub fn new() -> Self {
        Self {
            layers: Arc::new(Vec::new()),
        }
    }

    /// Returns `true` if no layers have been added.
    pub fn is_empty(&self) -> bool {
        self.layers.is_empty()
    }

    /// Returns the number of layers added.
    pub fn len(&self) -> usize {
        self.layers.len()
    }

    /// Add a layer as a function from the inner service to the wrapping service.
    ///
    /// It will be applied inside all previously added layers.
    pub fn push_fn<F>(&mut self, layer: F)
    where
        F: Fn(S) -> S + Send + Sync + 'static,
    {
        Arc::make_mut(&mut self.layers).push(Arc::new(layer));
    }

    /// Wrap `service` in all layers.
    pub fn apply(&self, service: S) -> S {
        self.layers
            .iter()
            .rev()
            .fold(service, |service, layer| layer(service))
    }
}

impl<DB: Database> Layers<BoxService<DB>> {
    /// Add a [`Layer`], which will be applied inside all previously added layers.
    pub fn push<L>(&mut self, layer: L)
    where
        L: Layer<BoxService<DB>> + Send + Sync + 'static,
        L::Service: Service<Request<DB>, Response = Response<DB>> + Send + 'static,
        <L::Service as Service<Request<DB>>>::Error: Into<BoxDynError> + 'static,
        <L::Service as Service<Request<DB>>>::Future: Send + 'static,
    {
        self.push_fn(move |inner| BoxService::new(layer.layer(inner)));
    }
}

impl<DB: Database> Layers<BoxCloneService<DB>> {
    /// Add a [`Layer`], which will be applied inside all previously added layers.
    pub fn push<L>(&mut self, layer: L)
    where
        L: Layer<BoxCloneService<DB>> + Send + Sync + 'static,
        L::Service: Service<Request<DB>, Response = Response<DB>> + Clone + Send + Sync + 'static,
        <L::Service as Service<Request<DB>>>::Error: Into<BoxDynError> + 'static,
        <L::Service as Service<Request<DB>>>::Future: Send + 'static,
    {
        self.push_fn(move |inner| BoxCloneService::new(layer.layer(inner)));
    }
}

impl<S> Default for Layers<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Clone for Layers<S> {
    fn clone(&self) -> Self {
        Self {
            layers: self.layers.clone(),
        }
    }
}

impl<S> Debug for Layers<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Layers")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

/// The middleware stack of a connection, for use by database drivers.
///
/// `D` is the driver's innermost service, which actually talks to the database.
/// If no layers were configured, requests are sent to it directly, without type erasure.
///
/// The driver service is expected to always be ready (its `poll_ready()` is not called).
#[doc(hidden)]
pub struct ConnectionService<DB: Database, D> {
    stack: Stack<DB, D>,
}

enum Stack<DB: Database, D> {
    Driver(D),
    Layered(BoxService<DB>),
}

impl<DB, D> ConnectionService<DB, D>
where
    DB: Database,
    D: Service<
            Request<DB>,
            Response = Response<DB>,
            Error = Error,
            Future = BoxFuture<'static, Result<Response<DB>, Error>>,
        > + Send
        + 'static,
{
    /// Wrap the driver service in `layers`.
    pub fn new(driver: D, layers: &Layers<BoxService<DB>>) -> Self {
        let stack = if layers.is_empty() {
            Stack::Driver(driver)
        } else {
            Stack::Layered(layers.apply(BoxService::new(driver)))
        };

        Self { stack }
    }

    /// Wait until the stack is ready, then send it `request`.
    pub fn dispatch(&mut self, request: Request<DB>) -> BoxFuture<'_, Result<Response<DB>, Error>> {
        match &mut self.stack {
            // The driver service is always ready, so we can skip the extra allocation.
            Stack::Driver(driver) => driver.call(request),
            Stack::Layered(service) => Box::pin(async move {
                poll_fn(|cx| service.poll_ready(cx)).await?;
                service.call(request).await
            }),
        }
    }

    /// Returns `true` if any layers were configured.
    pub fn is_layered(&self) -> bool {
        matches!(self.stack, Stack::Layered(_))
    }
}

impl<DB, D> Service<Request<DB>> for ConnectionService<DB, D>
where
    DB: Database,
    D: Service<
            Request<DB>,
            Response = Response<DB>,
            Error = Error,
            Future = BoxFuture<'static, Result<Response<DB>, Error>>,
        > + Send
        + 'static,
{
    type Response = Response<DB>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Response<DB>, Error>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.stack {
            Stack::Driver(driver) => driver.poll_ready(cx),
            Stack::Layered(service) => service.poll_ready(cx),
        }
    }

    #[inline]
    fn call(&mut self, request: Request<DB>) -> Self::Future {
        match &mut self.stack {
            Stack::Driver(driver) => driver.call(request),
            Stack::Layered(service) => service.call(request),
        }
    }
}

impl<DB: Database, D> Debug for ConnectionService<DB, D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionService")
            .field("layered", &matches!(self.stack, Stack::Layered(_)))
            .finish_non_exhaustive()
    }
}
