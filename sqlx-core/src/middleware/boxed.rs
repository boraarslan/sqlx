use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::future::{self, poll_fn, Future};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll, Waker};

use async_lock::futures::LockArc;
use async_lock::{Mutex, MutexGuardArc};
use futures_core::future::BoxFuture;
use sync_wrapper::SyncWrapper;

use crate::database::Database;
use crate::error::{BoxDynError, Error};
use crate::middleware::{ConnectionInfo, Layer, Request, Response, Service};

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

/// A type-erased middleware service.
///
/// Used as the inner service of layers added with [`ConnectOptions::layer()`] and
/// [`PoolOptions::layer()`], and as the type-erased result of applying them.
///
/// `BoxService` is `Clone + Send + Sync`, but the wrapped service only needs to be `Send`:
/// clones share the same instance of the wrapped service, so stateful middleware (like a rate
/// limit) applies to all clones. This allows using middleware which needs to clone its inner
/// service, like `tower::retry::Retry`.
///
/// The shared instance is locked from when [`poll_ready()`][Service::poll_ready] returns
/// `Ready` until [`call()`][Service::call] returns the response future. This lock is uncontended
/// unless clones are used concurrently.
///
/// [`ConnectOptions::layer()`]: crate::connection::ConnectOptions::layer
/// [`PoolOptions::layer()`]: crate::pool::PoolOptions::layer
pub struct BoxService<DB: Database> {
    shared: Arc<Mutex<Box<dyn ErasedService<DB>>>>,
    state: SyncWrapper<State<DB>>,
}

enum State<DB: Database> {
    Idle,
    Locking(Pin<Box<LockArc<Box<dyn ErasedService<DB>>>>>),
    Locked(MutexGuardArc<Box<dyn ErasedService<DB>>>),
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
            shared: Arc::new(Mutex::new(Box::new(service))),
            state: SyncWrapper::new(State::Idle),
        }
    }
}

impl<DB: Database> Clone for BoxService<DB> {
    /// Create another handle to the same service. The new handle is not ready.
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            state: SyncWrapper::new(State::Idle),
        }
    }
}

impl<DB: Database> Service<Request<DB>> for BoxService<DB> {
    type Response = Response<DB>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Response<DB>, Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let state = self.state.get_mut();

        loop {
            match state {
                State::Idle => {
                    *state = match self.shared.try_lock_arc() {
                        Some(guard) => State::Locked(guard),
                        None => State::Locking(Box::pin(self.shared.lock_arc())),
                    };
                }
                State::Locking(lock) => {
                    let guard = ready!(lock.as_mut().poll(cx));
                    *state = State::Locked(guard);
                }
                State::Locked(service) => {
                    let res = ready!(service.erased_poll_ready(cx));

                    if res.is_err() {
                        // Unlock the service.
                        *state = State::Idle;
                    }

                    return Poll::Ready(res);
                }
            }
        }
    }

    fn call(&mut self, request: Request<DB>) -> Self::Future {
        match std::mem::replace(self.state.get_mut(), State::Idle) {
            // The service is unlocked as soon as it returns the response future.
            State::Locked(mut service) => service.erased_call(request),
            _ => Box::pin(future::ready(Err(Error::Middleware(
                "BUG: `BoxService::call()` invoked before `poll_ready()` returned `Ready`".into(),
            )))),
        }
    }
}

impl<DB: Database> Debug for BoxService<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxService").finish_non_exhaustive()
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
/// The driver service is expected to be ready almost always: if it's not ready immediately,
/// requests sent with [`dispatch()`][Self::dispatch] need an extra allocation.
///
/// Before a request enters the middleware stack, its [`ConnectionInfo`] and transaction depth
/// are set.
#[doc(hidden)]
pub struct ConnectionService<DB: Database, D> {
    stack: Stack<DB, D>,
    info: Arc<ConnectionInfo>,
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
    pub fn new(driver: D, layers: &Layers<BoxService<DB>>, info: Arc<ConnectionInfo>) -> Self {
        let stack = if layers.is_empty() {
            Stack::Driver(driver)
        } else {
            Stack::Layered(layers.apply(BoxService::new(driver)))
        };

        Self { stack, info }
    }

    /// Wait until the stack is ready, then send it `request`.
    pub fn dispatch(
        &mut self,
        mut request: Request<DB>,
        transaction_depth: usize,
    ) -> BoxFuture<'_, Result<Response<DB>, Error>> {
        match &mut self.stack {
            // The driver service doesn't read the connection info or transaction depth.
            Stack::Driver(driver) => {
                // Driver services are usually always ready, which lets us skip the extra
                // allocation. A type-erased driver (like that of `AnyConnection`) may still
                // need `poll_ready()` to be called, however.
                match driver.poll_ready(&mut Context::from_waker(Waker::noop())) {
                    Poll::Ready(Ok(())) => driver.call(request),
                    Poll::Ready(Err(e)) => Box::pin(future::ready(Err(e))),
                    Poll::Pending => Box::pin(async move {
                        poll_fn(|cx| driver.poll_ready(cx)).await?;
                        driver.call(request).await
                    }),
                }
            }
            Stack::Layered(service) => {
                request.set_connection_info(self.info.clone());
                request.set_transaction_depth(transaction_depth);

                Box::pin(async move {
                    poll_fn(|cx| service.poll_ready(cx)).await?;
                    service.call(request).await
                })
            }
        }
    }

    /// See [`Service::poll_ready()`].
    #[inline]
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match &mut self.stack {
            Stack::Driver(driver) => driver.poll_ready(cx),
            Stack::Layered(service) => service.poll_ready(cx),
        }
    }

    /// See [`Service::call()`].
    ///
    /// Unlike [`dispatch()`][Self::dispatch], this always sets the connection info and
    /// transaction depth of the request, as the caller may be middleware which reads them.
    #[inline]
    pub fn call(
        &mut self,
        mut request: Request<DB>,
        transaction_depth: usize,
    ) -> BoxFuture<'static, Result<Response<DB>, Error>> {
        request.set_connection_info(self.info.clone());
        request.set_transaction_depth(transaction_depth);

        match &mut self.stack {
            Stack::Driver(driver) => driver.call(request),
            Stack::Layered(service) => service.call(request),
        }
    }

    /// The information about the database this connection is connected to.
    pub fn info(&self) -> &Arc<ConnectionInfo> {
        &self.info
    }

    /// Returns `true` if any layers were configured.
    pub fn is_layered(&self) -> bool {
        matches!(self.stack, Stack::Layered(_))
    }
}

impl<DB: Database, D> Debug for ConnectionService<DB, D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionService")
            .field("layered", &matches!(self.stack, Stack::Layered(_)))
            .field("info", &self.info)
            .finish_non_exhaustive()
    }
}
