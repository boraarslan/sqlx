use std::fmt::{self, Debug, Formatter};
use std::future::{poll_fn, Future};
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use futures_core::future::BoxFuture;

use crate::connection::{ConnectOptions, Connection};
use crate::database::Database;
use crate::error::{BoxDynError, Error};
use crate::middleware::{BoxConnectService, ConnectionInfo, Layer, Layers, Service};

/// The connect options of `DB`.
type Options<DB> = <<DB as Database>::Connection as Connection>::Options;

/// A request to open a connection, sent through the layers added with
/// [`ConnectOptions::connect_layer()`]. The response is the new connection.
///
/// Middleware may change the options used to connect, for example to set a password which is
/// only valid for a short time, like an AWS RDS IAM authentication token, or to pick one of
/// several servers.
///
/// The `Debug` output of a request only contains its [`ConnectionInfo`], which never contains
/// credentials.
pub struct ConnectRequest<DB: Database> {
    options: Options<DB>,
}

impl<DB: Database> ConnectRequest<DB> {
    /// Create a request to connect with `options`.
    pub fn new(options: Options<DB>) -> Self {
        Self { options }
    }

    /// The options that will be used to connect.
    pub fn options(&self) -> &Options<DB> {
        &self.options
    }

    /// A mutable reference to the options that will be used to connect.
    pub fn options_mut(&mut self) -> &mut Options<DB> {
        &mut self.options
    }

    /// Change the options that will be used to connect.
    ///
    /// Useful with the builder methods of connect options, which take `self` by value:
    ///
    /// ```rust,no_run
    /// # fn example(request: sqlx::middleware::ConnectRequest<sqlx::Postgres>) {
    /// let request = request.map_options(|options| options.password("hunter2"));
    /// # }
    /// ```
    pub fn map_options(self, f: impl FnOnce(Options<DB>) -> Options<DB>) -> Self {
        Self {
            options: f(self.options),
        }
    }

    /// Unwrap the options that would have been used to connect.
    pub fn into_options(self) -> Options<DB> {
        self.options
    }

    /// Information about the database this request connects to.
    ///
    /// This never contains credentials.
    pub fn connection_info(&self) -> ConnectionInfo {
        self.options.connection_info()
    }
}

impl<DB: Database> Clone for ConnectRequest<DB> {
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
        }
    }
}

impl<DB: Database> Debug for ConnectRequest<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectRequest")
            .field("connection_info", &self.connection_info())
            .finish_non_exhaustive()
    }
}

/// The layers added with [`ConnectOptions::connect_layer()`].
///
/// Unlike the layers added with [`ConnectOptions::layer()`], which are applied to every
/// connection separately, there is one instance of the connect middleware stack, shared by all
/// clones of the connect options (e.g. all connections opened by a pool). This allows stateful
/// middleware (like a rate limit or a circuit breaker) to apply to all attempts to connect.
/// The stack is created when it's first used; adding a layer creates a new stack.
///
/// Layers are applied like `tower::ServiceBuilder`: the first layer added is the outermost
/// and sees each request first.
pub struct ConnectLayers<DB: Database> {
    layers: Layers<BoxConnectService<DB>>,
    stack: Arc<OnceLock<BoxConnectService<DB>>>,
}

/// Opens a connection with the given options, without going through connect middleware.
type Establish<DB> =
    fn(Options<DB>) -> BoxFuture<'static, Result<<DB as Database>::Connection, Error>>;

impl<DB: Database> ConnectLayers<DB> {
    /// An empty list of layers.
    pub fn new() -> Self {
        Self {
            layers: Layers::new(),
            stack: Arc::new(OnceLock::new()),
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

    /// Add a [`Layer`], which will be applied inside all previously added layers.
    pub fn push<L>(&mut self, layer: L)
    where
        L: Layer<BoxConnectService<DB>> + Send + Sync + 'static,
        L::Service: Service<ConnectRequest<DB>, Response = DB::Connection> + Send + 'static,
        <L::Service as Service<ConnectRequest<DB>>>::Error: Into<BoxDynError> + 'static,
        <L::Service as Service<ConnectRequest<DB>>>::Future: Send + 'static,
    {
        self.layers
            .push_fn(move |inner| BoxConnectService::new(layer.layer(inner)));

        // Clones made before this keep using the stack without the new layer.
        self.stack = Arc::new(OnceLock::new());
    }

    /// Connect with `options` through the middleware stack, at the bottom of which `establish`
    /// opens the connection.
    ///
    /// For use by database drivers in [`ConnectOptions::connect()`], if there are any layers.
    #[doc(hidden)]
    pub fn connect(
        &self,
        options: &Options<DB>,
        establish: Establish<DB>,
    ) -> impl Future<Output = Result<DB::Connection, Error>> + Send + 'static {
        let mut stack = self
            .stack
            .get_or_init(|| {
                self.layers
                    .apply(BoxConnectService::new(Driver { establish }))
            })
            .clone();

        let request = ConnectRequest::new(options.clone());

        async move {
            poll_fn(|cx| stack.poll_ready(cx)).await?;
            stack.call(request).await
        }
    }
}

impl<DB: Database> Default for ConnectLayers<DB> {
    fn default() -> Self {
        Self::new()
    }
}

impl<DB: Database> Clone for ConnectLayers<DB> {
    fn clone(&self) -> Self {
        Self {
            layers: self.layers.clone(),
            stack: self.stack.clone(),
        }
    }
}

impl<DB: Database> Debug for ConnectLayers<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectLayers")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

/// The innermost service of a connect middleware stack, which opens the connection.
struct Driver<DB: Database> {
    establish: Establish<DB>,
}

impl<DB: Database> Service<ConnectRequest<DB>> for Driver<DB> {
    type Response = DB::Connection;
    type Error = Error;
    type Future = BoxFuture<'static, Result<DB::Connection, Error>>;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: ConnectRequest<DB>) -> Self::Future {
        (self.establish)(request.into_options())
    }
}

/// Create a connect middleware [`Layer`] from an async function or closure.
///
/// The function receives each [`ConnectRequest`] and a [`ConnectNext`] to pass it on with
/// [`ConnectNext::run()`]. It can change the request, return an error instead, or wrap
/// the call, e.g. to retry it.
///
/// ```rust,no_run
/// use sqlx::middleware::{self, ConnectNext, ConnectRequest};
/// use sqlx::postgres::{PgConnectOptions, PgPoolOptions, Postgres};
///
/// async fn fetch_auth_token() -> sqlx::Result<String> {
///     // e.g. generate an AWS RDS IAM authentication token
/// #   unimplemented!()
/// }
///
/// # async fn example() -> sqlx::Result<()> {
/// let options: PgConnectOptions = "postgres://app@mydb.example.com/app".parse()?;
///
/// // Every connection opened by the pool uses a fresh token as its password.
/// let pool = PgPoolOptions::new()
///     .connect_with(options.connect_layer(middleware::connect_fn(
///         |request: ConnectRequest<Postgres>, next: ConnectNext<Postgres>| async move {
///             let token = fetch_auth_token().await?;
///
///             next.run(request.map_options(|options| options.password(&token))).await
///         },
///     )))
///     .await?;
/// # Ok(())
/// # }
/// ```
pub fn connect_fn<F, DB>(f: F) -> ConnectFnLayer<F, DB>
where
    DB: Database,
{
    ConnectFnLayer {
        f,
        _db: PhantomData,
    }
}

/// The [`Layer`] created by [`connect_fn()`].
pub struct ConnectFnLayer<F, DB> {
    f: F,
    _db: PhantomData<fn() -> DB>,
}

impl<F: Clone, DB> Clone for ConnectFnLayer<F, DB> {
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            _db: PhantomData,
        }
    }
}

impl<F, DB> Debug for ConnectFnLayer<F, DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectFnLayer").finish_non_exhaustive()
    }
}

impl<S, F, DB> Layer<S> for ConnectFnLayer<F, DB>
where
    DB: Database,
    F: Clone,
    S: Service<ConnectRequest<DB>, Response = DB::Connection> + Send + 'static,
    S::Error: Into<BoxDynError> + 'static,
    S::Future: Send + 'static,
{
    type Service = ConnectFn<F, DB>;

    fn layer(&self, inner: S) -> Self::Service {
        ConnectFn {
            f: self.f.clone(),
            inner: BoxConnectService::new(inner),
        }
    }
}

/// The [`Service`] created by [`ConnectFnLayer`].
pub struct ConnectFn<F, DB: Database> {
    f: F,
    inner: BoxConnectService<DB>,
}

impl<F: Clone, DB: Database> Clone for ConnectFn<F, DB> {
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<F, DB: Database> Debug for ConnectFn<F, DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectFn").finish_non_exhaustive()
    }
}

impl<F, Fut, DB> Service<ConnectRequest<DB>> for ConnectFn<F, DB>
where
    DB: Database,
    F: FnMut(ConnectRequest<DB>, ConnectNext<DB>) -> Fut,
    Fut: Future<Output = Result<DB::Connection, Error>>,
{
    type Response = DB::Connection;
    type Error = Error;
    type Future = Fut;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // The readiness of the inner service is checked in `ConnectNext::run()`.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: ConnectRequest<DB>) -> Self::Future {
        let next = ConnectNext {
            inner: self.inner.clone(),
        };

        (self.f)(request, next)
    }
}

/// The rest of the connect middleware stack, passed to the function given to [`connect_fn()`].
pub struct ConnectNext<DB: Database> {
    inner: BoxConnectService<DB>,
}

impl<DB: Database> ConnectNext<DB> {
    /// Send `request` to the rest of the stack and return the new connection.
    ///
    /// `ConnectNext` is `Clone`, so a request can be sent more than once, e.g. to retry it.
    pub async fn run(mut self, request: ConnectRequest<DB>) -> Result<DB::Connection, Error> {
        poll_fn(|cx| self.inner.poll_ready(cx)).await?;
        self.inner.call(request).await
    }
}

impl<DB: Database> Clone for ConnectNext<DB> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<DB: Database> Debug for ConnectNext<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectNext").finish_non_exhaustive()
    }
}
