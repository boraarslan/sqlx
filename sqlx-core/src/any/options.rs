use crate::any::{Any, AnyConnection};
use crate::connection::{ConnectOptions, LogSettings};
use crate::error::BoxDynError;
use crate::error::Error;
use crate::middleware::{BoxService, ConnectionInfo, Layer, Layers, Request, Response, Service};
use log::LevelFilter;
use std::future::Future;
use std::str::FromStr;
use std::time::Duration;
use url::Url;

/// Opaque options for connecting to a database. These may only be constructed by parsing from
/// a connection url.
///
/// ```text
/// postgres://postgres:password@localhost/database
/// mysql://root:password@localhost/database
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AnyConnectOptions {
    pub database_url: Url,
    pub log_settings: LogSettings,
    pub(crate) layers: Layers<BoxService<Any>>,
}
impl FromStr for AnyConnectOptions {
    type Err = Error;

    fn from_str(url: &str) -> Result<Self, Self::Err> {
        Ok(AnyConnectOptions {
            database_url: url
                .parse::<Url>()
                .map_err(|e| Error::Configuration(e.into()))?,
            log_settings: LogSettings::default(),
            layers: Layers::new(),
        })
    }
}

impl ConnectOptions for AnyConnectOptions {
    type Connection = AnyConnection;

    fn from_url(url: &Url) -> Result<Self, Error> {
        Ok(AnyConnectOptions {
            database_url: url.clone(),
            log_settings: LogSettings::default(),
            layers: Layers::new(),
        })
    }

    fn to_url_lossy(&self) -> Url {
        self.database_url.clone()
    }

    #[inline]
    fn connect(&self) -> impl Future<Output = Result<AnyConnection, Error>> + Send + '_ {
        AnyConnection::connect(self)
    }

    fn log_statements(mut self, level: LevelFilter) -> Self {
        self.log_settings.statements_level = level;
        self
    }

    fn log_slow_statements(mut self, level: LevelFilter, duration: Duration) -> Self {
        self.log_settings.slow_statements_level = level;
        self.log_settings.slow_statements_duration = duration;
        self
    }

    fn layers(&self) -> &Layers<BoxService<Any>> {
        &self.layers
    }

    fn layers_mut(&mut self) -> &mut Layers<BoxService<Any>> {
        &mut self.layers
    }

    fn connection_info(&self) -> ConnectionInfo {
        let url = &self.database_url;
        let mut info = ConnectionInfo::new();

        let database = url.path().trim_start_matches('/');

        if !database.is_empty() {
            info = info.with_database(database);
        }

        if let Some(host) = url.host_str().filter(|host| !host.is_empty()) {
            info = info.with_server_address(host);
        }

        if let Some(port) = url.port() {
            info = info.with_server_port(port);
        }

        if !url.username().is_empty() {
            info = info.with_user(url.username());
        }

        info
    }
}

impl AnyConnectOptions {
    /// Add a [Tower] middleware [`Layer`] to every connection made with these options.
    ///
    /// See [`ConnectOptions::layer()`] and the [`middleware`][crate::middleware] module for details.
    ///
    /// [Tower]: https://docs.rs/tower
    pub fn layer<L>(self, layer: L) -> Self
    where
        L: Layer<BoxService<Any>> + Send + Sync + 'static,
        L::Service: Service<Request<Any>, Response = Response<Any>> + Send + 'static,
        <L::Service as Service<Request<Any>>>::Error: Into<BoxDynError> + 'static,
        <L::Service as Service<Request<Any>>>::Future: Send + 'static,
    {
        ConnectOptions::layer(self, layer)
    }
}
