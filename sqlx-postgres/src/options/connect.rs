use crate::connection::ConnectOptions;
use crate::error::Error;
use crate::{PgConnectOptions, PgConnection, Postgres};
use log::LevelFilter;
use sqlx_core::middleware::{BoxService, ConnectLayers, ConnectionInfo, Layers};
use sqlx_core::Url;
use std::time::Duration;

impl ConnectOptions for PgConnectOptions {
    type Connection = PgConnection;

    fn from_url(url: &Url) -> Result<Self, Error> {
        Self::parse_from_url(url)
    }

    fn to_url_lossy(&self) -> Url {
        self.build_url()
    }

    async fn connect(&self) -> Result<Self::Connection, Error>
    where
        Self::Connection: Sized,
    {
        if self.connect_layers.is_empty() {
            PgConnection::establish(self).await
        } else {
            self.connect_layers
                .connect(self, |options| {
                    Box::pin(async move { PgConnection::establish(&options).await })
                })
                .await
        }
    }

    fn log_statements(mut self, level: LevelFilter) -> Self {
        self.log_settings.log_statements(level);
        self
    }

    fn log_slow_statements(mut self, level: LevelFilter, duration: Duration) -> Self {
        self.log_settings.log_slow_statements(level, duration);
        self
    }

    fn layers(&self) -> &Layers<BoxService<Postgres>> {
        &self.layers
    }

    fn layers_mut(&mut self) -> &mut Layers<BoxService<Postgres>> {
        &mut self.layers
    }

    fn connect_layers(&self) -> &ConnectLayers<Postgres> {
        &self.connect_layers
    }

    fn connect_layers_mut(&mut self) -> &mut ConnectLayers<Postgres> {
        &mut self.connect_layers
    }

    fn connection_info(&self) -> ConnectionInfo {
        let mut info = ConnectionInfo::new().with_user(&self.username);

        if let Some(database) = &self.database {
            info = info.with_database(database);
        }

        match self.fetch_socket() {
            Some(socket) => info.with_server_address(socket),
            None => info
                .with_server_address(&self.host)
                .with_server_port(self.port),
        }
    }
}
