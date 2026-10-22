/// Information about the database a [`Request`][super::Request] is sent to, from the connect
/// options of the connection or pool.
///
/// Intended for middleware such as tracing, e.g. to populate the OpenTelemetry attributes
/// `db.namespace`, `server.address` and `server.port`.
///
/// This never contains credentials.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectionInfo {
    /// The name of the database, if known. For SQLite, the path of the database file.
    pub database: Option<String>,
    /// The host name or IP address of the server, or the path of its Unix domain socket.
    pub server_address: Option<String>,
    /// The port of the server, if connecting over TCP.
    pub server_port: Option<u16>,
    /// The name of the database user, if known.
    pub user: Option<String>,
}

impl ConnectionInfo {
    /// Create an empty `ConnectionInfo`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the name of the database.
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Set the host name, IP address or Unix domain socket path of the server.
    pub fn with_server_address(mut self, server_address: impl Into<String>) -> Self {
        self.server_address = Some(server_address.into());
        self
    }

    /// Set the port of the server.
    pub fn with_server_port(mut self, server_port: u16) -> Self {
        self.server_port = Some(server_port);
        self
    }

    /// Set the name of the database user.
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }
}
