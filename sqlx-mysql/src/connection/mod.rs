use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use async_lock::{Mutex, MutexGuardArc};
use futures_core::future::BoxFuture;
pub(crate) use service::MySqlDriver;
pub(crate) use sqlx_core::connection::*;
use sqlx_core::middleware::{
    BoxService, ConnectionInfo, ConnectionService, Layers, Request, Response, Service,
};
use sqlx_core::sql_str::{SqlSafeStr, SqlStr};
use sqlx_core::transaction::rollback_ansi_transaction_sql;
pub(crate) use stream::{MySqlStream, Waiting};

use crate::collation::Collation;
use crate::common::StatementCache;
use crate::error::Error;
use crate::protocol::response::Status;
use crate::protocol::statement::StmtClose;
use crate::protocol::text::{Ping, Query, Quit};
use crate::statement::MySqlStatementMetadata;
use crate::transaction::Transaction;
use crate::{MySql, MySqlConnectOptions};

mod auth;
mod establish;
mod executor;
mod service;
mod stream;
mod tls;

const MAX_PACKET_SIZE: u32 = 1024;

/// The charset parameter sent in the `Protocol::HandshakeResponse41` packet.
///
/// This becomes the default if `set_names = false`,
/// and also ensures that any error messages returned before `SET NAMES` are encoded correctly.
#[allow(clippy::cast_possible_truncation)]
const INITIAL_CHARSET: u8 = Collation::UTF8MB4_GENERAL_CI.0 as u8;

/// A connection to a MySQL database.
///
/// Statements are sent through the connection's [middleware] stack, configured with
/// [`MySqlConnectOptions::layer()`]. The connection itself implements
/// `Service<Request<MySql>>`, see [`sqlx_core::middleware`].
///
/// [middleware]: sqlx_core::middleware
pub struct MySqlConnection {
    /// The state of the connection, shared with the driver service at the bottom of the
    /// middleware stack so that it can return `'static` responses.
    pub(crate) conn: Arc<Mutex<MySqlConn>>,
    pub(crate) state: Arc<MySqlConnectionState>,
    pub(crate) service: ConnectionService<MySql, MySqlDriver>,
}

/// State of a [`MySqlConnection`] that is accessed without locking [`MySqlConn`].
pub(crate) struct MySqlConnectionState {
    pub(crate) transaction_depth: AtomicUsize,

    /// `ROLLBACK` statements queued by `start_rollback()` while the connection was locked,
    /// which happens if middleware keeps a response stream alive after its request completed.
    ///
    /// These are queued on the connection the next time it is locked.
    pending_rollbacks: std::sync::Mutex<Vec<SqlStr>>,
}

/// The protocol state of a [`MySqlConnection`]: the stream to the server and its caches.
///
/// This is only accessed while locked, which is uncontended unless middleware keeps a
/// response alive after the request that produced it completed.
pub(crate) struct MySqlConn {
    pub(crate) inner: MySqlConnectionInner,
}

pub(crate) struct MySqlConnectionInner {
    // underlying TCP stream,
    // wrapped in a potentially TLS stream,
    // wrapped in a buffered stream
    pub(crate) stream: MySqlStream,

    // transaction status
    status_flags: Status,

    // cache by query string to the statement id and metadata
    cache_statement: StatementCache<(u32, MySqlStatementMetadata)>,

    log_settings: LogSettings,
}

impl MySqlConnection {
    pub(crate) fn new(conn: MySqlConn, info: Arc<ConnectionInfo>) -> Self {
        let state = Arc::new(MySqlConnectionState {
            transaction_depth: AtomicUsize::new(0),
            pending_rollbacks: Default::default(),
        });

        let conn = Arc::new(Mutex::new(conn));

        let driver = MySqlDriver {
            conn: conn.clone(),
            state: state.clone(),
        };

        // Middleware is installed once the connection is fully initialized.
        // See `install_layers()`.
        Self {
            conn,
            state,
            service: ConnectionService::new(driver, &Layers::new(), info),
        }
    }

    /// Wrap the connection in the middleware layers from its options.
    pub(crate) fn install_layers(&mut self, layers: &Layers<BoxService<MySql>>) {
        let driver = MySqlDriver {
            conn: self.conn.clone(),
            state: self.state.clone(),
        };

        self.service = ConnectionService::new(driver, layers, self.service.info().clone());
    }

    /// Lock the protocol state of the connection, bypassing middleware.
    pub(crate) fn lock(&self) -> impl Future<Output = MutexGuardArc<MySqlConn>> + Send + 'static {
        lock(self.conn.clone(), self.state.clone())
    }

    /// Lock the protocol state of the connection if it's not in use.
    pub(crate) fn try_lock(&self) -> Option<MutexGuardArc<MySqlConn>> {
        let mut conn = self.conn.try_lock_arc()?;
        self.state.queue_pending_rollbacks(&mut conn);
        Some(conn)
    }

    pub(crate) fn transaction_depth(&self) -> usize {
        self.state.transaction_depth()
    }
}

/// Lock the protocol state of a connection.
pub(crate) async fn lock(
    conn: Arc<Mutex<MySqlConn>>,
    state: Arc<MySqlConnectionState>,
) -> MutexGuardArc<MySqlConn> {
    let mut conn = conn.lock_arc().await;
    state.queue_pending_rollbacks(&mut conn);
    conn
}

impl MySqlConnectionState {
    pub(crate) fn transaction_depth(&self) -> usize {
        self.transaction_depth.load(Ordering::Acquire)
    }

    pub(crate) fn set_transaction_depth(&self, depth: usize) {
        self.transaction_depth.store(depth, Ordering::Release);
    }

    /// Queue a `ROLLBACK` of the current transaction or savepoint, if any,
    /// without waiting for it.
    ///
    /// `conn` should be `None` if the connection is locked elsewhere.
    pub(crate) fn start_rollback(&self, conn: Option<&mut MySqlConn>) {
        let depth = self.transaction_depth();

        if depth == 0 {
            return;
        }

        let statement = rollback_ansi_transaction_sql(depth);

        match conn {
            Some(conn) => conn.queue_simple_query(statement.as_str()),
            None => self
                .pending_rollbacks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(statement),
        }

        self.set_transaction_depth(depth - 1);
    }

    fn queue_pending_rollbacks(&self, conn: &mut MySqlConn) {
        let mut pending = self
            .pending_rollbacks
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        for statement in pending.drain(..) {
            conn.queue_simple_query(statement.as_str());
        }
    }
}

impl MySqlConn {
    pub(crate) fn in_transaction(&self) -> bool {
        self.inner
            .status_flags
            .intersects(Status::SERVER_STATUS_IN_TRANS)
    }

    /// Queue a simple query (not prepared) to execute the next time this connection is used.
    pub(crate) fn queue_simple_query(&mut self, query: &str) {
        self.inner.stream.waiting.push_back(Waiting::Result);
        self.inner.stream.sequence_id = 0;
        self.inner
            .stream
            .write_packet(Query(query))
            .expect("BUG: unexpected error queueing query");
    }
}

impl Debug for MySqlConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MySqlConnection").finish()
    }
}

impl Debug for MySqlConn {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MySqlConn").finish_non_exhaustive()
    }
}

/// Sends requests through the middleware stack of this connection.
///
/// See the [`middleware`][sqlx_core::middleware] module for details.
impl Service<Request<MySql>> for MySqlConnection {
    type Response = Response<MySql>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Response<MySql>, Error>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    #[inline]
    fn call(&mut self, request: Request<MySql>) -> Self::Future {
        let depth = self.transaction_depth();
        self.service.call(request, depth)
    }
}

impl Connection for MySqlConnection {
    type Database = MySql;

    type Options = MySqlConnectOptions;

    async fn close(self) -> Result<(), Error> {
        let mut conn = self.lock().await;

        conn.inner.stream.send_packet(Quit).await?;
        conn.inner.stream.shutdown().await?;

        Ok(())
    }

    async fn close_hard(self) -> Result<(), Error> {
        self.lock().await.inner.stream.shutdown().await?;
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), Error> {
        let mut conn = self.lock().await;

        conn.inner.stream.wait_until_ready().await?;
        conn.inner.stream.send_packet(Ping).await?;
        conn.inner.stream.recv_ok().await?;

        Ok(())
    }

    #[doc(hidden)]
    async fn flush(&mut self) -> Result<(), Error> {
        self.lock().await.inner.stream.wait_until_ready().await
    }

    fn cached_statements_size(&self) -> usize {
        // The connection can only be locked by a response stream that middleware kept alive.
        self.try_lock()
            .map_or(0, |conn| conn.inner.cache_statement.len())
    }

    async fn clear_cached_statements(&mut self) -> Result<(), Error> {
        let mut conn = self.lock().await;

        while let Some((statement_id, _)) = conn.inner.cache_statement.remove_lru() {
            conn.inner
                .stream
                .send_packet(StmtClose {
                    statement: statement_id,
                })
                .await?;
        }

        Ok(())
    }

    #[doc(hidden)]
    fn should_flush(&self) -> bool {
        // If the connection is locked, we can't check, so assume a flush is needed.
        self.try_lock()
            .is_none_or(|conn| !conn.inner.stream.write_buffer().is_empty())
    }

    fn begin(
        &mut self,
    ) -> impl Future<Output = Result<Transaction<'_, Self::Database>, Error>> + Send + '_ {
        Transaction::begin(self, None)
    }

    fn begin_with(
        &mut self,
        statement: impl SqlSafeStr,
    ) -> impl Future<Output = Result<Transaction<'_, Self::Database>, Error>> + Send + '_
    where
        Self: Sized,
    {
        Transaction::begin(self, Some(statement.into_sql_str()))
    }

    fn shrink_buffers(&mut self) {
        if let Some(mut conn) = self.try_lock() {
            conn.inner.stream.shrink_buffers();
        }
    }
}
