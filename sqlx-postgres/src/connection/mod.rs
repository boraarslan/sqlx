use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use async_lock::{Mutex, MutexGuardArc};
use futures_core::future::BoxFuture;
use sqlx_core::middleware::{
    BoxService, ConnectionInfo, ConnectionService, Layers, Request, Response, Service,
};
use sqlx_core::sql_str::SqlStr;
use sqlx_core::transaction::rollback_ansi_transaction_sql;

use crate::HashMap;

use crate::common::{PendingQueries, StatementCache};
use crate::error::Error;
use crate::ext::ustr::UStr;
use crate::io::StatementId;
use crate::message::{
    BackendMessageFormat, Close, Query, ReadyForQuery, ReceivedMessage, Terminate,
    TransactionStatus,
};
use crate::statement::PgStatementMetadata;
use crate::transaction::Transaction;
use crate::types::Oid;
use crate::{PgConnectOptions, PgTypeInfo, Postgres};

pub(crate) use sqlx_core::connection::*;
use sqlx_core::sql_str::SqlSafeStr;

pub use self::stream::PgStream;
pub(crate) use service::PgDriver;

#[cfg(feature = "offline")]
mod describe;
mod establish;
mod executor;
mod resolve;
mod sasl;
mod service;
mod stream;
mod tls;

/// A connection to a PostgreSQL database.
///
/// See [`PgConnectOptions`] for connection URL reference.
///
/// Statements are sent through the connection's [middleware] stack, configured with
/// [`PgConnectOptions::layer()`]. The connection itself implements
/// `Service<Request<Postgres>>`, see [`sqlx_core::middleware`].
///
/// [middleware]: sqlx_core::middleware
pub struct PgConnection {
    /// The state of the connection, shared with the driver service at the bottom of the
    /// middleware stack so that it can return `'static` responses.
    pub(crate) conn: Arc<Mutex<PgConn>>,
    pub(crate) state: Arc<PgConnectionState>,
    pub(crate) service: ConnectionService<Postgres, PgDriver>,
}

/// State of a [`PgConnection`] that is accessed without locking [`PgConn`].
pub(crate) struct PgConnectionState {
    pub(crate) transaction_depth: AtomicUsize,

    /// Simple queries (e.g. a `ROLLBACK` queued by `start_rollback()`) that were queued while
    /// the connection was locked, which happens if middleware keeps a response stream alive
    /// after its request completed.
    ///
    /// These are queued on the connection the next time it is locked.
    pending_queries: PendingQueries,

    server_version_num: Option<u32>,
}

/// The protocol state of a [`PgConnection`]: the stream to the server and its caches.
///
/// This is only accessed while locked, which is uncontended unless middleware keeps a
/// response alive after the request that produced it completed.
pub(crate) struct PgConn {
    pub(crate) inner: PgConnectionInner,
}

pub struct PgConnectionInner {
    // underlying TCP or UDS stream,
    // wrapped in a potentially TLS stream,
    // wrapped in a buffered stream
    pub(crate) stream: PgStream,

    // process id of this backend
    // used to send cancel requests
    #[allow(dead_code)]
    process_id: u32,

    // secret key of this backend
    // used to send cancel requests
    #[allow(dead_code)]
    secret_key: u32,

    // sequence of statement IDs for use in preparing statements
    // in PostgreSQL, the statement is prepared to a user-supplied identifier
    next_statement_id: StatementId,

    // cache statement by query string to the id and columns
    cache_statement: StatementCache<(StatementId, Arc<PgStatementMetadata>)>,

    // cache user-defined types by id <-> info
    cache_type_info: HashMap<Oid, PgTypeInfo>,
    cache_type_oid: HashMap<UStr, Oid>,
    cache_elem_type_to_array: HashMap<Oid, Oid>,
    cache_table_data: HashMap<Oid, TableData>,

    // number of ReadyForQuery messages that we are currently expecting
    pub(crate) pending_ready_for_query_count: usize,

    // current transaction status
    transaction_status: TransactionStatus,

    log_settings: LogSettings,
}

pub(crate) struct TableData {
    table_name: Arc<str>,
    /// Attribute number -> name.
    columns: BTreeMap<i16, Arc<str>>,
}

impl PgConnection {
    pub(crate) fn new(
        conn: PgConn,
        layers: &Layers<BoxService<Postgres>>,
        info: Arc<ConnectionInfo>,
    ) -> Self {
        let state = Arc::new(PgConnectionState {
            transaction_depth: AtomicUsize::new(0),
            pending_queries: Default::default(),
            server_version_num: conn.inner.stream.server_version_num,
        });

        let conn = Arc::new(Mutex::new(conn));

        let driver = PgDriver {
            conn: conn.clone(),
            state: state.clone(),
        };

        Self {
            conn,
            state,
            service: ConnectionService::new(driver, layers, info),
        }
    }

    /// the version number of the server in `libpq` format
    pub fn server_version_num(&self) -> Option<u32> {
        self.state.server_version_num
    }

    /// Lock the protocol state of the connection, bypassing middleware.
    pub(crate) fn lock(&self) -> impl Future<Output = MutexGuardArc<PgConn>> + Send + 'static {
        lock(self.conn.clone(), self.state.clone())
    }

    /// Lock the protocol state of the connection if it's not in use.
    pub(crate) fn try_lock(&self) -> Option<MutexGuardArc<PgConn>> {
        let mut conn = self.conn.try_lock_arc()?;
        self.state.queue_pending_queries(&mut conn);
        Some(conn)
    }

    /// Queue a simple query (not prepared) to execute the next time this connection is used.
    ///
    /// Used for rolling back transactions and releasing advisory locks.
    pub(crate) fn queue_simple_query(&mut self, query: SqlStr) {
        let mut conn = self.try_lock();
        self.state.queue_simple_query(conn.as_deref_mut(), query);
    }

    pub(crate) fn transaction_depth(&self) -> usize {
        self.state.transaction_depth.load(Ordering::Acquire)
    }
}

/// Lock the protocol state of a connection.
pub(crate) async fn lock(
    conn: Arc<Mutex<PgConn>>,
    state: Arc<PgConnectionState>,
) -> MutexGuardArc<PgConn> {
    let mut conn = conn.lock_arc().await;
    state.queue_pending_queries(&mut conn);
    conn
}

impl PgConnectionState {
    pub(crate) fn transaction_depth(&self) -> usize {
        self.transaction_depth.load(Ordering::Acquire)
    }

    pub(crate) fn increment_transaction_depth(&self) {
        self.transaction_depth.fetch_add(1, Ordering::AcqRel);
    }

    /// Decrement the transaction depth if it's not zero, returning the previous depth.
    pub(crate) fn decrement_transaction_depth(&self) -> Option<usize> {
        crate::common::decrement_nonzero(&self.transaction_depth)
    }

    /// Queue a `ROLLBACK` of the current transaction or savepoint, if any,
    /// without waiting for it.
    ///
    /// `conn` should be `None` if the connection is locked elsewhere.
    pub(crate) fn start_rollback(&self, conn: Option<&mut PgConn>) {
        if let Some(depth) = self.decrement_transaction_depth() {
            self.queue_simple_query(conn, rollback_ansi_transaction_sql(depth));
        }
    }

    /// Queue a simple query on `conn`, or on the connection the next time it's locked if `None`.
    pub(crate) fn queue_simple_query(&self, conn: Option<&mut PgConn>, query: SqlStr) {
        match conn {
            Some(conn) => conn
                .queue_simple_query(query.as_str())
                .expect("BUG: queued query somehow too large for protocol"),
            None => self.pending_queries.push(query),
        }
    }

    pub(crate) fn queue_pending_queries(&self, conn: &mut PgConn) {
        for query in self.pending_queries.take() {
            conn.queue_simple_query(query.as_str())
                .expect("BUG: queued query somehow too large for protocol");
        }
    }
}

impl PgConn {
    // will return when the connection is ready for another query
    pub(crate) async fn wait_until_ready(&mut self) -> Result<(), Error> {
        if !self.inner.stream.write_buffer_mut().is_empty() {
            self.inner.stream.flush().await?;
        }

        while self.inner.pending_ready_for_query_count > 0 {
            let message = self.inner.stream.recv().await?;

            if let BackendMessageFormat::ReadyForQuery = message.format {
                self.handle_ready_for_query(message)?;
            }
        }

        Ok(())
    }

    async fn recv_ready_for_query(&mut self) -> Result<(), Error> {
        let r: ReadyForQuery = self.inner.stream.recv_expect().await?;

        self.inner.pending_ready_for_query_count -= 1;
        self.inner.transaction_status = r.transaction_status;

        Ok(())
    }

    #[inline(always)]
    fn handle_ready_for_query(&mut self, message: ReceivedMessage) -> Result<(), Error> {
        self.inner.pending_ready_for_query_count = self
            .inner
            .pending_ready_for_query_count
            .checked_sub(1)
            .ok_or_else(|| err_protocol!("received more ReadyForQuery messages than expected"))?;

        self.inner.transaction_status = message.decode::<ReadyForQuery>()?.transaction_status;

        Ok(())
    }

    /// Queue a simple query (not prepared) to execute the next time this connection is used.
    ///
    /// Used for rolling back transactions and releasing advisory locks.
    #[inline(always)]
    pub(crate) fn queue_simple_query(&mut self, query: &str) -> Result<(), Error> {
        self.inner.stream.write_msg(Query(query))?;
        self.inner.pending_ready_for_query_count += 1;

        Ok(())
    }

    pub(crate) fn in_transaction(&self) -> bool {
        match self.inner.transaction_status {
            TransactionStatus::Transaction => true,
            TransactionStatus::Error | TransactionStatus::Idle => false,
        }
    }

    pub(crate) async fn invalidate_cached_statement(
        &mut self,
        sql: &str,
        backend: bool,
    ) -> Result<(), Error> {
        self.wait_until_ready().await?;

        let Some((statement_id, _)) = self.inner.cache_statement.remove(sql) else {
            return Ok(());
        };

        if backend {
            self.inner
                .stream
                .write_msg(Close::Statement(statement_id))?;
            self.write_sync();
            self.inner.stream.flush().await?;

            self.wait_for_close_complete(1).await?;
            self.recv_ready_for_query().await?;
        }

        Ok(())
    }
}

impl Debug for PgConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgConnection").finish()
    }
}

impl Debug for PgConn {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgConn").finish_non_exhaustive()
    }
}

/// Sends requests through the middleware stack of this connection.
///
/// The stream of a response keeps the connection busy until it's exhausted or dropped, even
/// though it doesn't borrow the connection: drop it before sending another request on the same
/// task, or that request waits forever.
///
/// See the [`middleware`][sqlx_core::middleware] module for details.
impl Service<Request<Postgres>> for PgConnection {
    type Response = Response<Postgres>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Response<Postgres>, Error>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    #[inline]
    fn call(&mut self, request: Request<Postgres>) -> Self::Future {
        let depth = self.transaction_depth();
        self.service.call(request, depth)
    }
}

impl Connection for PgConnection {
    type Database = Postgres;

    type Options = PgConnectOptions;

    async fn close(self) -> Result<(), Error> {
        let mut conn = self.lock().await;

        // The normal, graceful termination procedure is that the frontend sends a Terminate
        // message and immediately closes the connection.

        // On receipt of this message, the backend closes the
        // connection and terminates.
        conn.inner.stream.send(Terminate).await?;
        conn.inner.stream.shutdown().await?;

        Ok(())
    }

    async fn close_hard(self) -> Result<(), Error> {
        self.lock().await.inner.stream.shutdown().await?;

        Ok(())
    }

    async fn ping(&mut self) -> Result<(), Error> {
        let mut conn = self.lock().await;

        // Users were complaining about this showing up in query statistics on the server.
        // By sending a comment we avoid an error if the connection was in the middle of a rowset
        // self.execute("/* SQLx ping */").map_ok(|_| ()).boxed()

        // The simplest call-and-response that's possible.
        conn.write_sync();
        conn.wait_until_ready().await
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

    fn cached_statements_size(&self) -> usize {
        // The connection can only be locked by a response stream that middleware kept alive.
        self.try_lock()
            .map_or(0, |conn| conn.inner.cache_statement.len())
    }

    async fn clear_cached_statements(&mut self) -> Result<(), Error> {
        let mut conn = self.lock().await;

        conn.inner.cache_type_oid.clear();

        let mut cleared = 0_usize;

        conn.wait_until_ready().await?;

        while let Some((id, _)) = conn.inner.cache_statement.remove_lru() {
            conn.inner.stream.write_msg(Close::Statement(id))?;
            cleared += 1;
        }

        if cleared > 0 {
            conn.write_sync();
            conn.inner.stream.flush().await?;

            conn.wait_for_close_complete(cleared).await?;
            conn.recv_ready_for_query().await?;
        }

        Ok(())
    }

    fn shrink_buffers(&mut self) {
        if let Some(mut conn) = self.try_lock() {
            conn.inner.stream.shrink_buffers();
        }
    }

    #[doc(hidden)]
    async fn flush(&mut self) -> Result<(), Error> {
        self.lock().await.wait_until_ready().await
    }

    #[doc(hidden)]
    fn should_flush(&self) -> bool {
        // If the connection is locked, we can't check, so assume a flush is needed.
        self.try_lock()
            .is_none_or(|conn| !conn.inner.stream.write_buffer().is_empty())
    }
}

// Implement `AsMut<Self>` so that `PgConnection` can be wrapped in
// a `PgAdvisoryLockGuard`.
//
// See: https://github.com/launchbadge/sqlx/issues/2520
impl AsMut<PgConnection> for PgConnection {
    fn as_mut(&mut self) -> &mut PgConnection {
        self
    }
}
