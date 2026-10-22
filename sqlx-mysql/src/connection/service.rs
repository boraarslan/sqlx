use std::future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_lock::{Mutex, MutexGuardArc};
use futures_core::future::BoxFuture;
use futures_util::TryStreamExt;
use sqlx_core::middleware::{Request, RequestKind, RequestParts, Response, ResultStream, Service};
use sqlx_core::sql_str::SqlStr;

use crate::connection::{lock, MySqlConn, MySqlConnectionState};
use crate::error::Error;
use crate::MySql;

/// The innermost service of a [`MySqlConnection`][crate::MySqlConnection]'s middleware stack,
/// which executes requests on the connection.
///
/// It shares the connection's protocol state with the
/// [`MySqlConnection`][crate::MySqlConnection] so that responses can be `'static`:
/// the stream of a [`Response::Stream`] keeps the connection locked until it is exhausted
/// or dropped.
#[derive(Clone)]
pub(crate) struct MySqlDriver {
    pub(crate) conn: Arc<Mutex<MySqlConn>>,
    pub(crate) state: Arc<MySqlConnectionState>,
}

impl Service<Request<MySql>> for MySqlDriver {
    type Response = Response<MySql>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Response<MySql>, Error>>;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<MySql>) -> Self::Future {
        // Lock the connection right away if it's not in use, which avoids an allocation.
        let locked = match self.conn.try_lock_arc() {
            Some(mut conn) => {
                self.state.queue_pending_queries(&mut conn);
                Ok(conn)
            }
            None => Err(self.conn.clone()),
        };
        let state = self.state.clone();

        let RequestParts {
            kind,
            sql,
            arguments,
            persistent,
            ..
        } = request.into_parts();

        // Each kind of request has its own future, so that the common ones stay small.
        match kind {
            RequestKind::Fetch | RequestKind::FetchOptional => Box::pin(async move {
                let conn = acquire(locked, &state).await;

                // Stop reading after the first row; the rest of the response is discarded
                // before the next request.
                let first_row_only = kind == RequestKind::FetchOptional;

                // The stream owns the lock guard.
                let stream =
                    MySqlConn::run(conn, sql, arguments, persistent, first_row_only).await?;

                ResultStream::new(stream)
                    .prefetch()
                    .await
                    .map(Response::Stream)
            }),
            RequestKind::Prepare => Box::pin(async move {
                let mut conn = acquire(locked, &state).await;

                conn.prepare_with_cache(sql).await.map(Response::Statement)
            }),
            #[cfg(feature = "offline")]
            RequestKind::Describe => Box::pin(async move {
                let mut conn = acquire(locked, &state).await;

                conn.describe(sql).await.map(Response::Describe)
            }),
            RequestKind::Begin => Box::pin(async move {
                let mut conn = acquire(locked, &state).await;
                begin(&mut conn, &state, sql).await?;

                Ok(Response::Done)
            }),
            RequestKind::Commit | RequestKind::Rollback => Box::pin(async move {
                let mut conn = acquire(locked, &state).await;

                if state.transaction_depth() > 0 {
                    execute(&mut conn, sql).await?;
                    state.decrement_transaction_depth();
                }

                Ok(Response::Done)
            }),
            kind => Box::pin(future::ready(Err(Error::InvalidArgument(format!(
                "unsupported request kind for MySQL: {kind:?}"
            ))))),
        }
    }
}

/// Wait for the lock on the connection if `call()` could not take it right away.
async fn acquire(
    locked: Result<MutexGuardArc<MySqlConn>, Arc<Mutex<MySqlConn>>>,
    state: &Arc<MySqlConnectionState>,
) -> MutexGuardArc<MySqlConn> {
    match locked {
        Ok(conn) => conn,
        Err(conn) => lock(conn, state.clone()).await,
    }
}

async fn begin(
    conn: &mut MySqlConn,
    state: &MySqlConnectionState,
    statement: SqlStr,
) -> Result<(), Error> {
    // Claim the depth before the round trip, not after: if this future is cancelled while
    // waiting for the response, the guard queues a `ROLLBACK`, so that the connection is not
    // left in a transaction that nothing will end (see the Postgres driver).
    state.increment_transaction_depth();
    let mut rollback = Rollback {
        conn,
        state,
        defused: false,
    };

    let res = execute(rollback.conn, statement).await;

    // The statement completed, so there is no need to roll back if it failed or didn't start a
    // transaction. (`execute()` receives the responses to statements sent earlier before sending
    // this one, so an error from them means this one was not sent.)
    rollback.defused = true;

    if let Err(e) = res {
        state.decrement_transaction_depth();
        return Err(e);
    }

    if !rollback.conn.in_transaction() {
        state.decrement_transaction_depth();
        return Err(Error::BeginFailed);
    }

    Ok(())
}

/// Rolls back the transaction claimed by [`begin()`] if dropped before being defused.
struct Rollback<'c> {
    conn: &'c mut MySqlConn,
    state: &'c MySqlConnectionState,
    defused: bool,
}

impl Drop for Rollback<'_> {
    fn drop(&mut self) {
        if !self.defused {
            self.state.start_rollback(Some(self.conn));
        }
    }
}

/// Execute a statement using the text protocol and wait for it to complete.
async fn execute(conn: &mut MySqlConn, statement: SqlStr) -> Result<(), Error> {
    let mut stream = pin!(MySqlConn::run(&mut *conn, statement, None, false, false).await?);
    while stream.try_next().await?.is_some() {}

    Ok(())
}
