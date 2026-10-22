use std::future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_lock::{Mutex, MutexGuardArc};
use futures_core::future::BoxFuture;
use futures_util::TryStreamExt;
use sqlx_core::middleware::{Request, RequestKind, RequestParts, Response, ResultStream, Service};
use sqlx_core::sql_str::SqlStr;

use crate::connection::{lock, PgConn, PgConnectionState};
use crate::error::Error;
use crate::Postgres;

/// The innermost service of a [`PgConnection`][crate::PgConnection]'s middleware stack,
/// which executes requests on the connection.
///
/// It shares the connection's protocol state with the [`PgConnection`][crate::PgConnection]
/// so that responses can be `'static`: the stream of a [`Response::Stream`] keeps the
/// connection locked until it is exhausted or dropped.
#[derive(Clone)]
pub(crate) struct PgDriver {
    pub(crate) conn: Arc<Mutex<PgConn>>,
    pub(crate) state: Arc<PgConnectionState>,
}

impl Service<Request<Postgres>> for PgDriver {
    type Response = Response<Postgres>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Response<Postgres>, Error>>;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Postgres>) -> Self::Future {
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
            statement,
            parameter_types,
            ..
        } = request.into_parts();

        // Each kind of request has its own future, so that the common ones stay small.
        match kind {
            RequestKind::Fetch | RequestKind::FetchOptional => Box::pin(async move {
                let conn = acquire(locked, &state).await;
                let metadata = statement.map(|statement| statement.metadata);

                // The stream owns the lock guard.
                let stream = PgConn::run(conn, sql, arguments, persistent, metadata).await?;

                ResultStream::new(stream)
                    .prefetch()
                    .await
                    .map(Response::Stream)
            }),
            RequestKind::Prepare => Box::pin(async move {
                let mut conn = acquire(locked, &state).await;

                conn.prepare_statement(sql, &parameter_types)
                    .await
                    .map(Response::Statement)
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
                end_transaction(&mut conn, &state, sql).await?;

                Ok(Response::Done)
            }),
            kind => Box::pin(future::ready(Err(Error::InvalidArgument(format!(
                "unsupported request kind for Postgres: {kind:?}"
            ))))),
        }
    }
}

/// Wait for the lock on the connection if `call()` could not take it right away.
async fn acquire(
    locked: Result<MutexGuardArc<PgConn>, Arc<Mutex<PgConn>>>,
    state: &Arc<PgConnectionState>,
) -> MutexGuardArc<PgConn> {
    match locked {
        Ok(conn) => conn,
        Err(conn) => lock(conn, state.clone()).await,
    }
}

async fn begin(
    conn: &mut MutexGuardArc<PgConn>,
    state: &PgConnectionState,
    statement: SqlStr,
) -> Result<(), Error> {
    // Receive the responses to statements sent earlier (like queued rollbacks) first, so that an
    // error below is the response to this statement.
    conn.wait_until_ready().await?;

    let rollback = Rollback::new(conn, state);
    rollback.conn.queue_simple_query(statement.as_str())?;
    // Claim the depth before the round trip, not after. `start_rollback` -- which this guard
    // calls -- is a no-op while the depth is zero, so a future cancelled during the await below
    // would otherwise leave the server in a transaction with nothing queued to end it. `Pool`
    // then hands that connection to the next borrower, whose statements silently run inside
    // it. Unwound just below if the BEGIN did not take.
    state.increment_transaction_depth();

    let in_transaction = match rollback.conn.wait_until_ready().await {
        Ok(()) => rollback.conn.in_transaction(),
        Err(e) => {
            // The statement failed, so there is no transaction or savepoint to roll back.
            rollback.defuse();
            state.decrement_transaction_depth();
            return Err(e);
        }
    };

    rollback.defuse();

    if !in_transaction {
        state.decrement_transaction_depth();
        return Err(Error::BeginFailed);
    }

    Ok(())
}

/// Execute a `COMMIT` or `ROLLBACK` (or their savepoint equivalents).
async fn end_transaction(
    conn: &mut PgConn,
    state: &PgConnectionState,
    statement: SqlStr,
) -> Result<(), Error> {
    if state.transaction_depth() > 0 {
        let mut stream = pin!(PgConn::run(&mut *conn, statement, None, false, None).await?);
        while stream.try_next().await?.is_some() {}

        state.decrement_transaction_depth();
    }

    Ok(())
}

struct Rollback<'c> {
    conn: &'c mut PgConn,
    state: &'c PgConnectionState,
    defuse: bool,
}

impl Drop for Rollback<'_> {
    fn drop(&mut self) {
        if !self.defuse {
            self.state.start_rollback(Some(self.conn));
        }
    }
}

impl<'c> Rollback<'c> {
    fn new(conn: &'c mut PgConn, state: &'c PgConnectionState) -> Self {
        Self {
            conn,
            state,
            defuse: false,
        }
    }

    fn defuse(mut self) {
        self.defuse = true;
    }
}
