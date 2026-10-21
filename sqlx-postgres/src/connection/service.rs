use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_lock::{Mutex, MutexGuardArc};
use futures_core::future::BoxFuture;
use futures_util::TryStreamExt;
use sqlx_core::middleware::{Request, RequestKind, Response, ResultStream, Service};
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
        let conn = lock(self.conn.clone(), self.state.clone());
        let state = self.state.clone();

        Box::pin(async move {
            let mut conn = conn.await;
            let request = request.into_parts();

            match request.kind {
                RequestKind::Fetch | RequestKind::FetchOptional => {
                    let metadata = request.statement.map(|statement| statement.metadata);

                    let stream = try_stream! {
                        let mut stream = pin!(
                            conn.run(request.sql, request.arguments, request.persistent, metadata)
                                .await?
                        );

                        while let Some(item) = stream.try_next().await? {
                            r#yield!(item);
                        }

                        Ok(())
                    };

                    ResultStream::new(stream)
                        .prefetch()
                        .await
                        .map(Response::Stream)
                }
                RequestKind::Prepare => conn
                    .prepare_statement(request.sql, &request.parameter_types)
                    .await
                    .map(Response::Statement),
                #[cfg(feature = "offline")]
                RequestKind::Describe => conn.describe(request.sql).await.map(Response::Describe),
                RequestKind::Begin => {
                    begin(&mut conn, &state, request.sql).await?;
                    Ok(Response::Done)
                }
                RequestKind::Commit | RequestKind::Rollback => {
                    end_transaction(&mut conn, &state, request.sql).await?;
                    Ok(Response::Done)
                }
                kind => Err(Error::InvalidArgument(format!(
                    "unsupported request kind for Postgres: {kind:?}"
                ))),
            }
        })
    }
}

async fn begin(
    conn: &mut MutexGuardArc<PgConn>,
    state: &PgConnectionState,
    statement: SqlStr,
) -> Result<(), Error> {
    let depth = state.transaction_depth();

    let rollback = Rollback::new(conn, state);
    rollback.conn.queue_simple_query(statement.as_str())?;
    // Claim the depth before the round trip, not after. `start_rollback` -- which both
    // this guard and `Transaction`'s own drop guard call -- is a no-op while the depth is
    // zero, so a future cancelled during the await below would otherwise leave the server
    // in a transaction with nothing queued to end it. `Pool` then hands that connection
    // to the next borrower, whose statements silently run inside it. Unwound just below
    // if the BEGIN did not take.
    state.set_transaction_depth(depth + 1);
    rollback.conn.wait_until_ready().await?;
    if !rollback.conn.in_transaction() {
        state.set_transaction_depth(depth);
        return Err(Error::BeginFailed);
    }
    rollback.defuse();

    Ok(())
}

/// Execute a `COMMIT` or `ROLLBACK` (or their savepoint equivalents).
async fn end_transaction(
    conn: &mut PgConn,
    state: &PgConnectionState,
    statement: SqlStr,
) -> Result<(), Error> {
    let depth = state.transaction_depth();

    if depth > 0 {
        let mut stream = pin!(conn.run(statement, None, false, None).await?);
        while stream.try_next().await?.is_some() {}

        state.set_transaction_depth(depth - 1);
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
