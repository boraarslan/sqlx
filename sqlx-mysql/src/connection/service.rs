use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_lock::Mutex;
use futures_core::future::BoxFuture;
use futures_util::TryStreamExt;
use sqlx_core::middleware::{Request, RequestKind, Response, ResultStream, Service};
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
        let conn = lock(self.conn.clone(), self.state.clone());
        let state = self.state.clone();

        Box::pin(async move {
            let mut conn = conn.await;
            let request = request.into_parts();

            match request.kind {
                RequestKind::Fetch | RequestKind::FetchOptional => {
                    // Stop reading after the first row; the rest of the response is discarded
                    // before the next request.
                    let optional = request.kind == RequestKind::FetchOptional;

                    let stream = try_stream! {
                        let mut stream = pin!(
                            conn.run(request.sql, request.arguments, request.persistent).await?
                        );

                        while let Some(item) = stream.try_next().await? {
                            let is_row = item.is_right();

                            r#yield!(item);

                            if optional && is_row {
                                break;
                            }
                        }

                        Ok(())
                    };

                    ResultStream::new(stream)
                        .prefetch()
                        .await
                        .map(Response::Stream)
                }
                RequestKind::Prepare => conn
                    .prepare_with_cache(request.sql)
                    .await
                    .map(Response::Statement),
                #[cfg(feature = "offline")]
                RequestKind::Describe => conn.describe(request.sql).await.map(Response::Describe),
                RequestKind::Begin => {
                    let depth = state.transaction_depth();

                    execute(&mut conn, request.sql).await?;

                    if !conn.in_transaction() {
                        return Err(Error::BeginFailed);
                    }

                    state.set_transaction_depth(depth + 1);

                    Ok(Response::Done)
                }
                RequestKind::Commit | RequestKind::Rollback => {
                    let depth = state.transaction_depth();

                    if depth > 0 {
                        execute(&mut conn, request.sql).await?;
                        state.set_transaction_depth(depth - 1);
                    }

                    Ok(Response::Done)
                }
                kind => Err(Error::InvalidArgument(format!(
                    "unsupported request kind for MySQL: {kind:?}"
                ))),
            }
        })
    }
}

/// Execute a statement using the text protocol and wait for it to complete.
async fn execute(conn: &mut MySqlConn, statement: SqlStr) -> Result<(), Error> {
    let mut stream = pin!(conn.run(statement, None, false).await?);
    while stream.try_next().await?.is_some() {}

    Ok(())
}
