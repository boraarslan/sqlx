use std::future;
use std::task::{Context, Poll};

use futures_core::future::BoxFuture;
use sqlx_core::error::Error;
use sqlx_core::middleware::{Request, RequestKind, RequestParts, Response, ResultStream, Service};

use crate::connection::worker::ConnectionWorker;
use crate::Sqlite;

/// The innermost service of a [`SqliteConnection`][crate::SqliteConnection]'s middleware stack,
/// which sends requests to the worker thread.
///
/// Responses are `'static` as they are received from the worker over channels.
#[derive(Clone)]
pub(crate) struct SqliteDriver {
    pub(crate) worker: ConnectionWorker,
    pub(crate) row_channel_size: usize,
}

impl Service<Request<Sqlite>> for SqliteDriver {
    type Response = Response<Sqlite>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Response<Sqlite>, Error>>;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Sqlite>) -> Self::Future {
        let worker = self.worker.clone();

        let RequestParts {
            kind,
            sql,
            arguments,
            persistent,
            ..
        } = request.into_parts();

        // Each kind of request has its own future, so that the common ones stay small.
        match kind {
            RequestKind::Fetch | RequestKind::FetchOptional => {
                let row_channel_size = self.row_channel_size;

                Box::pin(async move {
                    let persistent = persistent && arguments.is_some();
                    let limit = (kind == RequestKind::FetchOptional).then_some(1);

                    let rows = worker
                        .execute(sql, arguments, row_channel_size, persistent, limit)
                        .await?;

                    ResultStream::new(rows.into_stream())
                        .prefetch()
                        .await
                        .map(Response::Stream)
                })
            }
            RequestKind::Prepare => {
                Box::pin(async move { worker.prepare(sql).await.map(Response::Statement) })
            }
            #[cfg(feature = "offline")]
            RequestKind::Describe => {
                Box::pin(async move { worker.describe(sql).await.map(Response::Describe) })
            }
            RequestKind::Begin => Box::pin(async move {
                worker.begin(sql).await?;
                Ok(Response::Done)
            }),
            RequestKind::Commit => Box::pin(async move {
                worker.commit(sql).await?;
                Ok(Response::Done)
            }),
            RequestKind::Rollback => Box::pin(async move {
                worker.rollback(sql).await?;
                Ok(Response::Done)
            }),
            kind => Box::pin(future::ready(Err(Error::InvalidArgument(format!(
                "unsupported request kind for SQLite: {kind:?}"
            ))))),
        }
    }
}
