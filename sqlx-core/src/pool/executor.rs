use std::future::poll_fn;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use either::Either;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_core::Stream;
use futures_util::{StreamExt, TryStreamExt};

use crate::database::Database;
use crate::error::Error;
use crate::executor::{provided, Execute, Executor};
use crate::middleware::{BoxService, Request, Response, ResultItem, ResultStream, Service};
use crate::pool::inner::PoolInner;
use crate::pool::{Pool, PoolConnection};
use crate::sql_str::SqlStr;

impl<'p, DB: Database> Executor<'p> for &'_ Pool<DB>
where
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
{
    type Database = DB;

    fn dispatch<'e>(self, mut request: Request<DB>) -> BoxFuture<'e, Result<Response<DB>, Error>>
    where
        'p: 'e,
    {
        let pool = self.clone();

        Box::pin(async move {
            match service(&pool.0) {
                Some(service) => {
                    request.set_connection_info(pool.0.connection_info());

                    let mut service = service.clone();
                    poll_fn(|cx| service.poll_ready(cx)).await?;
                    service.call(request).await
                }
                None => acquire_and_dispatch(pool, request).await,
            }
        })
    }

    // Without pool middleware, queries are passed to the methods of the connection, so that
    // connections whose `Executor` doesn't implement `dispatch()` keep working.

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<DB::QueryResult, DB::Row>, Error>>
    where
        'p: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        if !self.0.options.layers.is_empty() {
            return provided::fetch_many(self, query);
        }

        let pool = self.clone();

        Box::pin(try_stream! {
            let mut conn = pool.acquire().await?;
            let mut s = conn.fetch_many(query);

            while let Some(v) = s.try_next().await? {
                r#yield!(v);
            }

            Ok(())
        })
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<DB::Row>, Error>>
    where
        'p: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        if !self.0.options.layers.is_empty() {
            return provided::fetch_optional(self, query);
        }

        let pool = self.clone();

        Box::pin(async move { pool.acquire().await?.fetch_optional(query).await })
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::Statement, Error>>
    where
        'p: 'e,
    {
        if !self.0.options.layers.is_empty() {
            return provided::prepare_with(self, sql, parameters);
        }

        let pool = self.clone();

        Box::pin(async move { pool.acquire().await?.prepare_with(sql, parameters).await })
    }

    #[doc(hidden)]
    #[cfg(feature = "offline")]
    fn describe<'e>(
        self,
        sql: SqlStr,
    ) -> BoxFuture<'e, Result<crate::describe::Describe<Self::Database>, Error>>
    where
        'p: 'e,
    {
        if !self.0.options.layers.is_empty() {
            return provided::describe(self, sql);
        }

        let pool = self.clone();

        Box::pin(async move { pool.acquire().await?.describe(sql).await })
    }
}

/// Sends requests through the middleware stack configured with
/// [`PoolOptions::layer()`][crate::pool::PoolOptions::layer], like `&Pool` as an [`Executor`].
///
/// The future returned by `call()` acquires a connection, then sends the request to it.
/// The response keeps the connection until it is dropped or, for a
/// [`ResultStream`], until the stream is exhausted.
impl<DB: Database> Service<Request<DB>> for Pool<DB>
where
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
{
    type Response = Response<DB>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Response<DB>, Error>>;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // The middleware stack is shared by all users of the pool, so its readiness is
        // awaited by the response future, through its own handle to the stack.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<DB>) -> Self::Future {
        (&*self).dispatch(request)
    }
}

fn service<DB: Database>(pool: &Arc<PoolInner<DB>>) -> Option<&BoxService<DB>>
where
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
{
    pool.service
        .get_or_init(|| {
            if pool.options.layers.is_empty() {
                return None;
            }

            // Use a weak reference to avoid a reference cycle through `PoolInner::service`.
            let inner = BoxService::new(PoolService {
                pool: Arc::downgrade(pool),
            });

            Some(pool.options.layers.apply(inner))
        })
        .as_ref()
}

/// The innermost service of the pool middleware stack.
struct PoolService<DB: Database> {
    pool: Weak<PoolInner<DB>>,
}

impl<DB: Database> Clone for PoolService<DB> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}

impl<DB: Database> Service<Request<DB>> for PoolService<DB>
where
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
{
    type Response = Response<DB>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Response<DB>, Error>>;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<DB>) -> Self::Future {
        let pool = self.pool.upgrade();

        Box::pin(async move {
            let pool = pool.ok_or(Error::PoolClosed)?;
            acquire_and_dispatch(Pool(pool), request).await
        })
    }
}

async fn acquire_and_dispatch<DB: Database>(
    pool: Pool<DB>,
    request: Request<DB>,
) -> Result<Response<DB>, Error>
where
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
{
    if request.kind().is_transaction_control() {
        return Err(Error::InvalidArgument(format!(
            "a {:?} request cannot be sent to a pool as the connection would be returned to the \
             pool immediately; use `Pool::begin()` instead",
            request.kind()
        )));
    }

    let mut conn = pool.acquire().await?;
    let response = (&mut *conn).dispatch(request).await?;

    // Keep the connection until the stream is finished; otherwise it's returned to the pool here.
    Ok(response.map_stream(|stream| {
        ResultStream::new(WithConnection {
            stream,
            conn: Some(conn),
        })
    }))
}

struct WithConnection<DB: Database> {
    // Must be dropped before `conn` as it may hold the connection locked.
    stream: ResultStream<DB>,
    conn: Option<PoolConnection<DB>>,
}

// We never pin-project into the fields.
impl<DB: Database> Unpin for WithConnection<DB> {}

impl<DB: Database> Stream for WithConnection<DB> {
    type Item = ResultItem<DB>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = self.stream.poll_next_unpin(cx);

        if let Poll::Ready(None) = res {
            // Release the connection back to the pool as soon as possible.
            self.stream = ResultStream::empty();
            self.conn = None;
        }

        res
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
