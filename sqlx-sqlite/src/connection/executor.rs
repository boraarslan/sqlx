use crate::{Sqlite, SqliteConnection};
use futures_core::future::BoxFuture;
use sqlx_core::error::Error;
use sqlx_core::executor::Executor;
use sqlx_core::middleware::{Request, Response};

impl<'c> Executor<'c> for &'c mut SqliteConnection {
    type Database = Sqlite;

    #[inline]
    fn dispatch<'e>(
        self,
        request: Request<Sqlite>,
    ) -> BoxFuture<'e, Result<Response<Sqlite>, Error>>
    where
        'c: 'e,
    {
        let depth = self.worker.shared.get_transaction_depth();
        self.service.dispatch(request, depth)
    }
}
