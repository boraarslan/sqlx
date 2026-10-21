use crate::any::{Any, AnyConnection};
use crate::error::Error;
use crate::executor::Executor;
use crate::middleware::{Request, Response};
use futures_core::future::BoxFuture;

impl<'c> Executor<'c> for &'c mut AnyConnection {
    type Database = Any;

    #[inline]
    fn dispatch<'e>(self, request: Request<Any>) -> BoxFuture<'e, Result<Response<Any>, Error>>
    where
        'c: 'e,
    {
        self.service.dispatch(request)
    }
}
