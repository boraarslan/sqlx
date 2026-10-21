use std::sync::Arc;
use std::task::{Context, Poll};

use either::Either;
use futures_core::future::BoxFuture;
use futures_util::{future, StreamExt};

use crate::any::{Any, AnyArguments, AnyColumn, AnyQueryResult, AnyRow, AnyStatement, AnyTypeInfo};
use crate::database::Database;
use crate::error::{BoxDynError, Error};
use crate::ext::ustr::UStr;
use crate::middleware::{Request, RequestParts, Response, ResultStream, Service};
use crate::HashMap;

/// Adapts the request service of a database driver to handle `Request<Any>`,
/// converting arguments, rows, results and statements between `Any` and the driver's types.
///
/// For use in implementations of [`AnyConnectionBackend::service()`].
///
/// [`AnyConnectionBackend::service()`]: crate::any::AnyConnectionBackend::service
#[doc(hidden)]
pub struct AnyServiceAdapter<S, DB: Database> {
    service: S,
    convert_arguments: fn(AnyArguments) -> Result<DB::Arguments, BoxDynError>,
    column_names: fn(&DB::Statement) -> Arc<HashMap<UStr, usize>>,
}

impl<S, DB: Database> AnyServiceAdapter<S, DB> {
    /// Wrap the request service of a driver.
    ///
    /// * `convert_arguments` converts the arguments of a request.
    /// * `column_names` returns the column name lookup table of a prepared statement.
    pub fn new(
        service: S,
        convert_arguments: fn(AnyArguments) -> Result<DB::Arguments, BoxDynError>,
        column_names: fn(&DB::Statement) -> Arc<HashMap<UStr, usize>>,
    ) -> Self {
        Self {
            service,
            convert_arguments,
            column_names,
        }
    }
}

impl<S, DB> Service<Request<Any>> for AnyServiceAdapter<S, DB>
where
    DB: Database,
    S: Service<
        Request<DB>,
        Response = Response<DB>,
        Error = Error,
        Future = BoxFuture<'static, Result<Response<DB>, Error>>,
    >,
    AnyQueryResult: From<DB::QueryResult>,
    AnyRow: for<'a> TryFrom<&'a DB::Row, Error = Error>,
    AnyTypeInfo: for<'a> TryFrom<&'a DB::TypeInfo, Error = Error>,
    AnyColumn: for<'a> TryFrom<&'a DB::Column, Error = Error>,
{
    type Response = Response<Any>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Response<Any>, Error>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Any>) -> Self::Future {
        let parts = request.into_parts();

        let arguments = match parts.arguments.map(self.convert_arguments).transpose() {
            Ok(arguments) => arguments,
            Err(e) => return Box::pin(future::ready(Err(Error::Encode(e)))),
        };

        // `AnyStatement` and `AnyTypeInfo` cannot be converted back to the driver's types,
        // so the statement and parameter types are not passed on.
        let request = Request::from_parts(RequestParts {
            kind: parts.kind,
            sql: parts.sql,
            arguments,
            persistent: parts.persistent,
            statement: None,
            parameter_types: Vec::new(),
            extensions: parts.extensions,
        });

        let response = self.service.call(request);
        let column_names = self.column_names;

        Box::pin(async move {
            Ok(match response.await? {
                Response::Stream(stream) => {
                    Response::Stream(ResultStream::new(stream.map(|item| match item? {
                        Either::Left(result) => Ok(Either::Left(result.into())),
                        Either::Right(row) => Ok(Either::Right(AnyRow::try_from(&row)?)),
                    })))
                }
                Response::Statement(statement) => {
                    let column_names = column_names(&statement);
                    Response::Statement(AnyStatement::try_from_statement(statement, column_names)?)
                }
                Response::Describe(describe) => Response::Describe(describe.try_into_any()?),
                Response::Done => Response::Done,
            })
        })
    }
}
