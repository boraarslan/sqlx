use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use crate::arguments::Arguments;
use crate::database::Database;
use crate::error::{BoxDynError, Error};
use crate::executor::Execute;
use crate::middleware::{ConnectionInfo, Extensions};
use crate::sql_str::{SqlSafeStr, SqlStr};
use crate::transaction::{
    begin_ansi_transaction_sql, commit_ansi_transaction_sql, rollback_ansi_transaction_sql,
};

/// The kind of operation a [`Request`] asks the database to perform.
///
/// Determines which [`Response`][super::Response] variant the driver answers with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RequestKind {
    /// Execute the SQL and stream back all results and rows.
    ///
    /// Sent by [`Executor::fetch_many()`][crate::executor::Executor::fetch_many] and the methods
    /// built on it (`execute`, `fetch`, `fetch_all`, ...).
    ///
    /// Answered with [`Response::Stream`][super::Response::Stream].
    Fetch,
    /// Execute the SQL; the caller only needs the first row, if any.
    ///
    /// Sent by [`Executor::fetch_optional()`][crate::executor::Executor::fetch_optional] and
    /// [`Executor::fetch_one()`][crate::executor::Executor::fetch_one].
    /// Drivers may use this as a hint to stop reading rows early.
    ///
    /// Answered with [`Response::Stream`][super::Response::Stream].
    FetchOptional,
    /// Prepare the SQL as a statement without executing it.
    ///
    /// Sent by [`Executor::prepare_with()`][crate::executor::Executor::prepare_with].
    ///
    /// Answered with [`Response::Statement`][super::Response::Statement].
    Prepare,
    /// Describe the parameters and columns of the SQL; used by the query macros.
    ///
    /// Answered with [`Response::Describe`][super::Response::Describe].
    #[doc(hidden)]
    Describe,
    /// Begin a transaction or create a savepoint. The SQL is the statement to execute.
    ///
    /// Answered with [`Response::Done`][super::Response::Done].
    Begin,
    /// Commit a transaction or release a savepoint. The SQL is the statement to execute.
    ///
    /// Answered with [`Response::Done`][super::Response::Done].
    Commit,
    /// Roll back a transaction or roll back to a savepoint. The SQL is the statement to execute.
    ///
    /// Answered with [`Response::Done`][super::Response::Done].
    Rollback,
}

impl RequestKind {
    /// Returns `true` for [`Fetch`][Self::Fetch] and [`FetchOptional`][Self::FetchOptional].
    #[inline]
    pub fn is_fetch(&self) -> bool {
        matches!(self, Self::Fetch | Self::FetchOptional)
    }

    /// Returns `true` for [`Begin`][Self::Begin], [`Commit`][Self::Commit]
    /// and [`Rollback`][Self::Rollback].
    #[inline]
    pub fn is_transaction_control(&self) -> bool {
        matches!(self, Self::Begin | Self::Commit | Self::Rollback)
    }

    /// A short, static name for this kind of request, e.g. for use as a span or metric label.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Fetch => "fetch",
            Self::FetchOptional => "fetch_optional",
            Self::Prepare => "prepare",
            Self::Describe => "describe",
            Self::Begin => "begin",
            Self::Commit => "commit",
            Self::Rollback => "rollback",
        }
    }
}

/// A request sent through the middleware stack of a connection or pool.
///
/// Every statement SQLx sends to the database on behalf of the user is represented as a
/// `Request`: queries, statement preparation, and transaction control (`BEGIN`, `COMMIT`,
/// `ROLLBACK` and their savepoint equivalents).
///
/// A `Request` owns all of its data, so middleware can freely move it into futures,
/// clone it (if `DB::Arguments: Clone`), or send it to another task.
///
/// See [the module documentation][super] for details.
pub struct Request<DB: Database> {
    kind: RequestKind,
    sql: SqlStr,
    arguments: Option<DB::Arguments>,
    persistent: bool,
    statement: Option<DB::Statement>,
    parameter_types: Vec<DB::TypeInfo>,
    extensions: Extensions,
    connection_info: Option<Arc<ConnectionInfo>>,
    transaction_depth: usize,
}

/// The constituent parts of a [`Request`], for drivers and advanced middleware.
///
/// See [`Request::into_parts()`] and [`Request::from_parts()`].
#[non_exhaustive]
pub struct RequestParts<DB: Database> {
    /// See [`Request::kind()`].
    pub kind: RequestKind,
    /// See [`Request::sql()`].
    pub sql: SqlStr,
    /// See [`Request::arguments()`].
    pub arguments: Option<DB::Arguments>,
    /// See [`Request::persistent()`].
    pub persistent: bool,
    /// See [`Request::statement()`].
    pub statement: Option<DB::Statement>,
    /// See [`Request::parameter_types()`].
    pub parameter_types: Vec<DB::TypeInfo>,
    /// See [`Request::extensions()`].
    pub extensions: Extensions,
    /// See [`Request::connection_info()`].
    pub connection_info: Option<Arc<ConnectionInfo>>,
    /// See [`Request::transaction_depth()`].
    pub transaction_depth: usize,
}

impl<DB: Database> Request<DB> {
    /// Create a new request of the given kind.
    ///
    /// The request has no arguments, so if it's a query it will be executed using the
    /// simple (unprepared) query protocol, if the database has one.
    pub fn new(kind: RequestKind, sql: impl SqlSafeStr) -> Self {
        Self {
            kind,
            sql: sql.into_sql_str(),
            arguments: None,
            persistent: true,
            statement: None,
            parameter_types: Vec::new(),
            extensions: Extensions::new(),
            connection_info: None,
            transaction_depth: 0,
        }
    }

    /// Create a [`RequestKind::Fetch`] request.
    pub fn fetch(sql: impl SqlSafeStr) -> Self {
        Self::new(RequestKind::Fetch, sql)
    }

    /// Create a [`RequestKind::Prepare`] request.
    pub fn prepare(sql: impl SqlSafeStr, parameter_types: Vec<DB::TypeInfo>) -> Self {
        let mut this = Self::new(RequestKind::Prepare, sql);
        this.parameter_types = parameter_types;
        this
    }

    /// Create a [`RequestKind::Begin`] request for a connection at the given transaction depth.
    ///
    /// Returns [`Error::InvalidSavePointStatement`] if a custom `statement` is given while
    /// already in a transaction.
    #[doc(hidden)]
    pub fn begin(depth: usize, statement: Option<SqlStr>) -> Result<Self, Error> {
        let sql = match statement {
            // custom `BEGIN` statements are not allowed if we're already in
            // a transaction (we need to issue a `SAVEPOINT` instead)
            Some(_) if depth > 0 => return Err(Error::InvalidSavePointStatement),
            Some(statement) => statement,
            None => begin_ansi_transaction_sql(depth),
        };

        Ok(Self::new(RequestKind::Begin, sql))
    }

    /// Create a [`RequestKind::Commit`] request for a connection at the given (nonzero)
    /// transaction depth.
    #[doc(hidden)]
    pub fn commit(depth: usize) -> Self {
        Self::new(RequestKind::Commit, commit_ansi_transaction_sql(depth))
    }

    /// Create a [`RequestKind::Rollback`] request for a connection at the given (nonzero)
    /// transaction depth.
    #[doc(hidden)]
    pub fn rollback(depth: usize) -> Self {
        Self::new(RequestKind::Rollback, rollback_ansi_transaction_sql(depth))
    }

    /// Create a request from an [`Execute`] implementation, such as a [`Query`][crate::query::Query].
    ///
    /// Returns an error if encoding any of the bind arguments failed.
    pub fn from_execute<'q, E>(mut query: E, kind: RequestKind) -> Result<Self, Error>
    where
        E: Execute<'q, DB>,
    {
        let arguments = query.take_arguments().map_err(Error::Encode)?;
        let persistent = query.persistent();
        let statement = query.statement().cloned();
        let extensions = query.take_extensions();

        Ok(Self {
            kind,
            sql: query.sql(),
            arguments,
            persistent,
            statement,
            parameter_types: Vec::new(),
            extensions,
            connection_info: None,
            transaction_depth: 0,
        })
    }

    /// Set the bind arguments. A request with arguments is executed as a prepared statement.
    pub fn with_arguments(mut self, arguments: DB::Arguments) -> Self {
        self.arguments = Some(arguments);
        self
    }

    /// Set whether the prepared statement should be cached on the connection.
    pub fn with_persistent(mut self, persistent: bool) -> Self {
        self.persistent = persistent;
        self
    }

    /// Insert a value into the [extensions][Self::extensions] of this request.
    pub fn with_extension<T: Clone + Send + Sync + 'static>(mut self, value: T) -> Self {
        self.extensions.insert(value);
        self
    }

    /// The kind of operation requested.
    #[inline]
    pub fn kind(&self) -> RequestKind {
        self.kind
    }

    /// The SQL that will be sent to the database.
    #[inline]
    pub fn sql(&self) -> &SqlStr {
        &self.sql
    }

    /// Replace the SQL that will be sent to the database, returning the previous SQL.
    ///
    /// Useful for middleware that annotates queries, such as adding a comment with trace context.
    ///
    /// # Note
    /// Changing the SQL of a request built from a prepared [`Statement`][crate::statement::Statement]
    /// clears that statement, as its metadata may no longer apply.
    ///
    /// For transaction control requests ([`RequestKind::is_transaction_control()`]),
    /// the new SQL must have the same effect on the transaction state as the original.
    pub fn set_sql(&mut self, sql: impl SqlSafeStr) -> SqlStr {
        self.statement = None;
        std::mem::replace(&mut self.sql, sql.into_sql_str())
    }

    /// The bind arguments of the query, if any.
    ///
    /// A request without arguments is executed using the simple (unprepared) query protocol,
    /// if the database has one.
    #[inline]
    pub fn arguments(&self) -> Option<&DB::Arguments> {
        self.arguments.as_ref()
    }

    /// A mutable reference to the bind arguments of the query.
    #[inline]
    pub fn arguments_mut(&mut self) -> &mut Option<DB::Arguments> {
        &mut self.arguments
    }

    /// The number of bind arguments, if the request has arguments.
    #[inline]
    pub fn arguments_len(&self) -> Option<usize> {
        self.arguments.as_ref().map(Arguments::len)
    }

    /// If `true`, the prepared statement should be cached on the connection for reuse.
    #[inline]
    pub fn persistent(&self) -> bool {
        self.persistent
    }

    /// Set whether the prepared statement should be cached on the connection for reuse.
    #[inline]
    pub fn set_persistent(&mut self, persistent: bool) {
        self.persistent = persistent;
    }

    /// The previously prepared statement this request was created from, if any.
    #[inline]
    pub fn statement(&self) -> Option<&DB::Statement> {
        self.statement.as_ref()
    }

    /// The parameter types given for a [`RequestKind::Prepare`] request.
    #[inline]
    pub fn parameter_types(&self) -> &[DB::TypeInfo] {
        &self.parameter_types
    }

    /// Arbitrary values attached to this request by middleware or the caller.
    #[inline]
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// A mutable reference to the [extensions][Self::extensions] of this request.
    #[inline]
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.extensions
    }

    /// Information about the database the request is sent to, from the connect options of the
    /// connection or pool.
    ///
    /// This is set by the connection (or pool) the request is sent to, before it enters the
    /// middleware stack. It is `None` for requests which were not (yet) sent through a connection
    /// or pool.
    #[inline]
    pub fn connection_info(&self) -> Option<&ConnectionInfo> {
        self.connection_info.as_deref()
    }

    /// Set the information about the database the request is sent to.
    ///
    /// Called by connections and pools; this is not normally needed otherwise.
    #[doc(hidden)]
    #[inline]
    pub fn set_connection_info(&mut self, connection_info: Arc<ConnectionInfo>) {
        self.connection_info = Some(connection_info);
    }

    /// The transaction depth of the connection when the request was made: `0` if not in a
    /// transaction, `1` in a transaction, and `2` or more within savepoints.
    ///
    /// For example, middleware which retries failed queries should not retry
    /// within a transaction, as the failure likely aborted the transaction.
    ///
    /// Requests sent through a [`Pool`][crate::pool::Pool] are never in a transaction
    /// when they enter the pool's middleware stack.
    #[inline]
    pub fn transaction_depth(&self) -> usize {
        self.transaction_depth
    }

    /// Returns `true` if [`transaction_depth()`][Self::transaction_depth] is nonzero.
    #[inline]
    pub fn in_transaction(&self) -> bool {
        self.transaction_depth != 0
    }

    /// Set the transaction depth of the connection the request is sent to.
    ///
    /// Called by connections; this is not normally needed otherwise.
    #[doc(hidden)]
    #[inline]
    pub fn set_transaction_depth(&mut self, transaction_depth: usize) {
        self.transaction_depth = transaction_depth;
    }

    /// Deconstruct the request into its parts.
    pub fn into_parts(self) -> RequestParts<DB> {
        RequestParts {
            kind: self.kind,
            sql: self.sql,
            arguments: self.arguments,
            persistent: self.persistent,
            statement: self.statement,
            parameter_types: self.parameter_types,
            extensions: self.extensions,
            connection_info: self.connection_info,
            transaction_depth: self.transaction_depth,
        }
    }

    /// Reassemble a request from its parts.
    pub fn from_parts(parts: RequestParts<DB>) -> Self {
        Self {
            kind: parts.kind,
            sql: parts.sql,
            arguments: parts.arguments,
            persistent: parts.persistent,
            statement: parts.statement,
            parameter_types: parts.parameter_types,
            extensions: parts.extensions,
            connection_info: parts.connection_info,
            transaction_depth: parts.transaction_depth,
        }
    }
}

impl<DB: Database> Clone for Request<DB>
where
    DB::Arguments: Clone,
{
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            sql: self.sql.clone(),
            arguments: self.arguments.clone(),
            persistent: self.persistent,
            statement: self.statement.clone(),
            parameter_types: self.parameter_types.clone(),
            extensions: self.extensions.clone(),
            connection_info: self.connection_info.clone(),
            transaction_depth: self.transaction_depth,
        }
    }
}

impl<DB: Database> Debug for Request<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request")
            .field("kind", &self.kind)
            .field("sql", &self.sql.as_str())
            .field("arguments", &self.arguments_len())
            .field("persistent", &self.persistent)
            .field("statement", &self.statement.is_some())
            .field("parameter_types", &self.parameter_types)
            .field("extensions", &self.extensions)
            .field("connection_info", &self.connection_info)
            .field("transaction_depth", &self.transaction_depth)
            .finish()
    }
}

/// Executing a `Request` through [`Executor`][crate::executor::Executor] methods like `fetch_all()`
/// uses its SQL, arguments, statement and extensions; the [kind][Request::kind] is
/// decided by the method called.
impl<DB: Database> Execute<'_, DB> for Request<DB> {
    #[inline]
    fn sql(self) -> SqlStr {
        self.sql
    }

    #[inline]
    fn statement(&self) -> Option<&DB::Statement> {
        self.statement.as_ref()
    }

    #[inline]
    fn take_arguments(&mut self) -> Result<Option<DB::Arguments>, BoxDynError> {
        Ok(self.arguments.take())
    }

    #[inline]
    fn persistent(&self) -> bool {
        self.persistent
    }

    #[inline]
    fn take_extensions(&mut self) -> Extensions {
        std::mem::take(&mut self.extensions)
    }
}
