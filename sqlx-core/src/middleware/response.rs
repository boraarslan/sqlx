use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

use either::Either;
use futures_core::stream::BoxStream;
use futures_core::Stream;
use futures_util::StreamExt;

use crate::database::Database;
use crate::describe::Describe;
use crate::error::Error;

/// The response to a [`Request`][super::Request].
///
/// Which variant is returned depends on the [`RequestKind`][super::RequestKind] of the request.
#[non_exhaustive]
pub enum Response<DB: Database> {
    /// The results of a [`Fetch`][super::RequestKind::Fetch] or
    /// [`FetchOptional`][super::RequestKind::FetchOptional] request.
    Stream(ResultStream<DB>),
    /// The statement prepared by a [`Prepare`][super::RequestKind::Prepare] request.
    Statement(DB::Statement),
    /// The response to a [`Describe`][super::RequestKind::Describe] request.
    #[doc(hidden)]
    Describe(Describe<DB>),
    /// The response to a transaction control request.
    Done,
}

impl<DB: Database> Response<DB> {
    /// Unwrap a [`Response::Stream`], or return an error if this is a different variant.
    pub fn into_stream(self) -> Result<ResultStream<DB>, Error> {
        match self {
            Self::Stream(stream) => Ok(stream),
            other => Err(other.unexpected("Stream")),
        }
    }

    /// Unwrap a [`Response::Statement`], or return an error if this is a different variant.
    pub fn into_statement(self) -> Result<DB::Statement, Error> {
        match self {
            Self::Statement(statement) => Ok(statement),
            other => Err(other.unexpected("Statement")),
        }
    }

    #[doc(hidden)]
    pub fn into_describe(self) -> Result<Describe<DB>, Error> {
        match self {
            Self::Describe(describe) => Ok(describe),
            other => Err(other.unexpected("Describe")),
        }
    }

    /// Transform the [`ResultStream`] of a [`Response::Stream`]; other variants are returned as-is.
    ///
    /// Useful for middleware which needs to observe the rows of a query or the end of its execution.
    pub fn map_stream<F>(self, f: F) -> Self
    where
        F: FnOnce(ResultStream<DB>) -> ResultStream<DB>,
    {
        match self {
            Self::Stream(stream) => Self::Stream(f(stream)),
            other => other,
        }
    }

    fn variant_name(&self) -> &'static str {
        match self {
            Self::Stream(_) => "Stream",
            Self::Statement(_) => "Statement",
            Self::Describe(_) => "Describe",
            Self::Done => "Done",
        }
    }

    fn unexpected(&self, expected: &str) -> Error {
        Error::Middleware(
            format!(
                "expected Response::{expected}, got Response::{}",
                self.variant_name()
            )
            .into(),
        )
    }
}

impl<DB: Database> Debug for Response<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stream(stream) => f.debug_tuple("Stream").field(stream).finish(),
            Self::Statement(_) => f.debug_tuple("Statement").finish_non_exhaustive(),
            Self::Describe(describe) => f.debug_tuple("Describe").field(describe).finish(),
            Self::Done => f.write_str("Done"),
        }
    }
}

/// A single item yielded by a [`ResultStream`].
pub type ResultItem<DB> =
    Result<Either<<DB as Database>::QueryResult, <DB as Database>::Row>, Error>;

/// The stream of results and rows produced by executing a query.
///
/// Yields `Either::Left(QueryResult)` at the end of each statement and `Either::Right(Row)`
/// for each row, exactly like [`Executor::fetch_many()`][crate::executor::Executor::fetch_many].
///
/// The stream is `'static`: drivers keep the connection reserved for this stream until it is
/// exhausted or dropped. Dropping the stream early is allowed; the remainder of the response
/// is discarded by the connection before the next request.
pub struct ResultStream<DB: Database> {
    // An item that was read ahead of time by `prefetch()`.
    first: Option<Either<DB::QueryResult, DB::Row>>,
    inner: Option<BoxStream<'static, ResultItem<DB>>>,
}

impl<DB: Database> ResultStream<DB> {
    /// Wrap a stream of results and rows.
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = ResultItem<DB>> + Send + 'static,
    {
        Self::from_boxed(Box::pin(stream))
    }

    /// Wrap an already-boxed stream of results and rows without boxing it again.
    pub fn from_boxed(stream: BoxStream<'static, ResultItem<DB>>) -> Self {
        Self {
            first: None,
            inner: Some(stream),
        }
    }

    /// A stream that yields nothing.
    pub fn empty() -> Self {
        Self {
            first: None,
            inner: None,
        }
    }

    /// Wait for the first item of the stream.
    ///
    /// If that item is an error, it's returned directly, otherwise it's kept and yielded again
    /// as the first item of the returned stream.
    ///
    /// Drivers use this so that the future of a [`Fetch`][super::RequestKind::Fetch] request
    /// completes only once the database has started responding, which makes middleware
    /// such as timeouts and retries meaningful for queries.
    pub async fn prefetch(mut self) -> Result<Self, Error> {
        if self.first.is_some() {
            return Ok(self);
        }

        let Some(inner) = &mut self.inner else {
            return Ok(self);
        };

        match inner.next().await {
            Some(Ok(first)) => {
                self.first = Some(first);
                Ok(self)
            }
            Some(Err(e)) => Err(e),
            None => Ok(Self::empty()),
        }
    }

    /// Box this stream as a [`BoxStream`].
    pub fn into_boxed(self) -> BoxStream<'static, ResultItem<DB>> {
        match self {
            Self {
                first: None,
                inner: Some(inner),
            } => inner,
            this => Box::pin(this),
        }
    }
}

// We never pin-project into `first`, and `inner` is already boxed and pinned.
impl<DB: Database> Unpin for ResultStream<DB> {}

impl<DB: Database> Stream for ResultStream<DB> {
    type Item = ResultItem<DB>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(first) = self.first.take() {
            return Poll::Ready(Some(Ok(first)));
        }

        let Some(inner) = &mut self.inner else {
            return Poll::Ready(None);
        };

        let res = inner.as_mut().poll_next(cx);

        if let Poll::Ready(None) = res {
            // Release the underlying stream (and the connection it holds) as soon as possible.
            self.inner = None;
        }

        res
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let first = usize::from(self.first.is_some());

        match &self.inner {
            Some(inner) => {
                let (lower, upper) = inner.size_hint();
                (
                    lower.saturating_add(first),
                    upper.and_then(|upper| upper.checked_add(first)),
                )
            }
            None => (first, Some(first)),
        }
    }
}

impl<DB: Database> Debug for ResultStream<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResultStream")
            .field("prefetched", &self.first.is_some())
            .field("finished", &self.inner.is_none())
            .finish()
    }
}
