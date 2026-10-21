use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use futures_channel::oneshot;
use futures_intrusive::sync::{Mutex, MutexGuard};
use sqlx_core::sql_str::SqlStr;
use tracing::span::Span;

use sqlx_core::error::Error;
use sqlx_core::transaction::rollback_ansi_transaction_sql;
use sqlx_core::Either;

use crate::connection::establish::EstablishParams;
use crate::connection::execute;
use crate::connection::ConnectionState;
use crate::{SqliteArguments, SqliteQueryResult, SqliteRow, SqliteStatement};

#[cfg(feature = "deserialize")]
use crate::connection::deserialize::{deserialize, serialize, SchemaName, SqliteOwnedBuf};

// Each SQLite connection has a dedicated thread.

// TODO: Tweak this so that we can use a thread pool per pool of SQLite3 connections to reduce
//       OS resource usage. Low priority because a high concurrent load for SQLite3 is very
//       unlikely.

#[derive(Clone)]
pub(crate) struct ConnectionWorker {
    command_tx: flume::Sender<(Command, tracing::Span)>,
    /// Mutex for locking access to the database.
    pub(crate) shared: Arc<WorkerSharedState>,
}

pub(crate) struct WorkerSharedState {
    /// The transaction depth as seen by the connection.
    ///
    /// This is maintained by the sender of transaction commands rather than the worker,
    /// so that it's up-to-date as soon as a `ROLLBACK` is queued by `start_rollback()`.
    transaction_depth: AtomicUsize,
    cached_statements_size: AtomicUsize,
    pub(crate) conn: Mutex<ConnectionState>,
}

impl WorkerSharedState {
    pub(crate) fn get_transaction_depth(&self) -> usize {
        self.transaction_depth.load(Ordering::Acquire)
    }

    fn increment_transaction_depth(&self) {
        self.transaction_depth.fetch_add(1, Ordering::AcqRel);
    }

    fn decrement_transaction_depth(&self) {
        let _ = self
            .transaction_depth
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |depth| {
                depth.checked_sub(1)
            });
    }

    pub(crate) fn get_cached_statements_size(&self) -> usize {
        self.cached_statements_size.load(Ordering::Acquire)
    }
}

enum Command {
    Prepare {
        query: SqlStr,
        tx: oneshot::Sender<Result<SqliteStatement, Error>>,
    },
    #[cfg(feature = "offline")]
    Describe {
        query: SqlStr,
        tx: oneshot::Sender<Result<sqlx_core::describe::Describe<crate::Sqlite>, Error>>,
    },
    Execute {
        query: SqlStr,
        arguments: Option<SqliteArguments>,
        persistent: bool,
        tx: flume::Sender<Result<Either<SqliteQueryResult, SqliteRow>, Error>>,
        limit: Option<usize>,
    },
    #[cfg(feature = "deserialize")]
    Serialize {
        schema: Option<SchemaName>,
        tx: oneshot::Sender<Result<SqliteOwnedBuf, Error>>,
    },
    #[cfg(feature = "deserialize")]
    Deserialize {
        schema: Option<SchemaName>,
        data: SqliteOwnedBuf,
        read_only: bool,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Begin {
        tx: rendezvous_oneshot::Sender<Result<(), Error>>,
        statement: SqlStr,
        /// The transaction depth before this statement.
        depth: usize,
    },
    Commit {
        tx: rendezvous_oneshot::Sender<Result<(), Error>>,
        statement: SqlStr,
    },
    Rollback {
        tx: Option<rendezvous_oneshot::Sender<Result<(), Error>>>,
        statement: SqlStr,
    },
    UnlockDb,
    ClearCache {
        tx: oneshot::Sender<()>,
    },
    Ping {
        tx: oneshot::Sender<()>,
    },
    Shutdown {
        tx: oneshot::Sender<()>,
    },
}

impl ConnectionWorker {
    pub(crate) async fn establish(params: EstablishParams) -> Result<Self, Error> {
        let (establish_tx, establish_rx) = oneshot::channel();

        thread::Builder::new()
            .name(params.thread_name.clone())
            .spawn(move || {
                let (command_tx, command_rx) = flume::bounded(params.command_channel_size);

                let conn = match params.establish() {
                    Ok(conn) => conn,
                    Err(e) => {
                        establish_tx.send(Err(e)).ok();
                        return;
                    }
                };

                let shared = Arc::new(WorkerSharedState {
                    transaction_depth: AtomicUsize::new(0),
                    cached_statements_size: AtomicUsize::new(0),
                    // note: must be fair because in `Command::UnlockDb` we unlock the mutex
                    // and then immediately try to relock it; an unfair mutex would immediately
                    // grant us the lock even if another task is waiting.
                    conn: Mutex::new(conn, true),
                });
                let mut conn = shared.conn.try_lock().unwrap();

                if establish_tx
                    .send(Ok(Self {
                        command_tx,
                        shared: Arc::clone(&shared),
                    }))
                    .is_err()
                {
                    return;
                }

                // If COMMIT or ROLLBACK is processed but not acknowledged, there would be another
                // ROLLBACK sent when the `Transaction` drops. We need to ignore it otherwise we
                // would rollback an already completed transaction.
                let mut ignore_next_start_rollback = false;

                for (cmd, span) in command_rx {
                    let _guard = span.enter();
                    match cmd {
                        Command::Prepare { query, tx } => {
                            tx.send(prepare(&mut conn, query)).ok();

                            // This may issue an unnecessary write on failure,
                            // but it doesn't matter in the grand scheme of things.
                            update_cached_statements_size(
                                &conn,
                                &shared.cached_statements_size,
                            );
                        }
                        #[cfg(feature = "offline")]
                        Command::Describe { query, tx } => {
                            tx.send(crate::connection::describe::describe(&mut conn, query)).ok();
                        }
                        Command::Execute {
                            query,
                            arguments,
                            persistent,
                            tx,
                            limit
                        } => {
                            let iter = match execute::iter(&mut conn, query, arguments, persistent)
                            {
                                Ok(iter) => iter,
                                Err(e) => {
                                    tx.send(Err(e)).ok();
                                    continue;
                                }
                            };

                            match limit {
                                None => {
                                    for res in iter {
                                        let has_error = res.is_err();
                                        if tx.send(res).is_err() || has_error {
                                            break;
                                        }
                                    }
                                },
                                Some(limit) => {
                                    let mut iter = iter;
                                    let mut rows_returned = 0;

                                    while let Some(res) = iter.next() {
                                        if let Ok(ok) = &res {
                                            if ok.is_right() {
                                                rows_returned += 1;
                                                if rows_returned >= limit {
                                                    drop(iter);
                                                    let _ = tx.send(res);
                                                    break;
                                                }
                                            }
                                        }
                                        let has_error = res.is_err();
                                        if tx.send(res).is_err() || has_error {
                                            break;
                                        }
                                    }
                                },
                            }

                            update_cached_statements_size(&conn, &shared.cached_statements_size);
                        }
                        Command::Begin { tx, statement, depth } => {
                            let res = conn.handle.exec(statement.as_str()).and_then(|res| {
                                if !conn.handle.in_transaction() {
                                    return Err(Error::BeginFailed);
                                }

                                Ok(res)
                            });
                            let res_ok = res.is_ok();

                            if tx.blocking_send(res).is_err() && res_ok {
                                // The BEGIN was processed but not acknowledged. This means no
                                // `Transaction` was created and so there is no way to commit /
                                // rollback this transaction. We need to roll it back
                                // immediately otherwise it would remain started forever.
                                if let Err(error) = conn
                                    .handle
                                    .exec(rollback_ansi_transaction_sql(depth + 1).as_str())
                                {
                                    // The rollback failed. To prevent leaving the connection
                                    // in an inconsistent state we shutdown this worker which
                                    // causes any subsequent operation on the connection to fail.
                                    tracing::error!(%error, "failed to rollback cancelled transaction");
                                    break;
                                }
                            }
                        }
                        Command::Commit { tx, statement } => {
                            let res = conn.handle.exec(statement.as_str()).map(|_| ());
                            let res_ok = res.is_ok();

                            if tx.blocking_send(res).is_err() && res_ok {
                                // The COMMIT was processed but not acknowledged. This means that
                                // the `Transaction` doesn't know it was committed and will try to
                                // rollback on drop. We need to ignore that rollback.
                                ignore_next_start_rollback = true;
                            }
                        }
                        Command::Rollback { tx, statement } => {
                            if ignore_next_start_rollback && tx.is_none() {
                                ignore_next_start_rollback = false;
                                continue;
                            }

                            let res = conn.handle.exec(statement.as_str()).map(|_| ());
                            let res_ok = res.is_ok();

                            if let Some(tx) = tx {
                                if tx.blocking_send(res).is_err() && res_ok {
                                    // The ROLLBACK was processed but not acknowledged. This means
                                    // that the `Transaction` doesn't know it was rolled back and
                                    // will try to rollback again on drop. We need to ignore that
                                    // rollback.
                                    ignore_next_start_rollback = true;
                                }
                            }
                        }
                        #[cfg(feature = "deserialize")]
                        Command::Serialize { schema, tx } => {
                            tx.send(serialize(&mut conn, schema)).ok();
                        }
                        #[cfg(feature = "deserialize")]
                        Command::Deserialize { schema, data, read_only, tx } => {
                            tx.send(deserialize(&mut conn, schema, data, read_only)).ok();
                        }
                        Command::ClearCache { tx } => {
                            conn.statements.clear();
                            update_cached_statements_size(&conn, &shared.cached_statements_size);
                            tx.send(()).ok();
                        }
                        Command::UnlockDb => {
                            drop(conn);
                            conn = futures_executor::block_on(shared.conn.lock());
                        }
                        Command::Ping { tx } => {
                            tx.send(()).ok();
                        }
                        Command::Shutdown { tx } => {
                            // drop the connection references before sending confirmation
                            // and ending the command loop
                            drop(conn);
                            drop(shared);
                            let _ = tx.send(());
                            return;
                        }
                    }
                }
            })?;

        establish_rx.await.map_err(|_| Error::WorkerCrashed)?
    }

    pub(crate) async fn prepare(&self, query: SqlStr) -> Result<SqliteStatement, Error> {
        self.oneshot_cmd(|tx| Command::Prepare { query, tx })
            .await?
    }

    #[cfg(feature = "offline")]
    pub(crate) async fn describe(
        &self,
        query: SqlStr,
    ) -> Result<sqlx_core::describe::Describe<crate::Sqlite>, Error> {
        self.oneshot_cmd(|tx| Command::Describe { query, tx })
            .await?
    }

    pub(crate) async fn execute(
        &self,
        query: SqlStr,
        args: Option<SqliteArguments>,
        chan_size: usize,
        persistent: bool,
        limit: Option<usize>,
    ) -> Result<flume::Receiver<Result<Either<SqliteQueryResult, SqliteRow>, Error>>, Error> {
        let (tx, rx) = flume::bounded(chan_size);

        self.command_tx
            .send_async((
                Command::Execute {
                    query,
                    arguments: args,
                    persistent,
                    tx,
                    limit,
                },
                Span::current(),
            ))
            .await
            .map_err(|_| Error::WorkerCrashed)?;

        Ok(rx)
    }

    /// Execute a statement which begins a transaction or creates a savepoint.
    pub(crate) async fn begin(&self, statement: SqlStr) -> Result<(), Error> {
        let depth = self.shared.get_transaction_depth();

        self.oneshot_cmd_with_ack(|tx| Command::Begin {
            tx,
            statement,
            depth,
        })
        .await??;

        self.shared.increment_transaction_depth();

        Ok(())
    }

    /// Execute a statement which commits a transaction or releases a savepoint,
    /// if in a transaction.
    pub(crate) async fn commit(&self, statement: SqlStr) -> Result<(), Error> {
        if self.shared.get_transaction_depth() > 0 {
            self.oneshot_cmd_with_ack(|tx| Command::Commit { tx, statement })
                .await??;

            self.shared.decrement_transaction_depth();
        }

        Ok(())
    }

    /// Execute a statement which rolls back a transaction or to a savepoint,
    /// if in a transaction.
    pub(crate) async fn rollback(&self, statement: SqlStr) -> Result<(), Error> {
        if self.shared.get_transaction_depth() > 0 {
            self.oneshot_cmd_with_ack(|tx| Command::Rollback {
                tx: Some(tx),
                statement,
            })
            .await??;

            self.shared.decrement_transaction_depth();
        }

        Ok(())
    }

    /// Queue a rollback of the current transaction or savepoint, if any, without waiting for it.
    pub(crate) fn start_rollback(&self) -> Result<(), Error> {
        let depth = self.shared.get_transaction_depth();

        if depth > 0 {
            self.command_tx
                .send((
                    Command::Rollback {
                        tx: None,
                        statement: rollback_ansi_transaction_sql(depth),
                    },
                    Span::current(),
                ))
                .map_err(|_| Error::WorkerCrashed)?;

            self.shared.decrement_transaction_depth();
        }

        Ok(())
    }

    pub(crate) async fn ping(&self) -> Result<(), Error> {
        self.oneshot_cmd(|tx| Command::Ping { tx }).await
    }

    #[cfg(feature = "deserialize")]
    pub(crate) async fn deserialize(
        &self,
        schema: Option<SchemaName>,
        data: SqliteOwnedBuf,
        read_only: bool,
    ) -> Result<(), Error> {
        self.oneshot_cmd(|tx| Command::Deserialize {
            schema,
            data,
            read_only,
            tx,
        })
        .await?
    }

    #[cfg(feature = "deserialize")]
    pub(crate) async fn serialize(
        &self,
        schema: Option<SchemaName>,
    ) -> Result<SqliteOwnedBuf, Error> {
        self.oneshot_cmd(|tx| Command::Serialize { schema, tx })
            .await?
    }

    async fn oneshot_cmd<F, T>(&self, command: F) -> Result<T, Error>
    where
        F: FnOnce(oneshot::Sender<T>) -> Command,
    {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send_async((command(tx), Span::current()))
            .await
            .map_err(|_| Error::WorkerCrashed)?;

        rx.await.map_err(|_| Error::WorkerCrashed)
    }

    async fn oneshot_cmd_with_ack<F, T>(&self, command: F) -> Result<T, Error>
    where
        F: FnOnce(rendezvous_oneshot::Sender<T>) -> Command,
    {
        let (tx, rx) = rendezvous_oneshot::channel();

        self.command_tx
            .send_async((command(tx), Span::current()))
            .await
            .map_err(|_| Error::WorkerCrashed)?;

        rx.recv().await.map_err(|_| Error::WorkerCrashed)
    }

    pub(crate) async fn clear_cache(&self) -> Result<(), Error> {
        self.oneshot_cmd(|tx| Command::ClearCache { tx }).await
    }

    pub(crate) async fn unlock_db(&mut self) -> Result<MutexGuard<'_, ConnectionState>, Error> {
        let (guard, res) = futures_util::future::join(
            // we need to join the wait queue for the lock before we send the message
            self.shared.conn.lock(),
            self.command_tx
                .send_async((Command::UnlockDb, Span::current())),
        )
        .await;

        res.map_err(|_| Error::WorkerCrashed)?;

        Ok(guard)
    }

    /// Send a command to the worker to shut down the processing thread.
    ///
    /// A `WorkerCrashed` error may be returned if the thread has already stopped.
    pub(crate) fn shutdown(&mut self) -> impl Future<Output = Result<(), Error>> {
        let (tx, rx) = oneshot::channel();

        let send_res = self
            .command_tx
            .send((Command::Shutdown { tx }, Span::current()))
            .map_err(|_| Error::WorkerCrashed);

        async move {
            send_res?;

            // wait for the response
            rx.await.map_err(|_| Error::WorkerCrashed)
        }
    }
}

fn prepare(conn: &mut ConnectionState, query: SqlStr) -> Result<SqliteStatement, Error> {
    // prepare statement object (or checkout from cache)
    let statement = conn.statements.get(query.as_str(), true)?;

    let mut parameters = 0;
    let mut columns = None;
    let mut column_names = None;

    while let Some(statement) = statement.prepare_next(&mut conn.handle)? {
        parameters += statement.handle.bind_parameter_count();

        // the first non-empty statement is chosen as the statement we pull columns from
        if !statement.columns.is_empty() && columns.is_none() {
            columns = Some(Arc::clone(statement.columns));
            column_names = Some(Arc::clone(statement.column_names));
        }
    }

    Ok(SqliteStatement {
        sql: query,
        columns: columns.unwrap_or_default(),
        column_names: column_names.unwrap_or_default(),
        parameters,
    })
}

fn update_cached_statements_size(conn: &ConnectionState, size: &AtomicUsize) {
    size.store(conn.statements.len(), Ordering::Release);
}

// A oneshot channel where send completes only after the receiver receives the value.
mod rendezvous_oneshot {
    use super::oneshot::{self, Canceled};

    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        let (inner_tx, inner_rx) = oneshot::channel();
        (Sender { inner: inner_tx }, Receiver { inner: inner_rx })
    }

    pub struct Sender<T> {
        inner: oneshot::Sender<(T, oneshot::Sender<()>)>,
    }

    impl<T> Sender<T> {
        pub async fn send(self, value: T) -> Result<(), Canceled> {
            let (ack_tx, ack_rx) = oneshot::channel();
            self.inner.send((value, ack_tx)).map_err(|_| Canceled)?;
            ack_rx.await
        }

        pub fn blocking_send(self, value: T) -> Result<(), Canceled> {
            futures_executor::block_on(self.send(value))
        }
    }

    pub struct Receiver<T> {
        inner: oneshot::Receiver<(T, oneshot::Sender<()>)>,
    }

    impl<T> Receiver<T> {
        pub async fn recv(self) -> Result<T, Canceled> {
            let (value, ack_tx) = self.inner.await?;
            ack_tx.send(()).map_err(|_| Canceled)?;
            Ok(value)
        }
    }
}
