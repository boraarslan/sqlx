use sqlx_core::executor::Executor;
use sqlx_core::middleware::{Request, RequestKind};
use sqlx_core::transaction::{check_transaction_depth, TransactionManager};
use sqlx_core::{error::Error, sql_str::SqlStr};

use crate::{Sqlite, SqliteConnection};

/// Implementation of [`TransactionManager`] for SQLite.
pub struct SqliteTransactionManager;

impl TransactionManager for SqliteTransactionManager {
    type Database = Sqlite;

    async fn begin(conn: &mut SqliteConnection, statement: Option<SqlStr>) -> Result<(), Error> {
        let depth = conn.worker.shared.get_transaction_depth();
        conn.dispatch(Request::begin(depth, statement)?).await?;
        check_transaction_depth(
            RequestKind::Begin,
            depth,
            conn.worker.shared.get_transaction_depth(),
        )
    }

    async fn commit(conn: &mut SqliteConnection) -> Result<(), Error> {
        let depth = conn.worker.shared.get_transaction_depth();

        if depth > 0 {
            conn.dispatch(Request::commit(depth)).await?;
            check_transaction_depth(
                RequestKind::Commit,
                depth,
                conn.worker.shared.get_transaction_depth(),
            )?;
        }

        Ok(())
    }

    async fn rollback(conn: &mut SqliteConnection) -> Result<(), Error> {
        let depth = conn.worker.shared.get_transaction_depth();

        if depth > 0 {
            conn.dispatch(Request::rollback(depth)).await?;
            check_transaction_depth(
                RequestKind::Rollback,
                depth,
                conn.worker.shared.get_transaction_depth(),
            )?;
        }

        Ok(())
    }

    fn start_rollback(conn: &mut SqliteConnection) {
        conn.worker.start_rollback().ok();
    }

    fn get_transaction_depth(conn: &SqliteConnection) -> usize {
        conn.worker.shared.get_transaction_depth()
    }
}
