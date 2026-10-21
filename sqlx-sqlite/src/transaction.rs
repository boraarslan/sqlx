use sqlx_core::executor::Executor;
use sqlx_core::middleware::Request;
use sqlx_core::transaction::TransactionManager;
use sqlx_core::{error::Error, sql_str::SqlStr};

use crate::{Sqlite, SqliteConnection};

/// Implementation of [`TransactionManager`] for SQLite.
pub struct SqliteTransactionManager;

impl TransactionManager for SqliteTransactionManager {
    type Database = Sqlite;

    async fn begin(conn: &mut SqliteConnection, statement: Option<SqlStr>) -> Result<(), Error> {
        let request = Request::begin(conn.worker.shared.get_transaction_depth(), statement)?;
        conn.dispatch(request).await?;
        Ok(())
    }

    async fn commit(conn: &mut SqliteConnection) -> Result<(), Error> {
        let depth = conn.worker.shared.get_transaction_depth();

        if depth > 0 {
            conn.dispatch(Request::commit(depth)).await?;
        }

        Ok(())
    }

    async fn rollback(conn: &mut SqliteConnection) -> Result<(), Error> {
        let depth = conn.worker.shared.get_transaction_depth();

        if depth > 0 {
            conn.dispatch(Request::rollback(depth)).await?;
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
