use sqlx_core::middleware::{Request, RequestKind};
use sqlx_core::sql_str::SqlStr;

use crate::error::Error;
use crate::executor::Executor;
use crate::{MySql, MySqlConnection};

pub(crate) use sqlx_core::transaction::*;

/// Implementation of [`TransactionManager`] for MySQL.
pub struct MySqlTransactionManager;

impl TransactionManager for MySqlTransactionManager {
    type Database = MySql;

    async fn begin(conn: &mut MySqlConnection, statement: Option<SqlStr>) -> Result<(), Error> {
        let depth = conn.transaction_depth();
        conn.dispatch(Request::begin(depth, statement)?).await?;
        check_transaction_depth(RequestKind::Begin, depth, conn.transaction_depth())
    }

    async fn commit(conn: &mut MySqlConnection) -> Result<(), Error> {
        let depth = conn.transaction_depth();

        if depth > 0 {
            conn.dispatch(Request::commit(depth)).await?;
            check_transaction_depth(RequestKind::Commit, depth, conn.transaction_depth())?;
        }

        Ok(())
    }

    async fn rollback(conn: &mut MySqlConnection) -> Result<(), Error> {
        let depth = conn.transaction_depth();

        if depth > 0 {
            conn.dispatch(Request::rollback(depth)).await?;
            check_transaction_depth(RequestKind::Rollback, depth, conn.transaction_depth())?;
        }

        Ok(())
    }

    fn start_rollback(conn: &mut MySqlConnection) {
        // The connection can only be locked by a response stream that middleware kept alive;
        // in that case, the rollback is queued when the connection is next locked.
        let mut locked = conn.try_lock();
        conn.state.start_rollback(locked.as_deref_mut());
    }

    fn get_transaction_depth(conn: &MySqlConnection) -> usize {
        conn.transaction_depth()
    }
}
