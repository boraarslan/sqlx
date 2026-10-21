use sqlx_core::database::Database;
use sqlx_core::middleware::Request;
use sqlx_core::sql_str::SqlStr;

use crate::error::Error;
use crate::executor::Executor;

use crate::{PgConnection, Postgres};

pub(crate) use sqlx_core::transaction::*;

/// Implementation of [`TransactionManager`] for PostgreSQL.
pub struct PgTransactionManager;

impl TransactionManager for PgTransactionManager {
    type Database = Postgres;

    async fn begin(conn: &mut PgConnection, statement: Option<SqlStr>) -> Result<(), Error> {
        let request = Request::begin(conn.transaction_depth(), statement)?;
        conn.dispatch(request).await?;
        Ok(())
    }

    async fn commit(conn: &mut PgConnection) -> Result<(), Error> {
        let depth = conn.transaction_depth();

        if depth > 0 {
            conn.dispatch(Request::commit(depth)).await?;
        }

        Ok(())
    }

    async fn rollback(conn: &mut PgConnection) -> Result<(), Error> {
        let depth = conn.transaction_depth();

        if depth > 0 {
            conn.dispatch(Request::rollback(depth)).await?;
        }

        Ok(())
    }

    fn start_rollback(conn: &mut PgConnection) {
        // The connection can only be locked by a response stream that middleware kept alive;
        // in that case, the rollback is queued when the connection is next locked.
        let mut locked = conn.try_lock();
        conn.state.start_rollback(locked.as_deref_mut());
    }

    fn get_transaction_depth(conn: &<Self::Database as Database>::Connection) -> usize {
        conn.transaction_depth()
    }
}
