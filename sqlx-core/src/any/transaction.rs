use crate::any::{Any, AnyConnection};
use crate::database::Database;
use crate::error::Error;
use crate::executor::Executor;
use crate::middleware::{Request, RequestKind};
use crate::sql_str::SqlStr;
use crate::transaction::{check_transaction_depth, TransactionManager};

pub struct AnyTransactionManager;

impl TransactionManager for AnyTransactionManager {
    type Database = Any;

    async fn begin(conn: &mut AnyConnection, statement: Option<SqlStr>) -> Result<(), Error> {
        let depth = conn.backend.get_transaction_depth();
        conn.dispatch(Request::begin(depth, statement)?).await?;
        check_transaction_depth(
            RequestKind::Begin,
            depth,
            conn.backend.get_transaction_depth(),
        )
    }

    async fn commit(conn: &mut AnyConnection) -> Result<(), Error> {
        let depth = conn.backend.get_transaction_depth();

        if depth > 0 {
            conn.dispatch(Request::commit(depth)).await?;
            check_transaction_depth(
                RequestKind::Commit,
                depth,
                conn.backend.get_transaction_depth(),
            )?;
        }

        Ok(())
    }

    async fn rollback(conn: &mut AnyConnection) -> Result<(), Error> {
        let depth = conn.backend.get_transaction_depth();

        if depth > 0 {
            conn.dispatch(Request::rollback(depth)).await?;
            check_transaction_depth(
                RequestKind::Rollback,
                depth,
                conn.backend.get_transaction_depth(),
            )?;
        }

        Ok(())
    }

    fn start_rollback(conn: &mut AnyConnection) {
        conn.backend.start_rollback()
    }

    fn get_transaction_depth(conn: &<Self::Database as Database>::Connection) -> usize {
        conn.backend.get_transaction_depth()
    }
}
