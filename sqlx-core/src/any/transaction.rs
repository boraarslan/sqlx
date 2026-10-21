use crate::any::{Any, AnyConnection};
use crate::database::Database;
use crate::error::Error;
use crate::executor::Executor;
use crate::middleware::Request;
use crate::sql_str::SqlStr;
use crate::transaction::TransactionManager;

pub struct AnyTransactionManager;

impl TransactionManager for AnyTransactionManager {
    type Database = Any;

    async fn begin(conn: &mut AnyConnection, statement: Option<SqlStr>) -> Result<(), Error> {
        let request = Request::begin(conn.backend.get_transaction_depth(), statement)?;
        conn.dispatch(request).await?;
        Ok(())
    }

    async fn commit(conn: &mut AnyConnection) -> Result<(), Error> {
        let depth = conn.backend.get_transaction_depth();

        if depth > 0 {
            conn.dispatch(Request::commit(depth)).await?;
        }

        Ok(())
    }

    async fn rollback(conn: &mut AnyConnection) -> Result<(), Error> {
        let depth = conn.backend.get_transaction_depth();

        if depth > 0 {
            conn.dispatch(Request::rollback(depth)).await?;
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
