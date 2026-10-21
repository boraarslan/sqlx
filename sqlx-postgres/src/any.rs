use crate::connection::PgDriver;
use crate::{
    PgColumn, PgConnectOptions, PgConnection, PgQueryResult, PgRow, PgStatement,
    PgTransactionManager, PgTypeInfo, Postgres,
};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;

use sqlx_core::any::{
    Any, AnyArguments, AnyColumn, AnyConnectOptions, AnyConnectionBackend, AnyQueryResult, AnyRow,
    AnyServiceAdapter, AnyTypeInfo, AnyTypeInfoKind,
};
use sqlx_core::middleware::BoxService;

use crate::type_info::PgType;
use sqlx_core::connection::Connection;
use sqlx_core::database::Database;
use sqlx_core::ext::ustr::UStr;
use sqlx_core::transaction::TransactionManager;

sqlx_core::declare_driver_with_optional_migrate!(DRIVER = Postgres);

impl AnyConnectionBackend for PgConnection {
    fn name(&self) -> &str {
        <Postgres as Database>::NAME
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close(*self).boxed()
    }

    fn close_hard(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close_hard(*self).boxed()
    }

    fn ping(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::ping(self).boxed()
    }

    fn service(&mut self) -> BoxService<Any> {
        BoxService::new(AnyServiceAdapter::new(
            PgDriver {
                conn: self.conn.clone(),
                state: self.state.clone(),
            },
            AnyArguments::convert_into,
            |statement: &PgStatement| statement.metadata.column_names.clone(),
        ))
    }

    fn start_rollback(&mut self) {
        PgTransactionManager::start_rollback(self)
    }

    fn get_transaction_depth(&self) -> usize {
        PgTransactionManager::get_transaction_depth(self)
    }

    fn shrink_buffers(&mut self) {
        Connection::shrink_buffers(self);
    }

    fn flush(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::flush(self).boxed()
    }

    fn should_flush(&self) -> bool {
        Connection::should_flush(self)
    }

    #[cfg(feature = "migrate")]
    fn as_migrate(
        &mut self,
    ) -> sqlx_core::Result<&mut (dyn sqlx_core::migrate::Migrate + Send + 'static)> {
        Ok(self)
    }
}

impl<'a> TryFrom<&'a PgTypeInfo> for AnyTypeInfo {
    type Error = sqlx_core::Error;

    fn try_from(pg_type: &'a PgTypeInfo) -> Result<Self, Self::Error> {
        Ok(AnyTypeInfo {
            kind: match &pg_type.0 {
                PgType::Bool => AnyTypeInfoKind::Bool,
                PgType::Void => AnyTypeInfoKind::Null,
                PgType::Int2 => AnyTypeInfoKind::SmallInt,
                PgType::Int4 => AnyTypeInfoKind::Integer,
                PgType::Int8 => AnyTypeInfoKind::BigInt,
                PgType::Float4 => AnyTypeInfoKind::Real,
                PgType::Float8 => AnyTypeInfoKind::Double,
                PgType::Bytea => AnyTypeInfoKind::Blob,
                PgType::Text | PgType::Varchar => AnyTypeInfoKind::Text,
                PgType::DeclareWithName(UStr::Static("citext")) => AnyTypeInfoKind::Text,
                _ => {
                    return Err(sqlx_core::Error::AnyDriverError(
                        format!("Any driver does not support the Postgres type {pg_type:?}").into(),
                    ))
                }
            },
        })
    }
}

impl<'a> TryFrom<&'a PgColumn> for AnyColumn {
    type Error = sqlx_core::Error;

    fn try_from(col: &'a PgColumn) -> Result<Self, Self::Error> {
        let type_info =
            AnyTypeInfo::try_from(&col.type_info).map_err(|e| sqlx_core::Error::ColumnDecode {
                index: col.name.to_string(),
                source: e.into(),
            })?;

        Ok(AnyColumn {
            ordinal: col.ordinal,
            name: col.name.clone(),
            type_info,
        })
    }
}

impl<'a> TryFrom<&'a PgRow> for AnyRow {
    type Error = sqlx_core::Error;

    fn try_from(row: &'a PgRow) -> Result<Self, Self::Error> {
        AnyRow::map_from(row, row.metadata.column_names.clone())
    }
}

impl<'a> TryFrom<&'a AnyConnectOptions> for PgConnectOptions {
    type Error = sqlx_core::Error;

    fn try_from(value: &'a AnyConnectOptions) -> Result<Self, Self::Error> {
        let mut opts = PgConnectOptions::parse_from_url(&value.database_url)?;
        opts.log_settings = value.log_settings.clone();
        Ok(opts)
    }
}

impl From<PgQueryResult> for AnyQueryResult {
    fn from(done: PgQueryResult) -> Self {
        AnyQueryResult {
            rows_affected: done.rows_affected(),
            last_insert_id: None,
        }
    }
}
