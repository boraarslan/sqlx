use crate::connection::MySqlDriver;
use crate::protocol::text::ColumnType;
use crate::{
    MySql, MySqlColumn, MySqlConnectOptions, MySqlConnection, MySqlQueryResult, MySqlRow,
    MySqlStatement, MySqlTransactionManager, MySqlTypeInfo,
};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use sqlx_core::any::{
    Any, AnyArguments, AnyColumn, AnyConnectOptions, AnyConnectionBackend, AnyQueryResult, AnyRow,
    AnyServiceAdapter, AnyTypeInfo, AnyTypeInfoKind,
};
use sqlx_core::connection::Connection;
use sqlx_core::database::Database;
use sqlx_core::middleware::BoxService;
use sqlx_core::transaction::TransactionManager;
use sqlx_core::types::Type;

sqlx_core::declare_driver_with_optional_migrate!(DRIVER = MySql);

impl AnyConnectionBackend for MySqlConnection {
    fn name(&self) -> &str {
        <MySql as Database>::NAME
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
            MySqlDriver {
                conn: self.conn.clone(),
                state: self.state.clone(),
            },
            AnyArguments::convert_into,
            |statement: &MySqlStatement| statement.metadata.column_names.clone(),
        ))
    }

    fn start_rollback(&mut self) {
        MySqlTransactionManager::start_rollback(self)
    }

    fn get_transaction_depth(&self) -> usize {
        MySqlTransactionManager::get_transaction_depth(self)
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

impl<'a> TryFrom<&'a MySqlTypeInfo> for AnyTypeInfo {
    type Error = sqlx_core::Error;

    fn try_from(type_info: &'a MySqlTypeInfo) -> Result<Self, Self::Error> {
        Ok(AnyTypeInfo {
            kind: match &type_info.r#type {
                ColumnType::Null => AnyTypeInfoKind::Null,
                ColumnType::Short => AnyTypeInfoKind::SmallInt,
                ColumnType::Long => AnyTypeInfoKind::Integer,
                ColumnType::LongLong => AnyTypeInfoKind::BigInt,
                ColumnType::Float => AnyTypeInfoKind::Real,
                ColumnType::Double => AnyTypeInfoKind::Double,
                // Checks for any applicable type and compatible collations
                _ if <str as Type<MySql>>::compatible(type_info) => AnyTypeInfoKind::Text,
                _ if <[u8] as Type<MySql>>::compatible(type_info) => AnyTypeInfoKind::Blob,
                _ => {
                    return Err(sqlx_core::Error::AnyDriverError(
                        format!("Any driver does not support MySql type {type_info:?}").into(),
                    ))
                }
            },
        })
    }
}

impl<'a> TryFrom<&'a MySqlColumn> for AnyColumn {
    type Error = sqlx_core::Error;

    fn try_from(column: &'a MySqlColumn) -> Result<Self, Self::Error> {
        let type_info = AnyTypeInfo::try_from(&column.type_info)?;

        Ok(AnyColumn {
            ordinal: column.ordinal,
            name: column.name.clone(),
            type_info,
        })
    }
}

impl<'a> TryFrom<&'a MySqlRow> for AnyRow {
    type Error = sqlx_core::Error;

    fn try_from(row: &'a MySqlRow) -> Result<Self, Self::Error> {
        AnyRow::map_from(row, row.column_names.clone())
    }
}

impl<'a> TryFrom<&'a AnyConnectOptions> for MySqlConnectOptions {
    type Error = sqlx_core::Error;

    fn try_from(any_opts: &'a AnyConnectOptions) -> Result<Self, Self::Error> {
        let mut opts = Self::parse_from_url(&any_opts.database_url)?;
        opts.log_settings = any_opts.log_settings.clone();
        Ok(opts)
    }
}

/// This conversion attempts to save last_insert_id by converting to i64.
impl From<MySqlQueryResult> for AnyQueryResult {
    fn from(done: MySqlQueryResult) -> Self {
        AnyQueryResult {
            rows_affected: done.rows_affected(),
            last_insert_id: done.last_insert_id().try_into().ok(),
        }
    }
}
