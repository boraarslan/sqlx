use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use sqlx::middleware::{self, FromFnLayer, Next, Request, RequestKind, Response, ResultStream};
use sqlx::sqlite::{Sqlite, SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Connection, Database, Error, Executor, Row, SqliteConnection};

#[derive(Debug, Clone, PartialEq)]
struct Recorded {
    kind: RequestKind,
    sql: String,
    transaction_depth: usize,
}

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Recorded>>>);

impl Recorder {
    fn layer<DB: Database>(
        &self,
    ) -> FromFnLayer<
        impl FnMut(Request<DB>, Next<DB>) -> BoxFuture<'static, sqlx::Result<Response<DB>>>
            + Clone
            + Send
            + Sync
            + 'static,
        DB,
    > {
        let recorded = self.0.clone();

        middleware::from_fn(move |request: Request<DB>, next: Next<DB>| {
            recorded.lock().unwrap().push(Recorded {
                kind: request.kind(),
                sql: request.sql().as_str().to_owned(),
                transaction_depth: request.transaction_depth(),
            });

            Box::pin(next.run(request)) as BoxFuture<'static, _>
        })
    }

    fn take(&self) -> Vec<Recorded> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }

    fn take_sql(&self) -> Vec<(RequestKind, String)> {
        self.take()
            .into_iter()
            .map(|recorded| (recorded.kind, recorded.sql))
            .collect()
    }
}

fn memory() -> SqliteConnectOptions {
    "sqlite::memory:".parse().unwrap()
}

#[sqlx_macros::test]
async fn it_sends_every_statement_through_connection_layers() -> anyhow::Result<()> {
    let recorder = Recorder::default();
    let mut conn = SqliteConnection::connect_with(&memory().layer(recorder.layer())).await?;

    // The `PRAGMA`s which initialize the connection don't pass through middleware.
    assert_eq!(recorder.take(), vec![]);

    conn.execute("CREATE TABLE test (id INTEGER)").await?;
    sqlx::query("INSERT INTO test (id) VALUES (?)")
        .bind(1)
        .execute(&mut conn)
        .await?;

    let mut tx = conn.begin().await?;
    sqlx::query("INSERT INTO test (id) VALUES (2)")
        .execute(&mut *tx)
        .await?;
    let mut savepoint = tx.begin().await?;
    sqlx::query("INSERT INTO test (id) VALUES (3)")
        .execute(&mut *savepoint)
        .await?;
    savepoint.rollback().await?;
    tx.commit().await?;

    let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM test ORDER BY id")
        .fetch_all(&mut conn)
        .await?;
    assert_eq!(ids, vec![1, 2]);

    assert_eq!(
        recorder.take(),
        vec![
            Recorded {
                kind: RequestKind::Fetch,
                sql: "CREATE TABLE test (id INTEGER)".into(),
                transaction_depth: 0
            },
            Recorded {
                kind: RequestKind::Fetch,
                sql: "INSERT INTO test (id) VALUES (?)".into(),
                transaction_depth: 0
            },
            Recorded {
                kind: RequestKind::Begin,
                sql: "BEGIN".into(),
                transaction_depth: 0
            },
            Recorded {
                kind: RequestKind::Fetch,
                sql: "INSERT INTO test (id) VALUES (2)".into(),
                transaction_depth: 1
            },
            Recorded {
                kind: RequestKind::Begin,
                sql: "SAVEPOINT _sqlx_savepoint_1".into(),
                transaction_depth: 1
            },
            Recorded {
                kind: RequestKind::Fetch,
                sql: "INSERT INTO test (id) VALUES (3)".into(),
                transaction_depth: 2
            },
            Recorded {
                kind: RequestKind::Rollback,
                sql: "ROLLBACK TO SAVEPOINT _sqlx_savepoint_1".into(),
                transaction_depth: 2
            },
            Recorded {
                kind: RequestKind::Commit,
                sql: "COMMIT".into(),
                transaction_depth: 1
            },
            Recorded {
                kind: RequestKind::Fetch,
                sql: "SELECT id FROM test ORDER BY id".into(),
                transaction_depth: 0
            },
        ]
    );

    Ok(())
}

#[sqlx_macros::test]
async fn it_applies_pool_and_connection_layers() -> anyhow::Result<()> {
    let pool_recorder = Recorder::default();
    let conn_recorder = Recorder::default();

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .layer(pool_recorder.layer())
        .connect_with(memory().layer(conn_recorder.layer()))
        .await?;

    let value: i64 = sqlx::query_scalar("SELECT 1").fetch_one(&pool).await?;
    assert_eq!(value, 1);

    let mut tx = pool.begin().await?;
    sqlx::query("SELECT 2").execute(&mut *tx).await?;
    tx.commit().await?;

    assert_eq!(
        pool_recorder.take_sql(),
        vec![(RequestKind::FetchOptional, "SELECT 1".into())]
    );

    assert_eq!(
        conn_recorder.take_sql(),
        vec![
            (RequestKind::FetchOptional, "SELECT 1".into()),
            (RequestKind::Begin, "BEGIN".into()),
            (RequestKind::Fetch, "SELECT 2".into()),
            (RequestKind::Commit, "COMMIT".into()),
        ]
    );

    Ok(())
}

#[sqlx_macros::test]
async fn it_can_rewrite_requests_and_pass_errors_through() -> anyhow::Result<()> {
    let layer = middleware::from_fn(|mut request: Request<Sqlite>, next: Next<Sqlite>| {
        if request.sql().as_str() == "SELECT 1" {
            request.set_sql("SELECT 2 /* rewritten */");
        }

        next.run(request)
    });

    let mut conn = SqliteConnection::connect_with(&memory().layer(layer)).await?;

    let value: i64 = sqlx::query_scalar("SELECT 1").fetch_one(&mut conn).await?;
    assert_eq!(value, 2);

    let err = conn.execute("SELECT * FROM").await.unwrap_err();
    assert!(matches!(err, Error::Database(_)), "{err:?}");

    let value: i64 = sqlx::query_scalar("SELECT 3").fetch_one(&mut conn).await?;
    assert_eq!(value, 3);

    Ok(())
}

#[sqlx_macros::test]
async fn it_provides_connection_info() -> anyhow::Result<()> {
    let info = Arc::new(Mutex::new(None));

    let layer = {
        let info = info.clone();
        middleware::from_fn(move |request: Request<Sqlite>, next: Next<Sqlite>| {
            *info.lock().unwrap() = request.connection_info().cloned();
            next.run(request)
        })
    };

    let mut conn = SqliteConnection::connect_with(&memory().layer(layer)).await?;
    conn.execute("SELECT 1").await?;

    let info = info
        .lock()
        .unwrap()
        .clone()
        .expect("connection info not set");
    assert_eq!(info.database.as_deref(), Some(":memory:"));
    assert_eq!(info.server_address, None);

    Ok(())
}

#[cfg(feature = "_rt-tokio")]
#[sqlx_macros::test]
async fn it_serializes_requests_while_a_response_is_detached() -> anyhow::Result<()> {
    use std::time::Duration;

    // Keeps reading result streams in a background task after the request returned.
    let layer = middleware::from_fn(|request: Request<Sqlite>, next: Next<Sqlite>| async move {
        let response = next.run(request).await?;

        let Response::Stream(mut stream) = response else {
            return Ok(response);
        };

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                tokio::time::sleep(Duration::from_millis(1)).await;
                if tx.send(item).is_err() {
                    break;
                }
            }
        });

        Ok(Response::Stream(ResultStream::new(
            futures_util::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            }),
        )))
    });

    let mut conn = SqliteConnection::connect_with(&memory().layer(layer)).await?;
    conn.execute("CREATE TABLE test (id INTEGER)").await?;

    let mut rows = sqlx::query(
        "WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 100) \
         SELECT n FROM seq",
    )
    .fetch(&mut conn);
    let first: i64 = rows.next().await.unwrap()?.try_get(0)?;
    assert_eq!(first, 1);
    drop(rows);

    let mut tx = conn.begin().await?;
    tx.execute("INSERT INTO test (id) VALUES (1)").await?;
    drop(tx);

    assert!(!conn.is_in_transaction());

    // The dropped transaction was rolled back.
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(count, 0);

    Ok(())
}

#[cfg(feature = "any")]
#[sqlx_macros::test]
async fn it_supports_layers_on_any_connections() -> anyhow::Result<()> {
    use sqlx::any::{Any, AnyConnectOptions};
    use sqlx::AnyConnection;

    sqlx::any::install_default_drivers();

    let recorder = Recorder::default();
    let options: AnyConnectOptions = "sqlite::memory:".parse()?;
    let mut conn = AnyConnection::connect_with(&options.layer(recorder.layer::<Any>())).await?;

    let value: i64 = sqlx::query_scalar("SELECT ?")
        .bind(5i64)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(value, 5);

    let mut tx = conn.begin().await?;
    sqlx::query("SELECT 1").execute(&mut *tx).await?;
    tx.commit().await?;

    assert_eq!(
        recorder.take_sql(),
        vec![
            (RequestKind::FetchOptional, "SELECT ?".into()),
            (RequestKind::Begin, "BEGIN".into()),
            (RequestKind::Fetch, "SELECT 1".into()),
            (RequestKind::Commit, "COMMIT".into()),
        ]
    );

    Ok(())
}
