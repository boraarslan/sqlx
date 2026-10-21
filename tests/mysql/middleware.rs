use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use sqlx::middleware::{self, FromFnLayer, Next, Request, RequestKind, Response, ResultStream};
use sqlx::mysql::{MySql, MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{Connection, Database, Error, Executor, MySqlConnection, Row};
use sqlx_test::setup_if_needed;

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

    fn take_sql(&self) -> Vec<(RequestKind, String)> {
        std::mem::take(&mut *self.0.lock().unwrap())
            .into_iter()
            .map(|recorded| (recorded.kind, recorded.sql))
            .collect()
    }
}

fn connect_options() -> anyhow::Result<MySqlConnectOptions> {
    setup_if_needed();
    Ok(std::env::var("DATABASE_URL")?.parse()?)
}

#[sqlx_macros::test]
async fn it_sends_every_statement_through_connection_layers() -> anyhow::Result<()> {
    let recorder = Recorder::default();
    let mut conn =
        MySqlConnection::connect_with(&connect_options()?.layer(recorder.layer())).await?;

    // The statements which initialize the connection don't pass through middleware.
    assert_eq!(recorder.take_sql(), vec![]);

    sqlx::query("SELECT 1").execute(&mut conn).await?;
    sqlx::query("SELECT ?")
        .bind(5i32)
        .fetch_one(&mut conn)
        .await?;

    let mut tx = conn.begin().await?;
    sqlx::query("SELECT 2").execute(&mut *tx).await?;
    let mut savepoint = tx.begin().await?;
    sqlx::query("SELECT 3").execute(&mut *savepoint).await?;
    savepoint.commit().await?;
    tx.rollback().await?;

    assert_eq!(
        recorder.take_sql(),
        vec![
            (RequestKind::Fetch, "SELECT 1".into()),
            (RequestKind::FetchOptional, "SELECT ?".into()),
            (RequestKind::Begin, "BEGIN".into()),
            (RequestKind::Fetch, "SELECT 2".into()),
            (RequestKind::Begin, "SAVEPOINT _sqlx_savepoint_1".into()),
            (RequestKind::Fetch, "SELECT 3".into()),
            (
                RequestKind::Commit,
                "RELEASE SAVEPOINT _sqlx_savepoint_1".into()
            ),
            (RequestKind::Rollback, "ROLLBACK".into()),
        ]
    );

    assert!(!conn.is_in_transaction());

    Ok(())
}

#[sqlx_macros::test]
async fn it_applies_pool_and_connection_layers() -> anyhow::Result<()> {
    let pool_recorder = Recorder::default();
    let conn_recorder = Recorder::default();

    let pool = MySqlPoolOptions::new()
        .max_connections(2)
        .layer(pool_recorder.layer())
        .connect_with(connect_options()?.layer(conn_recorder.layer()))
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
    let layer = middleware::from_fn(|mut request: Request<MySql>, next: Next<MySql>| {
        if request.sql().as_str() == "SELECT 1" {
            request.set_sql("SELECT 2 /* rewritten */");
        }

        next.run(request)
    });

    let mut conn = MySqlConnection::connect_with(&connect_options()?.layer(layer)).await?;

    let value: i64 = sqlx::query_scalar("SELECT 1").fetch_one(&mut conn).await?;
    assert_eq!(value, 2);

    let err = conn.execute("SELECT * FROM").await.unwrap_err();
    assert!(err.as_database_error().is_some(), "{err:?}");

    let value: i64 = sqlx::query_scalar("SELECT 3").fetch_one(&mut conn).await?;
    assert_eq!(value, 3);

    Ok(())
}

#[sqlx_macros::test]
async fn it_provides_connection_info() -> anyhow::Result<()> {
    let options = connect_options()?;
    let info = Arc::new(Mutex::new(None));

    let layer = {
        let info = info.clone();
        middleware::from_fn(move |request: Request<MySql>, next: Next<MySql>| {
            *info.lock().unwrap() = request.connection_info().cloned();
            next.run(request)
        })
    };

    let mut conn = MySqlConnection::connect_with(&options.clone().layer(layer)).await?;
    conn.execute("SELECT 1").await?;

    let info = info
        .lock()
        .unwrap()
        .clone()
        .expect("connection info not set");
    assert_eq!(info.database.as_deref(), options.get_database());
    assert_eq!(info.user.as_deref(), Some(options.get_username()));
    assert_eq!(info.server_address.as_deref(), Some(options.get_host()));
    assert_eq!(info.server_port, Some(options.get_port()));

    Ok(())
}

#[cfg(feature = "_rt-tokio")]
#[sqlx_macros::test]
async fn it_works_with_tower_timeout() -> anyhow::Result<()> {
    use tower::timeout::{error::Elapsed, TimeoutLayer};

    let mut conn = MySqlConnection::connect_with(
        &connect_options()?.layer(TimeoutLayer::new(Duration::from_millis(100))),
    )
    .await?;

    let err = conn.execute("SELECT SLEEP(1)").await.unwrap_err();

    match err {
        Error::Middleware(e) => assert!(e.is::<Elapsed>(), "{e:?}"),
        other => panic!("expected Error::Middleware, got {other:?}"),
    }

    // Wait for the server to finish the query.
    tokio::time::sleep(Duration::from_millis(1200)).await;

    let value: i64 = sqlx::query_scalar("SELECT 1").fetch_one(&mut conn).await?;
    assert_eq!(value, 1);

    Ok(())
}

#[cfg(feature = "_rt-tokio")]
#[sqlx_macros::test]
async fn it_serializes_requests_while_a_response_is_detached() -> anyhow::Result<()> {
    // Keeps reading result streams in a background task after the request returned.
    let layer = middleware::from_fn(|request: Request<MySql>, next: Next<MySql>| async move {
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

    let mut conn = MySqlConnection::connect_with(&connect_options()?.layer(layer)).await?;

    let mut rows = sqlx::query(
        "WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 100) \
         SELECT n FROM seq",
    )
    .fetch(&mut conn);
    let first: i64 = rows.next().await.unwrap()?.try_get(0)?;
    assert_eq!(first, 1);
    drop(rows);

    conn.execute("CREATE TEMPORARY TABLE middleware_rollback (id INT)")
        .await?;

    let mut tx = conn.begin().await?;
    tx.execute("INSERT INTO middleware_rollback (id) VALUES (1)")
        .await?;
    let value: i64 = sqlx::query_scalar("SELECT 2").fetch_one(&mut *tx).await?;
    assert_eq!(value, 2);
    drop(tx);

    assert!(!conn.is_in_transaction());

    let rows: Vec<i64> = sqlx::query_scalar(
        "WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5) \
         SELECT n FROM seq",
    )
    .fetch_all(&mut conn)
    .await?;
    assert_eq!(rows, vec![1, 2, 3, 4, 5]);

    // The dropped transaction was rolled back.
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM middleware_rollback")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(count, 0);

    Ok(())
}
