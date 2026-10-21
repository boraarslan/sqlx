use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use futures_util::{StreamExt, TryStreamExt};
use sqlx::middleware::{
    self, BoxService, FromFnLayer, Layer, Next, Request, RequestKind, Response, ResultStream,
    ServiceExecutor,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, Postgres};
use sqlx::{Connection, Database, Error, Executor, Row};
use sqlx_test::setup_if_needed;

#[derive(Debug, Clone, PartialEq)]
struct Recorded {
    kind: RequestKind,
    sql: String,
    transaction_depth: usize,
}

/// Records every request that passes through its layer.
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

fn connect_options() -> anyhow::Result<PgConnectOptions> {
    setup_if_needed();
    Ok(std::env::var("DATABASE_URL")?.parse()?)
}

#[sqlx_macros::test]
async fn it_sends_every_statement_through_connection_layers() -> anyhow::Result<()> {
    let recorder = Recorder::default();
    let mut conn = PgConnection::connect_with(&connect_options()?.layer(recorder.layer())).await?;

    // Connecting does not run any statements through the middleware.
    assert_eq!(recorder.take(), vec![]);

    sqlx::query("SELECT 1").execute(&mut conn).await?;
    sqlx::query("SELECT $1::int4")
        .bind(5i32)
        .fetch_one(&mut conn)
        .await?;
    sqlx::query("SELECT 2").fetch_optional(&mut conn).await?;
    sqlx::raw_sql("SELECT 3; SELECT 4")
        .fetch_all(&mut conn)
        .await?;
    conn.prepare("SELECT $1::text".into_sql_str()).await?;

    assert_eq!(
        recorder.take_sql(),
        vec![
            (RequestKind::Fetch, "SELECT 1".into()),
            (RequestKind::FetchOptional, "SELECT $1::int4".into()),
            (RequestKind::FetchOptional, "SELECT 2".into()),
            (RequestKind::Fetch, "SELECT 3; SELECT 4".into()),
            (RequestKind::Prepare, "SELECT $1::text".into()),
        ]
    );

    // Transactions and savepoints.
    let mut tx = conn.begin().await?;
    sqlx::query("SELECT 5").execute(&mut *tx).await?;
    let mut savepoint = tx.begin().await?;
    sqlx::query("SELECT 6").execute(&mut *savepoint).await?;
    savepoint.rollback().await?;
    tx.commit().await?;

    assert_eq!(
        recorder.take(),
        vec![
            Recorded {
                kind: RequestKind::Begin,
                sql: "BEGIN".into(),
                transaction_depth: 0
            },
            Recorded {
                kind: RequestKind::Fetch,
                sql: "SELECT 5".into(),
                transaction_depth: 1
            },
            Recorded {
                kind: RequestKind::Begin,
                sql: "SAVEPOINT _sqlx_savepoint_1".into(),
                transaction_depth: 1
            },
            Recorded {
                kind: RequestKind::Fetch,
                sql: "SELECT 6".into(),
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
        ]
    );

    assert!(!conn.is_in_transaction());

    conn.close().await?;

    Ok(())
}

#[sqlx_macros::test]
async fn it_provides_connection_info() -> anyhow::Result<()> {
    let options = connect_options()?;
    let info = Arc::new(Mutex::new(None));

    let layer = {
        let info = info.clone();
        middleware::from_fn(move |request: Request<Postgres>, next: Next<Postgres>| {
            *info.lock().unwrap() = request.connection_info().cloned();
            next.run(request)
        })
    };

    let mut conn = PgConnection::connect_with(&options.clone().layer(layer)).await?;
    sqlx::query("SELECT 1").execute(&mut conn).await?;

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

#[sqlx_macros::test]
async fn it_applies_connection_layers_to_pool_connections() -> anyhow::Result<()> {
    let recorder = Recorder::default();

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(connect_options()?.layer(recorder.layer()))
        .await?;

    sqlx::query("SELECT 1").fetch_one(&pool).await?;

    let mut tx = pool.begin().await?;
    sqlx::query("SELECT 2").execute(&mut *tx).await?;
    tx.commit().await?;

    let mut conn = pool.acquire().await?;
    sqlx::query("SELECT 3").execute(&mut *conn).await?;

    assert_eq!(
        recorder.take_sql(),
        vec![
            (RequestKind::FetchOptional, "SELECT 1".into()),
            (RequestKind::Begin, "BEGIN".into()),
            (RequestKind::Fetch, "SELECT 2".into()),
            (RequestKind::Commit, "COMMIT".into()),
            (RequestKind::Fetch, "SELECT 3".into()),
        ]
    );

    Ok(())
}

#[sqlx_macros::test]
async fn it_applies_pool_layers_to_pool_queries() -> anyhow::Result<()> {
    let pool_recorder = Recorder::default();
    let conn_recorder = Recorder::default();

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .layer(pool_recorder.layer())
        .connect_with(connect_options()?.layer(conn_recorder.layer()))
        .await?;

    sqlx::query("SELECT 1").fetch_one(&pool).await?;
    let rows = sqlx::query("SELECT generate_series(1, 3)")
        .fetch(&pool)
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(rows.len(), 3);
    pool.prepare("SELECT $1::int8".into_sql_str()).await?;

    // Not sent to the pool's executor, so the pool layer doesn't see these.
    let mut conn = pool.acquire().await?;
    sqlx::query("SELECT 2").execute(&mut *conn).await?;
    drop(conn);

    let expected_pool = vec![
        (RequestKind::FetchOptional, "SELECT 1".into()),
        (RequestKind::Fetch, "SELECT generate_series(1, 3)".into()),
        (RequestKind::Prepare, "SELECT $1::int8".into()),
    ];

    assert_eq!(pool_recorder.take_sql(), expected_pool);

    let mut expected_conn = expected_pool;
    expected_conn.push((RequestKind::Fetch, "SELECT 2".into()));
    assert_eq!(conn_recorder.take_sql(), expected_conn);

    // Transaction control cannot be sent to a pool.
    let err = (&pool)
        .dispatch(Request::new(RequestKind::Begin, "BEGIN"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");

    Ok(())
}

#[sqlx_macros::test]
async fn it_can_rewrite_requests() -> anyhow::Result<()> {
    let layer = middleware::from_fn(|mut request: Request<Postgres>, next: Next<Postgres>| {
        if request.sql().as_str() == "SELECT 1" {
            request.set_sql("SELECT 2 /* rewritten */");
        }

        next.run(request)
    });

    let mut conn = PgConnection::connect_with(&connect_options()?.layer(layer)).await?;

    let value: i32 = sqlx::query_scalar("SELECT 1").fetch_one(&mut conn).await?;
    assert_eq!(value, 2);

    Ok(())
}

#[sqlx_macros::test]
async fn it_can_short_circuit_requests() -> anyhow::Result<()> {
    let layer = middleware::from_fn(
        |request: Request<Postgres>, next: Next<Postgres>| async move {
            if request.sql().as_str().starts_with("DROP") {
                return Err(Error::InvalidArgument("DROP is not allowed".into()));
            }

            next.run(request).await
        },
    );

    let mut conn = PgConnection::connect_with(&connect_options()?.layer(layer)).await?;

    let err = conn.execute("DROP TABLE nonexistent").await.unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");

    // The connection is still usable.
    let value: i32 = sqlx::query_scalar("SELECT 1").fetch_one(&mut conn).await?;
    assert_eq!(value, 1);

    Ok(())
}

#[sqlx_macros::test]
async fn it_passes_database_errors_through_middleware() -> anyhow::Result<()> {
    let recorder = Recorder::default();
    let mut conn = PgConnection::connect_with(&connect_options()?.layer(recorder.layer())).await?;

    let err = conn.execute("SELECT * FROM").await.unwrap_err();
    let err = err
        .into_database_error()
        .expect("expected a database error");
    assert_eq!(err.code().as_deref(), Some("42601"));

    let value: i32 = sqlx::query_scalar("SELECT 1").fetch_one(&mut conn).await?;
    assert_eq!(value, 1);

    Ok(())
}

#[sqlx_macros::test]
async fn it_can_observe_result_streams() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let rows_seen = Arc::new(AtomicUsize::new(0));
    let streams_finished = Arc::new(AtomicUsize::new(0));

    let layer = {
        let rows_seen = rows_seen.clone();
        let streams_finished = streams_finished.clone();

        middleware::from_fn(move |request: Request<Postgres>, next: Next<Postgres>| {
            let rows_seen = rows_seen.clone();
            let streams_finished = streams_finished.clone();

            async move {
                let response = next.run(request).await?;

                Ok(response.map_stream(|stream| {
                    ResultStream::new(
                        stream
                            .inspect(move |item| {
                                if let Ok(sqlx::Either::Right(_)) = item {
                                    rows_seen.fetch_add(1, Ordering::SeqCst);
                                }
                            })
                            .chain(futures_util::stream::poll_fn(move |_| {
                                streams_finished.fetch_add(1, Ordering::SeqCst);
                                std::task::Poll::Ready(None)
                            })),
                    )
                }))
            }
        })
    };

    let mut conn = PgConnection::connect_with(&connect_options()?.layer(layer)).await?;

    let rows = sqlx::query("SELECT generate_series(1, 10)")
        .fetch_all(&mut conn)
        .await?;
    assert_eq!(rows.len(), 10);

    assert_eq!(rows_seen.load(Ordering::SeqCst), 10);
    assert_eq!(streams_finished.load(Ordering::SeqCst), 1);

    Ok(())
}

#[sqlx_macros::test]
async fn it_can_wrap_executors_ad_hoc() -> anyhow::Result<()> {
    use tower::ServiceBuilder;

    let recorder = Recorder::default();
    let mut conn = PgConnection::connect_with(&connect_options()?).await?;

    {
        // Generic Tower middleware can wrap a borrowed connection
        // (`from_fn()` cannot, as it requires a `'static` inner service).
        let recorded = recorder.0.clone();

        let mut executor = ServiceExecutor::new(
            ServiceBuilder::new()
                .map_request(move |request: Request<Postgres>| {
                    recorded.lock().unwrap().push(Recorded {
                        kind: request.kind(),
                        sql: request.sql().as_str().to_owned(),
                        transaction_depth: request.transaction_depth(),
                    });
                    request
                })
                .service(&mut conn),
        );

        let value: i64 = sqlx::query_scalar("SELECT 1::int8")
            .fetch_one(&mut executor)
            .await?;
        assert_eq!(value, 1);
    }

    // The connection itself has no layers.
    sqlx::query("SELECT 2").execute(&mut conn).await?;

    assert_eq!(
        recorder.take_sql(),
        vec![(RequestKind::FetchOptional, "SELECT 1::int8".into())]
    );

    // Pools can be wrapped too; the clone-able stack can be shared through `&ServiceExecutor`.
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(connect_options()?)
        .await?;

    let executor = ServiceExecutor::new(
        ServiceBuilder::new()
            .layer(recorder.layer::<Postgres>())
            .service(pool.clone()),
    );

    let (a, b): (i32, i32) = futures_util::future::try_join(
        sqlx::query_scalar("SELECT 1").fetch_one(&executor),
        sqlx::query_scalar("SELECT 2").fetch_one(&executor),
    )
    .await?;
    assert_eq!((a, b), (1, 2));
    assert_eq!(recorder.take().len(), 2);

    Ok(())
}

#[sqlx_macros::test]
async fn it_works_with_tower_middleware() -> anyhow::Result<()> {
    use tower::limit::GlobalConcurrencyLimitLayer;
    use tower::load_shed::LoadShedLayer;
    use tower::ServiceBuilder;

    // A global concurrency limit shared by all connections of the pool.
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            connect_options()?.layer(
                ServiceBuilder::new()
                    .layer(GlobalConcurrencyLimitLayer::new(1))
                    .into_inner(),
            ),
        )
        .await?;

    let start = Instant::now();

    let results = futures_util::future::try_join_all(
        (0..4)
            .map(|_| sqlx::query_scalar::<_, i32>("SELECT 1 FROM pg_sleep(0.2)").fetch_one(&pool)),
    )
    .await?;

    assert_eq!(results, vec![1; 4]);
    assert!(
        start.elapsed() >= Duration::from_millis(800),
        "queries should have run one at a time: {:?}",
        start.elapsed()
    );

    // `LoadShed` relies on `poll_ready()` being called before `call()`.
    let mut conn =
        PgConnection::connect_with(&connect_options()?.layer(LoadShedLayer::new())).await?;
    let value: i32 = sqlx::query_scalar("SELECT 1").fetch_one(&mut conn).await?;
    assert_eq!(value, 1);

    Ok(())
}

#[cfg(feature = "_rt-tokio")]
#[sqlx_macros::test]
async fn it_works_with_tower_timeout() -> anyhow::Result<()> {
    use tower::timeout::{error::Elapsed, TimeoutLayer};

    let mut conn = PgConnection::connect_with(
        &connect_options()?.layer(TimeoutLayer::new(Duration::from_millis(100))),
    )
    .await?;

    let err = sqlx::query("SELECT pg_sleep(1)")
        .execute(&mut conn)
        .await
        .unwrap_err();

    match err {
        Error::Middleware(e) => assert!(e.is::<Elapsed>(), "{e:?}"),
        other => panic!("expected Error::Middleware, got {other:?}"),
    }

    // The query keeps running on the server; the connection has to wait for it to finish
    // (discarding its result) before it can execute the next query.
    tokio::time::sleep(Duration::from_millis(1200)).await;

    let value: i32 = sqlx::query_scalar("SELECT 1").fetch_one(&mut conn).await?;
    assert_eq!(value, 1);

    Ok(())
}

#[cfg(feature = "_rt-tokio")]
#[sqlx_macros::test]
async fn it_works_with_tower_retry() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::retry::{Policy, RetryLayer};

    #[derive(Clone)]
    struct RetryOnce;

    impl<Req: Clone, Res, E> Policy<Req, Res, E> for RetryOnce {
        type Future = std::future::Ready<()>;

        fn retry(&mut self, _req: &mut Req, result: &mut Result<Res, E>) -> Option<Self::Future> {
            result.is_err().then(|| std::future::ready(()))
        }

        fn clone_request(&mut self, req: &Req) -> Option<Req> {
            Some(req.clone())
        }
    }

    // Fails the first attempt of every request.
    let attempts = Arc::new(AtomicUsize::new(0));
    let flaky = {
        let attempts = attempts.clone();
        middleware::from_fn(move |request: Request<Postgres>, next: Next<Postgres>| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);

            async move {
                if attempt % 2 == 0 {
                    return Err(Error::Protocol("simulated transient failure".into()));
                }

                next.run(request).await
            }
        })
    };

    let mut conn = PgConnection::connect_with(
        &connect_options()?
            .layer(RetryLayer::new(RetryOnce))
            .layer(flaky),
    )
    .await?;

    let value: i32 = sqlx::query_scalar("SELECT $1")
        .bind(7i32)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(value, 7);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    Ok(())
}

#[cfg(feature = "_rt-tokio")]
#[sqlx_macros::test]
async fn it_serializes_requests_while_a_response_is_detached() -> anyhow::Result<()> {
    // A (contrived) middleware that keeps reading the result stream in a background task after
    // returning, so the connection stays locked after the request returns.
    let layer = middleware::from_fn(
        |request: Request<Postgres>, next: Next<Postgres>| async move {
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
        },
    );

    let mut conn = PgConnection::connect_with(&connect_options()?.layer(layer)).await?;

    // Only read part of the response, leaving the background task holding the connection.
    let mut rows = sqlx::query("SELECT generate_series(1, 100)").fetch(&mut conn);
    let first: i32 = rows.next().await.unwrap()?.try_get(0)?;
    assert_eq!(first, 1);
    drop(rows);

    // These must wait for the previous response, then execute normally.
    let mut tx = conn.begin().await?;
    let value: i32 = sqlx::query_scalar("SELECT 2").fetch_one(&mut *tx).await?;
    assert_eq!(value, 2);
    drop(tx);

    assert!(!conn.is_in_transaction());

    let rows: Vec<i32> = sqlx::query_scalar("SELECT generate_series(1, 5)")
        .fetch_all(&mut conn)
        .await?;
    assert_eq!(rows, vec![1, 2, 3, 4, 5]);

    // The dropped transaction was rolled back.
    let in_transaction: bool = sqlx::query_scalar("SELECT txid_current_if_assigned() IS NOT NULL")
        .fetch_one(&mut conn)
        .await?;
    assert!(!in_transaction);

    Ok(())
}

#[sqlx_macros::test]
async fn it_supports_copy_and_listen_with_layers() -> anyhow::Result<()> {
    use sqlx::postgres::PgListener;

    let recorder = Recorder::default();
    let options = connect_options()?.layer(recorder.layer());

    let mut conn = PgConnection::connect_with(&options).await?;
    conn.execute("CREATE TEMPORARY TABLE middleware_copy (id INT8)")
        .await?;

    let mut copy = conn
        .copy_in_raw("COPY middleware_copy (id) FROM STDIN")
        .await?;
    copy.send("1\n2\n3\n".as_bytes()).await?;
    assert_eq!(copy.finish().await?, 3);

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM middleware_copy")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(count, 3);

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await?;

    let mut listener = PgListener::connect_with(&pool).await?;
    listener.listen("middleware_channel").await?;

    sqlx::query("SELECT pg_notify('middleware_channel', 'hello')")
        .execute(&pool)
        .await?;

    let notification = listener.recv().await?;
    assert_eq!(notification.payload(), "hello");

    let recorded: Vec<String> = recorder
        .take()
        .into_iter()
        .map(|recorded| recorded.sql)
        .collect();

    assert!(recorded.contains(&r#"LISTEN "middleware_channel""#.to_string()));

    Ok(())
}

#[sqlx_macros::test]
async fn it_orders_layers_like_service_builder() -> anyhow::Result<()> {
    let order = Arc::new(Mutex::new(Vec::new()));

    let layer = |name: &'static str| {
        let order = order.clone();
        middleware::from_fn(move |request: Request<Postgres>, next: Next<Postgres>| {
            order.lock().unwrap().push(name);
            next.run(request)
        })
    };

    let mut conn = PgConnection::connect_with(
        &connect_options()?
            .layer(layer("outer"))
            .layer(layer("inner")),
    )
    .await?;

    conn.execute("SELECT 1").await?;
    assert_eq!(*order.lock().unwrap(), vec!["outer", "inner"]);

    Ok(())
}

#[test]
fn layers_are_type_erased() {
    fn assert_layer<L: Layer<BoxService<Postgres>>>(_: &L) {}

    let layer =
        middleware::from_fn(|request: Request<Postgres>, next: Next<Postgres>| next.run(request));

    assert_layer(&layer);

    // Options with layers are still `Clone + Debug + Send + Sync`.
    fn assert_options<T: Clone + std::fmt::Debug + Send + Sync + 'static>(_: &T) {}

    let options = PgConnectOptions::new().layer(layer);
    assert_options(&options);
}

use sqlx::{PgConnection, SqlSafeStr};
