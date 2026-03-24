/// Integration tests that run against a real Postgres instance.
/// These require a Postgres server running on localhost:5432 with
/// a user 'postgres' and password 'postgres'.
///
/// Set PG_TEST_HOST, PG_TEST_PORT, PG_TEST_USER, PG_TEST_PASSWORD
/// environment variables to override.
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn pg_host() -> String {
    std::env::var("PG_TEST_HOST").unwrap_or_else(|_| "localhost".to_string())
}
fn pg_port() -> u16 {
    std::env::var("PG_TEST_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(5432)
}
fn pg_user() -> String {
    std::env::var("PG_TEST_USER").unwrap_or_else(|_| "postgres".to_string())
}
fn pg_password() -> String {
    std::env::var("PG_TEST_PASSWORD").unwrap_or_else(|_| "postgres".to_string())
}

/// Helper: start the multiplexer in background, return its listen address.
async fn start_multiplexer() -> (SocketAddr, SocketAddr, tokio::task::JoinHandle<()>) {
    use pgmux::admin;
    use pgmux::config::Config;
    use pgmux::monitor::DbSizeMonitor;
    use pgmux::pool::PoolManager;

    let cfg = Arc::new(Config {
        listen_addr: "127.0.0.1:0".to_string(),
        admin_listen_addr: "127.0.0.1:0".to_string(),
        upstream_host: pg_host(),
        upstream_port: pg_port(),
        ..Config::default()
    });

    let metrics = Arc::new(admin::metrics::Metrics::new());
    let pool_manager = Arc::new(PoolManager::new(cfg.clone(), metrics.clone()));
    let size_monitor = Arc::new(DbSizeMonitor::new(
        cfg.clone(),
        pool_manager.clone(),
        metrics.clone(),
    ));

    // Bind the PG listener
    let pg_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pg_addr = pg_listener.local_addr().unwrap();

    // Bind the admin listener
    let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_addr = admin_listener.local_addr().unwrap();

    let handle = {
        let pool_mgr = pool_manager.clone();
        let m = metrics.clone();
        let sz = size_monitor.clone();
        let c = cfg.clone();

        tokio::spawn(async move {
            // Start admin server in background
            let admin_state = admin::server::AdminState {
                cfg: c.clone(),
                metrics: m.clone(),
                pool_manager: pool_mgr.clone(),
                size_monitor: sz.clone(),
            };
            let app = axum::Router::new()
                .route("/health", axum::routing::get(|| async { "ok" }))
                .with_state(admin_state);
            tokio::spawn(async move {
                axum::serve(admin_listener, app).await.ok();
            });

            // Accept PG connections
            loop {
                let (stream, addr) = pg_listener.accept().await.unwrap();
                let pool_mgr = pool_mgr.clone();
                let m2 = m.clone();
                let sz2 = sz.clone();
                let c2 = c.clone();

                tokio::spawn(async move {
                    m2.client_connections_total.inc();
                    m2.client_connections_active.inc();
                    let _ = pgmux::protocol::handle_client(
                        stream, addr, pool_mgr, sz2, None, c2,
                    )
                    .await;
                    m2.client_connections_active.dec();
                });
            }
        })
    };

    // Give it a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;
    (pg_addr, admin_addr, handle)
}

/// Connect to the multiplexer via tokio-postgres client.
async fn connect_via_multiplexer(
    addr: SocketAddr,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let conn_str = format!(
        "host={} port={} user={} password={} dbname=postgres",
        addr.ip(),
        addr.port(),
        pg_user(),
        pg_password(),
    );

    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    Ok(client)
}

#[tokio::test]
async fn test_basic_query_through_multiplexer() {
    let (pg_addr, _, _handle) = start_multiplexer().await;
    let client = connect_via_multiplexer(pg_addr).await.unwrap();

    let rows = client.query("SELECT 1 as num", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let val: i32 = rows[0].get(0);
    assert_eq!(val, 1);
}

#[tokio::test]
async fn test_multiple_queries() {
    let (pg_addr, _, _handle) = start_multiplexer().await;
    let client = connect_via_multiplexer(pg_addr).await.unwrap();

    for i in 0..10 {
        let rows = client.query("SELECT $1::int as num", &[&i]).await.unwrap();
        let val: i32 = rows[0].get(0);
        assert_eq!(val, i);
    }
}

#[tokio::test]
async fn test_concurrent_connections() {
    let (pg_addr, _, _handle) = start_multiplexer().await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let addr = pg_addr;
        handles.push(tokio::spawn(async move {
            let client = connect_via_multiplexer(addr).await.unwrap();
            let rows = client.query("SELECT $1::int as num", &[&i]).await.unwrap();
            let val: i32 = rows[0].get(0);
            assert_eq!(val, i);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_create_table_insert_select() {
    let (pg_addr, _, _handle) = start_multiplexer().await;
    let client = connect_via_multiplexer(pg_addr).await.unwrap();

    // Create temp table
    client
        .execute(
            "CREATE TEMP TABLE test_mux (id serial primary key, name text)",
            &[],
        )
        .await
        .unwrap();

    // Insert
    client
        .execute("INSERT INTO test_mux (name) VALUES ($1)", &[&"hello"])
        .await
        .unwrap();

    // Select
    let rows = client
        .query("SELECT name FROM test_mux WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "hello");
}

#[tokio::test]
async fn test_transaction() {
    let (pg_addr, _, _handle) = start_multiplexer().await;
    let client = connect_via_multiplexer(pg_addr).await.unwrap();

    client
        .execute(
            "CREATE TEMP TABLE test_tx (id serial primary key, val int)",
            &[],
        )
        .await
        .unwrap();

    // Begin transaction
    client.execute("BEGIN", &[]).await.unwrap();
    client
        .execute("INSERT INTO test_tx (val) VALUES (42)", &[])
        .await
        .unwrap();
    client.execute("COMMIT", &[]).await.unwrap();

    let rows = client.query("SELECT val FROM test_tx", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let val: i32 = rows[0].get(0);
    assert_eq!(val, 42);
}

#[tokio::test]
async fn test_admin_health_endpoint() {
    let (_, admin_addr, _handle) = start_multiplexer().await;

    let resp = reqwest::get(format!("http://{}/health", admin_addr))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// Performance benchmark: measure queries per second through the multiplexer.
#[tokio::test]
async fn test_performance_throughput() {
    let (pg_addr, _, _handle) = start_multiplexer().await;
    let client = connect_via_multiplexer(pg_addr).await.unwrap();

    let num_queries = 1000;
    let start = Instant::now();

    for _ in 0..num_queries {
        client.query("SELECT 1", &[]).await.unwrap();
    }

    let elapsed = start.elapsed();
    let qps = num_queries as f64 / elapsed.as_secs_f64();

    println!(
        "\n=== Performance: {} queries in {:.2}s = {:.0} queries/sec ===\n",
        num_queries,
        elapsed.as_secs_f64(),
        qps
    );

    // Sanity check: should handle at least 100 qps even in CI
    assert!(qps > 100.0, "Query throughput too low: {:.0} qps", qps);
}

/// Performance benchmark: connection establishment time.
#[tokio::test]
async fn test_performance_connection_time() {
    let (pg_addr, _, _handle) = start_multiplexer().await;

    let num_connections = 50;
    let start = Instant::now();

    for _ in 0..num_connections {
        let client = connect_via_multiplexer(pg_addr).await.unwrap();
        client.query("SELECT 1", &[]).await.unwrap();
        // Drop client (disconnects)
    }

    let elapsed = start.elapsed();
    let avg_ms = elapsed.as_millis() as f64 / num_connections as f64;

    println!(
        "\n=== Performance: {} connections in {:.2}s = {:.1}ms avg connection time ===\n",
        num_connections,
        elapsed.as_secs_f64(),
        avg_ms,
    );

    // Connection should take less than 500ms on average
    assert!(
        avg_ms < 500.0,
        "Connection time too high: {:.1}ms avg",
        avg_ms
    );
}

/// Performance benchmark: concurrent query throughput.
#[tokio::test]
async fn test_performance_concurrent_throughput() {
    let (pg_addr, _, _handle) = start_multiplexer().await;

    let concurrency = 10;
    let queries_per_client = 100;
    let start = Instant::now();

    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let addr = pg_addr;
        handles.push(tokio::spawn(async move {
            let client = connect_via_multiplexer(addr).await.unwrap();
            for _ in 0..queries_per_client {
                client.query("SELECT 1", &[]).await.unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let total_queries = concurrency * queries_per_client;
    let qps = total_queries as f64 / elapsed.as_secs_f64();

    println!(
        "\n=== Performance: {} concurrent clients x {} queries = {} total in {:.2}s = {:.0} qps ===\n",
        concurrency, queries_per_client, total_queries, elapsed.as_secs_f64(), qps
    );

    assert!(qps > 500.0, "Concurrent throughput too low: {:.0} qps", qps);
}
