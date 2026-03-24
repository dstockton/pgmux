use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::sync::atomic::AtomicU64;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DatabaseLabels {
    pub database: String,
}

/// All application metrics.
#[allow(dead_code)]
pub struct Metrics {
    pub registry: Registry,

    // Client metrics
    pub client_connections_total: Counter<u64>,
    pub client_connections_active: Gauge,

    // Server/pool metrics
    pub server_connections_total: Counter<u64>,
    pub server_connections_active: Gauge,
    pub server_connection_errors_total: Counter<u64>,
    pub pool_hits_total: Counter<u64>,
    pub pool_misses_total: Counter<u64>,
    pub pool_timeouts_total: Counter<u64>,

    // DB size metrics
    pub db_size_bytes: Family<DatabaseLabels, Gauge<f64, AtomicU64>>,
    pub db_size_limit_bytes: Family<DatabaseLabels, Gauge<f64, AtomicU64>>,
    pub db_over_limit: Family<DatabaseLabels, Gauge<f64, AtomicU64>>,

    // Query metrics
    pub queries_total: Counter<u64>,
    pub queries_read_only_enforced: Counter<u64>,
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let client_connections_total = Counter::<u64>::default();
        let client_connections_active = Gauge::default();
        let server_connections_total = Counter::<u64>::default();
        let server_connections_active = Gauge::default();
        let server_connection_errors_total = Counter::<u64>::default();
        let pool_hits_total = Counter::<u64>::default();
        let pool_misses_total = Counter::<u64>::default();
        let pool_timeouts_total = Counter::<u64>::default();
        let db_size_bytes = Family::<DatabaseLabels, Gauge<f64, AtomicU64>>::default();
        let db_size_limit_bytes = Family::<DatabaseLabels, Gauge<f64, AtomicU64>>::default();
        let db_over_limit = Family::<DatabaseLabels, Gauge<f64, AtomicU64>>::default();
        let queries_total = Counter::<u64>::default();
        let queries_read_only_enforced = Counter::<u64>::default();

        registry.register(
            "pgmux_client_connections_total",
            "Total client connections accepted",
            client_connections_total.clone(),
        );
        registry.register(
            "pgmux_client_connections_active",
            "Currently active client connections",
            client_connections_active.clone(),
        );
        registry.register(
            "pgmux_server_connections_total",
            "Total server connections created",
            server_connections_total.clone(),
        );
        registry.register(
            "pgmux_server_connections_active",
            "Currently active server connections",
            server_connections_active.clone(),
        );
        registry.register(
            "pgmux_server_connection_errors_total",
            "Total server connection errors",
            server_connection_errors_total.clone(),
        );
        registry.register(
            "pgmux_pool_hits_total",
            "Connections served from pool",
            pool_hits_total.clone(),
        );
        registry.register(
            "pgmux_pool_misses_total",
            "Connections that required new backend connection",
            pool_misses_total.clone(),
        );
        registry.register(
            "pgmux_pool_timeouts_total",
            "Connection acquire timeouts",
            pool_timeouts_total.clone(),
        );
        registry.register(
            "pgmux_db_size_bytes",
            "Current database size in bytes",
            db_size_bytes.clone(),
        );
        registry.register(
            "pgmux_db_size_limit_bytes",
            "Configured database size limit in bytes",
            db_size_limit_bytes.clone(),
        );
        registry.register(
            "pgmux_db_over_limit",
            "Whether database is over its size limit (1=over, 0=ok)",
            db_over_limit.clone(),
        );
        registry.register(
            "pgmux_queries_total",
            "Total queries proxied",
            queries_total.clone(),
        );
        registry.register(
            "pgmux_queries_read_only_enforced",
            "Queries where read-only was enforced due to size limit",
            queries_read_only_enforced.clone(),
        );

        Self {
            registry,
            client_connections_total,
            client_connections_active,
            server_connections_total,
            server_connections_active,
            server_connection_errors_total,
            pool_hits_total,
            pool_misses_total,
            pool_timeouts_total,
            db_size_bytes,
            db_size_limit_bytes,
            db_over_limit,
            queries_total,
            queries_read_only_enforced,
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
