use anyhow::{bail, Result};
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::admin::metrics::Metrics;
use crate::config::Config;
use crate::protocol::backend;
use crate::protocol::PoolKey;

/// A pooled server connection with metadata.
pub struct PooledConnection {
    pub stream: TcpStream,
    pub created_at: Instant,
    pub last_used: Instant,
}

/// A pool for a specific (host, port, db, user) combination.
struct Pool {
    idle: Mutex<VecDeque<PooledConnection>>,
    active_count: AtomicU64,
    waiters: Notify,
    total_acquired: AtomicU64,
    total_errors: AtomicU64,
}

impl Pool {
    fn new() -> Self {
        Self {
            idle: Mutex::new(VecDeque::new()),
            active_count: AtomicU64::new(0),
            waiters: Notify::new(),
            total_acquired: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
        }
    }
}

/// Manages connection pools keyed by (host, port, db, user).
pub struct PoolManager {
    pools: DashMap<PoolKey, Arc<Pool>>,
    cfg: Arc<Config>,
    metrics: Arc<Metrics>,
    total_server_connections: AtomicU64,
}

impl PoolManager {
    pub fn new(cfg: Arc<Config>, metrics: Arc<Metrics>) -> Self {
        Self {
            pools: DashMap::new(),
            cfg,
            metrics,
            total_server_connections: AtomicU64::new(0),
        }
    }

    /// Acquire a backend connection from the pool or create a new one.
    pub async fn acquire(
        &self,
        key: &PoolKey,
        password: &str,
        extra_params: &[(String, String)],
    ) -> Result<TcpStream> {
        let pool = self
            .pools
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Pool::new()))
            .clone();

        let timeout = Duration::from_millis(self.cfg.pool.acquire_timeout_ms);
        let deadline = Instant::now() + timeout;

        loop {
            // Try to get an idle connection
            {
                let mut idle = pool.idle.lock();
                while let Some(mut conn) = idle.pop_front() {
                    // Check if the connection has expired
                    let lifetime = Duration::from_secs(self.cfg.pool.max_connection_lifetime_secs);
                    if conn.created_at.elapsed() > lifetime {
                        self.total_server_connections
                            .fetch_sub(1, Ordering::Relaxed);
                        self.metrics.server_connections_active.dec();
                        debug!("Evicting expired connection for {}", key);
                        continue;
                    }

                    // Check idle timeout
                    let idle_timeout = Duration::from_secs(self.cfg.pool.idle_timeout_secs);
                    if conn.last_used.elapsed() > idle_timeout {
                        self.total_server_connections
                            .fetch_sub(1, Ordering::Relaxed);
                        self.metrics.server_connections_active.dec();
                        debug!("Evicting idle connection for {}", key);
                        continue;
                    }

                    conn.last_used = Instant::now();
                    pool.active_count.fetch_add(1, Ordering::Relaxed);
                    pool.total_acquired.fetch_add(1, Ordering::Relaxed);
                    self.metrics.pool_hits_total.inc();
                    debug!("Reusing pooled connection for {}", key);
                    return Ok(conn.stream);
                }
            }

            // Check if we can create a new connection
            let total = self.total_server_connections.load(Ordering::Relaxed);
            let per_pool =
                pool.active_count.load(Ordering::Relaxed) + pool.idle.lock().len() as u64;

            if total >= self.cfg.pool.max_total_connections as u64 {
                // Wait for a connection to be returned
                if Instant::now() >= deadline {
                    pool.total_errors.fetch_add(1, Ordering::Relaxed);
                    self.metrics.pool_timeouts_total.inc();
                    bail!("Connection pool timeout: global limit reached");
                }
                let remaining = deadline - Instant::now();
                tokio::select! {
                    _ = pool.waiters.notified() => continue,
                    _ = tokio::time::sleep(remaining) => {
                        pool.total_errors.fetch_add(1, Ordering::Relaxed);
                        self.metrics.pool_timeouts_total.inc();
                        bail!("Connection pool timeout");
                    }
                }
            }

            if per_pool >= self.cfg.pool.max_connections_per_pool as u64 {
                if Instant::now() >= deadline {
                    pool.total_errors.fetch_add(1, Ordering::Relaxed);
                    self.metrics.pool_timeouts_total.inc();
                    bail!("Connection pool timeout: per-pool limit reached");
                }
                let remaining = deadline - Instant::now();
                tokio::select! {
                    _ = pool.waiters.notified() => continue,
                    _ = tokio::time::sleep(remaining) => {
                        pool.total_errors.fetch_add(1, Ordering::Relaxed);
                        self.metrics.pool_timeouts_total.inc();
                        bail!("Connection pool timeout");
                    }
                }
            }

            // Create a new connection
            self.metrics.pool_misses_total.inc();
            match backend::connect_backend(key, password, extra_params).await {
                Ok(stream) => {
                    self.total_server_connections
                        .fetch_add(1, Ordering::Relaxed);
                    self.metrics.server_connections_active.inc();
                    self.metrics.server_connections_total.inc();
                    pool.active_count.fetch_add(1, Ordering::Relaxed);
                    pool.total_acquired.fetch_add(1, Ordering::Relaxed);
                    info!("New backend connection to {}", key);
                    return Ok(stream);
                }
                Err(e) => {
                    pool.total_errors.fetch_add(1, Ordering::Relaxed);
                    self.metrics.server_connection_errors_total.inc();
                    bail!("Failed to connect to backend {}: {}", key, e);
                }
            }
        }
    }

    /// Return a connection to the pool for reuse.
    pub async fn release(&self, key: PoolKey, mut stream: TcpStream) {
        let pool = match self.pools.get(&key) {
            Some(p) => p.clone(),
            None => {
                // Pool was removed, drop the connection
                self.total_server_connections
                    .fetch_sub(1, Ordering::Relaxed);
                self.metrics.server_connections_active.dec();
                return;
            }
        };

        pool.active_count.fetch_sub(1, Ordering::Relaxed);

        // Reset connection state before returning to pool
        match backend::reset_connection(&mut stream).await {
            Ok(()) => {
                let conn = PooledConnection {
                    stream,
                    created_at: Instant::now(),
                    last_used: Instant::now(),
                };
                pool.idle.lock().push_back(conn);
                pool.waiters.notify_one();
                debug!("Connection returned to pool for {}", key);
            }
            Err(e) => {
                warn!("Failed to reset connection for {}: {}", key, e);
                self.total_server_connections
                    .fetch_sub(1, Ordering::Relaxed);
                self.metrics.server_connections_active.dec();
            }
        }
    }

    /// Get pool statistics for all pools.
    pub fn get_stats(&self) -> Vec<PoolStats> {
        self.pools
            .iter()
            .map(|entry| {
                let key = entry.key().clone();
                let pool = entry.value();
                PoolStats {
                    key,
                    idle_connections: pool.idle.lock().len() as u64,
                    active_connections: pool.active_count.load(Ordering::Relaxed),
                    total_acquired: pool.total_acquired.load(Ordering::Relaxed),
                    total_errors: pool.total_errors.load(Ordering::Relaxed),
                }
            })
            .collect()
    }

    /// Get total number of server connections across all pools.
    pub fn total_connections(&self) -> u64 {
        self.total_server_connections.load(Ordering::Relaxed)
    }

}

/// Statistics for a single connection pool.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PoolStats {
    #[serde(serialize_with = "serialize_pool_key")]
    pub key: PoolKey,
    pub idle_connections: u64,
    pub active_connections: u64,
    pub total_acquired: u64,
    pub total_errors: u64,
}

fn serialize_pool_key<S>(key: &PoolKey, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&key.to_string())
}
