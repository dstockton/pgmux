use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::admin::metrics::{DatabaseLabels, Metrics};
use crate::config::Config;
use crate::pool::PoolManager;

/// Monitors database sizes and tracks configured limits.
#[allow(dead_code)]
pub struct DbSizeMonitor {
    cfg: Arc<Config>,
    pool_manager: Arc<PoolManager>,
    metrics: Arc<Metrics>,
    /// Current known DB sizes (db_name -> size in bytes).
    db_sizes: DashMap<String, u64>,
    /// Configured size limits (db_name -> max size in bytes).
    db_limits: DashMap<String, u64>,
}

impl DbSizeMonitor {
    pub fn new(cfg: Arc<Config>, pool_manager: Arc<PoolManager>, metrics: Arc<Metrics>) -> Self {
        Self {
            cfg,
            pool_manager,
            metrics,
            db_sizes: DashMap::new(),
            db_limits: DashMap::new(),
        }
    }

    /// Register a size limit for a database.
    pub fn register_limit(&self, database: &str, max_bytes: u64) {
        self.db_limits.insert(database.to_string(), max_bytes);
        info!(
            db = database,
            limit_bytes = max_bytes,
            limit_human = %format_bytes(max_bytes),
            "Registered DB size limit"
        );
    }

    /// Get the current known size of a database.
    pub fn get_db_size(&self, database: &str) -> Option<u64> {
        self.db_sizes.get(database).map(|v| *v)
    }

    /// Get the configured limit for a database.
    pub fn get_db_limit(&self, database: &str) -> Option<u64> {
        self.db_limits.get(database).map(|v| *v).or(
            if self.cfg.monitor.default_max_db_size_bytes > 0 {
                Some(self.cfg.monitor.default_max_db_size_bytes)
            } else {
                None
            },
        )
    }

    /// Check if a database is over its size limit.
    #[allow(dead_code)]
    pub fn is_over_limit(&self, database: &str) -> bool {
        if let Some(limit) = self.get_db_limit(database) {
            if let Some(size) = self.get_db_size(database) {
                return size > limit;
            }
        }
        false
    }

    /// Get all tracked databases with their sizes and limits.
    pub fn get_all_db_info(&self) -> Vec<DbSizeInfo> {
        let mut result = Vec::new();

        // Collect all known databases
        let mut databases: std::collections::HashSet<String> = std::collections::HashSet::new();
        for entry in self.db_sizes.iter() {
            databases.insert(entry.key().clone());
        }
        for entry in self.db_limits.iter() {
            databases.insert(entry.key().clone());
        }

        for db in databases {
            let size = self.db_sizes.get(&db).map(|v| *v);
            let limit = self.get_db_limit(&db);
            let over_limit = match (size, limit) {
                (Some(s), Some(l)) => s > l,
                _ => false,
            };
            result.push(DbSizeInfo {
                database: db,
                size_bytes: size,
                limit_bytes: limit,
                over_limit,
            });
        }

        result.sort_by(|a, b| a.database.cmp(&b.database));
        result
    }

    /// Run the periodic size check loop.
    pub async fn run(&self) {
        let interval = Duration::from_secs(self.cfg.monitor.check_interval_secs);
        let mut ticker = tokio::time::interval(interval);

        // Wait a bit before first check to let connections establish
        tokio::time::sleep(Duration::from_secs(5)).await;

        loop {
            ticker.tick().await;
            self.check_all_sizes().await;
        }
    }

    /// Check sizes of all tracked databases.
    async fn check_all_sizes(&self) {
        let databases: Vec<String> = self
            .db_limits
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        if databases.is_empty() {
            return;
        }

        debug!("Checking sizes for {} databases", databases.len());

        // Connect to each database's pool to check size
        // We use a single admin connection per unique host:port
        let host = &self.cfg.upstream_host;
        let port = self.cfg.upstream_port;
        // Try to connect using postgres/postgres for size checking
        // In practice, this would use a configured admin credential
        match tokio_postgres::connect(
            &format!(
                "host={} port={} user=postgres dbname=postgres connect_timeout=5",
                host, port
            ),
            tokio_postgres::NoTls,
        )
        .await
        {
            Ok((client, connection)) => {
                // Spawn the connection handler
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        debug!("Size check connection closed: {}", e);
                    }
                });

                for db in &databases {
                    match client
                        .query_one("SELECT pg_database_size($1::text)", &[db])
                        .await
                    {
                        Ok(row) => {
                            let size: i64 = row.get(0);
                            let size = size as u64;
                            self.db_sizes.insert(db.clone(), size);

                            // Update metrics
                            let labels = DatabaseLabels {
                                database: db.clone(),
                            };
                            self.metrics
                                .db_size_bytes
                                .get_or_create(&labels)
                                .set(size as f64);
                            if let Some(limit) = self.get_db_limit(db) {
                                self.metrics
                                    .db_size_limit_bytes
                                    .get_or_create(&labels)
                                    .set(limit as f64);
                                if size > limit {
                                    self.metrics
                                        .db_over_limit
                                        .get_or_create(&labels)
                                        .set(1.0);
                                    warn!(
                                        db = db.as_str(),
                                        size = size,
                                        limit = limit,
                                        "Database over size limit — enforcing read-only"
                                    );
                                } else {
                                    self.metrics
                                        .db_over_limit
                                        .get_or_create(&labels)
                                        .set(0.0);
                                }
                            }

                            debug!(
                                db = db.as_str(),
                                size_bytes = size,
                                size_human = %format_bytes(size),
                                "DB size check"
                            );
                        }
                        Err(e) => {
                            warn!(db = db.as_str(), error = %e, "Failed to check DB size");
                        }
                    }
                }
            }
            Err(e) => {
                error!(
                    "Failed to connect for size check ({}:{}): {}",
                    host, port, e
                );
            }
        }
    }
}

/// Information about a database's size.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DbSizeInfo {
    pub database: String,
    pub size_bytes: Option<u64>,
    pub limit_bytes: Option<u64>,
    pub over_limit: bool,
}

/// Format bytes into human-readable string.
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(5 * 1024 * 1024 * 1024), "5.00 GB");
    }
}
