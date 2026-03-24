use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

/// Top-level configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Address to listen for Postgres client connections.
    pub listen_addr: String,

    /// Address to listen for admin HTTP server.
    pub admin_listen_addr: String,

    /// Default upstream Postgres host (clients provide db/user/password).
    pub upstream_host: String,

    /// Default upstream Postgres port.
    pub upstream_port: u16,

    /// Pool configuration.
    pub pool: PoolConfig,

    /// TLS configuration.
    pub tls: TlsConfig,

    /// DB size monitor configuration.
    pub monitor: MonitorConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PoolConfig {
    /// Max server connections per pool key (host, port, db, role).
    pub max_connections_per_pool: u32,

    /// Global max server connections across all pools.
    pub max_total_connections: u32,

    /// How long to wait for a server connection before timing out (ms).
    pub acquire_timeout_ms: u64,

    /// Idle server connection timeout before eviction (seconds).
    pub idle_timeout_secs: u64,

    /// Health check interval for idle connections (seconds).
    pub health_check_interval_secs: u64,

    /// Max client connections waiting in queue per pool.
    pub max_queue_size: u32,

    /// Connection lifetime before forced recycling (seconds).
    pub max_connection_lifetime_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    /// Enable TLS on the client-facing side.
    pub enabled: bool,

    /// Path to TLS certificate (PEM).
    pub cert_path: String,

    /// Path to TLS private key (PEM).
    pub key_path: String,

    /// Require TLS from clients (reject plaintext).
    pub require_tls: bool,

    /// Use TLS when connecting to upstream Postgres.
    pub upstream_tls: bool,

    /// Accept invalid certs from upstream (for self-signed).
    pub upstream_tls_accept_invalid: bool,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: String::new(),
            key_path: String::new(),
            require_tls: false,
            upstream_tls: true,
            upstream_tls_accept_invalid: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MonitorConfig {
    /// How often to check DB sizes (seconds).
    pub check_interval_secs: u64,

    /// Default max DB size if not specified in connection string (bytes, 0 = unlimited).
    pub default_max_db_size_bytes: u64,

    /// Allow DELETE/TRUNCATE even when DB is over limit.
    pub allow_shrink_operations_when_overlimit: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:5433".to_string(),
            admin_listen_addr: "0.0.0.0:9090".to_string(),
            upstream_host: "localhost".to_string(),
            upstream_port: 5432,
            pool: PoolConfig::default(),
            tls: TlsConfig::default(),
            monitor: MonitorConfig::default(),
        }
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections_per_pool: 20,
            max_total_connections: 500,
            acquire_timeout_ms: 5000,
            idle_timeout_secs: 300,
            health_check_interval_secs: 30,
            max_queue_size: 1000,
            max_connection_lifetime_secs: 3600,
        }
    }
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            check_interval_secs: 60,
            default_max_db_size_bytes: 0,
            allow_shrink_operations_when_overlimit: true,
        }
    }
}

impl Config {
    pub fn load(
        path: &str,
        listen_override: Option<&str>,
        admin_override: Option<&str>,
    ) -> Result<Self> {
        let mut cfg = if Path::new(path).exists() {
            let contents = std::fs::read_to_string(path)
                .with_context(|| format!("Reading config {}", path))?;
            toml::from_str::<Config>(&contents)
                .with_context(|| format!("Parsing config {}", path))?
        } else {
            Config::default()
        };

        // CLI overrides
        if let Some(listen) = listen_override {
            cfg.listen_addr = listen.to_string();
        }
        if let Some(admin) = admin_override {
            cfg.admin_listen_addr = admin.to_string();
        }

        // Env var overrides
        if let Ok(v) = std::env::var("PG_MUX_LISTEN") {
            cfg.listen_addr = v;
        }
        if let Ok(v) = std::env::var("PG_MUX_ADMIN_LISTEN") {
            cfg.admin_listen_addr = v;
        }
        if let Ok(v) = std::env::var("PG_MUX_UPSTREAM_HOST") {
            cfg.upstream_host = v;
        }
        if let Ok(v) = std::env::var("PG_MUX_UPSTREAM_PORT") {
            if let Ok(p) = v.parse() {
                cfg.upstream_port = p;
            }
        }
        if let Ok(v) = std::env::var("PG_MUX_TLS_CERT") {
            cfg.tls.cert_path = v;
            cfg.tls.enabled = true;
        }
        if let Ok(v) = std::env::var("PG_MUX_TLS_KEY") {
            cfg.tls.key_path = v;
        }
        if let Ok(v) = std::env::var("PG_MUX_UPSTREAM_TLS") {
            cfg.tls.upstream_tls = v
                .parse()
                .unwrap_or(v == "1" || v.eq_ignore_ascii_case("true"));
        }

        Ok(cfg)
    }
}

/// Parse a max_db_size parameter from a connection string or startup params.
/// Supports suffixes: KB, MB, GB, TB (case insensitive).
pub fn parse_size_limit(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() || s == "0" {
        return None;
    }

    let s_upper = s.to_uppercase();
    let (num_str, multiplier) = if s_upper.ends_with("TB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024 * 1024)
    } else if s_upper.ends_with("GB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024)
    } else if s_upper.ends_with("MB") {
        (&s[..s.len() - 2], 1024u64 * 1024)
    } else if s_upper.ends_with("KB") {
        (&s[..s.len() - 2], 1024u64)
    } else {
        (s, 1u64)
    };

    num_str.trim().parse::<u64>().ok().map(|n| n * multiplier)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size_limit() {
        assert_eq!(parse_size_limit("5GB"), Some(5 * 1024 * 1024 * 1024));
        assert_eq!(parse_size_limit("100MB"), Some(100 * 1024 * 1024));
        assert_eq!(parse_size_limit("1TB"), Some(1024 * 1024 * 1024 * 1024));
        assert_eq!(parse_size_limit("1024KB"), Some(1024 * 1024));
        assert_eq!(parse_size_limit("1048576"), Some(1048576));
        assert_eq!(parse_size_limit("0"), None);
        assert_eq!(parse_size_limit(""), None);
        assert_eq!(parse_size_limit("5gb"), Some(5 * 1024 * 1024 * 1024));
    }

    #[test]
    fn test_default_config() {
        let cfg = Config::default();
        assert_eq!(cfg.listen_addr, "0.0.0.0:5433");
        assert_eq!(cfg.upstream_port, 5432);
        assert_eq!(cfg.pool.max_connections_per_pool, 20);
    }

    #[test]
    fn test_upstream_tls_env_override() {
        std::env::set_var("PG_MUX_UPSTREAM_TLS", "false");
        let cfg = Config::load("nonexistent.toml", None, None).unwrap();
        assert!(!cfg.tls.upstream_tls);
        std::env::remove_var("PG_MUX_UPSTREAM_TLS");
    }

    #[test]
    fn test_default_upstream_tls_is_true() {
        let cfg = Config::default();
        assert!(cfg.tls.upstream_tls);
    }
}
