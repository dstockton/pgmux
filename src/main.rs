mod admin;
mod auth;
mod config;
mod monitor;
mod pool;
mod protocol;
mod tls;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(
    name = "pgmux",
    about = "Multi-tenant connection multiplexing for Postgres"
)]
struct Cli {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    /// Listen address for Postgres protocol
    #[arg(short, long)]
    listen: Option<String>,

    /// Listen address for admin HTTP server
    #[arg(short, long)]
    admin_listen: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pgmux=info".into()),
        )
        .init();

    let cli = Cli::parse();

    let cfg = config::Config::load(
        &cli.config,
        cli.listen.as_deref(),
        cli.admin_listen.as_deref(),
    )?;
    let cfg = Arc::new(cfg);

    info!(
        listen = %cfg.listen_addr,
        admin = %cfg.admin_listen_addr,
        "Starting pgmux"
    );

    let metrics = Arc::new(admin::metrics::Metrics::new());
    let pool_manager = Arc::new(pool::PoolManager::new(cfg.clone(), metrics.clone()));
    let size_monitor = Arc::new(monitor::DbSizeMonitor::new(
        cfg.clone(),
        pool_manager.clone(),
        metrics.clone(),
    ));

    // Start the DB size monitor background task
    let _monitor_handle = {
        let monitor = size_monitor.clone();
        tokio::spawn(async move {
            monitor.run().await;
        })
    };

    // Start the admin HTTP server
    let _admin_handle = {
        let cfg = cfg.clone();
        let metrics = metrics.clone();
        let pool_mgr = pool_manager.clone();
        let monitor = size_monitor.clone();
        tokio::spawn(async move {
            if let Err(e) = admin::server::run(cfg, metrics, pool_mgr, monitor).await {
                error!("Admin server error: {}", e);
            }
        })
    };

    // Start the main Postgres listener
    let tls_acceptor = if cfg.tls.enabled {
        Some(tls::build_server_tls_acceptor(&cfg.tls)?)
    } else {
        None
    };

    let listener = tokio::net::TcpListener::bind(&cfg.listen_addr).await?;
    info!("Listening for Postgres connections on {}", cfg.listen_addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        let pool_mgr = pool_manager.clone();
        let m = metrics.clone();
        let sz_monitor = size_monitor.clone();
        let tls_acc = tls_acceptor.clone();
        let cfg = cfg.clone();

        tokio::spawn(async move {
            m.client_connections_total.inc();
            m.client_connections_active.inc();

            if let Err(e) =
                protocol::handle_client(stream, addr, pool_mgr, sz_monitor, tls_acc, cfg).await
            {
                tracing::debug!("Client {} disconnected: {}", addr, e);
            }

            m.client_connections_active.dec();
        });
    }
}
