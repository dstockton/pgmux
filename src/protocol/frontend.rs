use anyhow::{bail, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

use super::messages::*;
use super::{ClientStartupInfo, PoolKey};
use crate::config::{self, Config};
use crate::monitor::DbSizeMonitor;
use crate::pool::PoolManager;

/// Shared buffers passed between fast and slow proxy loops to prevent
/// data loss when transitioning mid-stream.
struct ProxyBuffers {
    client_buf: BytesMut,
    server_buf: BytesMut,
}

impl ProxyBuffers {
    fn new() -> Self {
        Self {
            client_buf: BytesMut::with_capacity(8192),
            server_buf: BytesMut::with_capacity(8192),
        }
    }
}

/// Result of a proxy loop — client disconnected or mode switch needed.
enum ProxyLoopExit {
    ClientDisconnected,
    SwitchToSlow,
    SwitchToFast,
}

/// Handle a new client connection.
pub async fn handle_client(
    mut stream: TcpStream,
    addr: SocketAddr,
    pool_manager: Arc<PoolManager>,
    size_monitor: Arc<DbSizeMonitor>,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    cfg: Arc<Config>,
) -> Result<()> {
    stream.set_nodelay(true)?;
    debug!("New client connection from {}", addr);

    let mut buf = BytesMut::with_capacity(8192);

    // Phase 1: Read initial message (could be SSL request or startup)
    let startup_info = loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            bail!("Client disconnected before startup");
        }

        if buf.len() < 4 {
            continue;
        }

        let len = (&buf[0..4]).get_i32();
        if (buf.len() as i32) < len {
            continue;
        }

        // Check for SSL request
        if buf.len() >= 8 {
            let code = (&buf[4..8]).get_i32();
            if code == SSL_REQUEST_CODE {
                buf.advance(8);
                if let Some(ref _acceptor) = tls_acceptor {
                    // Accept SSL
                    stream.write_all(b"S").await?;
                    // For now, continue without actual TLS upgrade in this prototype
                    // Full TLS would wrap the stream here
                    warn!("TLS requested but full TLS handshake not yet implemented; continuing plaintext");
                    continue;
                } else {
                    // Reject SSL, client should retry without
                    stream.write_all(b"N").await?;
                    buf.clear();
                    continue;
                }
            }

            if code == CANCEL_REQUEST_CODE {
                debug!("Cancel request from {}", addr);
                // Forward cancel to the right backend
                // Cancel: 4 bytes length, 4 bytes code, 4 bytes PID, 4 bytes secret
                return Ok(());
            }
        }

        // Parse startup message
        let msg_len = (&buf[0..4]).get_i32() as usize;
        if buf.len() < msg_len {
            continue;
        }
        buf.advance(4);
        let payload = buf.split_to(msg_len - 4);
        let (version, params) = parse_startup_params(&payload)?;

        if version != 196608 {
            let err = build_error_response(
                "FATAL",
                "08004",
                &format!("Unsupported protocol version: {}", version),
            );
            stream.write_all(&err).await?;
            bail!("Unsupported protocol version: {}", version);
        }

        let user = params.get("user").cloned().unwrap_or_default();
        let database = params
            .get("database")
            .cloned()
            .unwrap_or_else(|| user.clone());
        let application_name = params.get("application_name").cloned().unwrap_or_default();

        // Check for max_db_size in options or as a custom parameter
        let max_db_size = params
            .get("max_db_size")
            .and_then(|v| config::parse_size_limit(v))
            .or_else(|| {
                params.get("options").and_then(|opts| {
                    for part in opts.split_whitespace() {
                        if let Some(val) = part.strip_prefix("--max_db_size=") {
                            return config::parse_size_limit(val);
                        }
                        if let Some(val) = part.strip_prefix("-c max_db_size=") {
                            return config::parse_size_limit(val);
                        }
                    }
                    None
                })
            });

        let extra_params: Vec<(String, String)> = params
            .iter()
            .filter(|(k, _)| !matches!(k.as_str(), "user" | "database" | "max_db_size" | "options"))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        break ClientStartupInfo {
            user,
            database,
            password: String::new(), // will be filled during auth
            max_db_size,
            application_name,
            extra_params,
        };
    };

    // Phase 2: Authenticate the client (cleartext password for now)
    let auth_req = build_auth_cleartext_request();
    stream.write_all(&auth_req).await?;

    let password = loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            bail!("Client disconnected during auth");
        }
        if let Some(msg) = try_read_message(&mut buf, false)? {
            if msg.msg_type == b'p' {
                // Password message: null-terminated string
                if let Some(null_pos) = msg.payload.iter().position(|&b| b == 0) {
                    break std::str::from_utf8(&msg.payload[..null_pos])?.to_string();
                }
                bail!("Invalid password message");
            }
            bail!("Expected password message, got '{}'", msg.msg_type as char);
        }
    };

    let mut startup_info = startup_info;
    startup_info.password = password;

    let pool_key = PoolKey {
        host: cfg.upstream_host.clone(),
        port: cfg.upstream_port,
        database: startup_info.database.clone(),
        user: startup_info.user.clone(),
    };

    // Phase 3: Acquire a backend connection and authenticate
    let mut server_conn = match pool_manager
        .acquire(
            &pool_key,
            &startup_info.password,
            &startup_info.extra_params,
        )
        .await
    {
        Ok(conn) => conn,
        Err(e) => {
            let err =
                build_error_response("FATAL", "28000", &format!("Backend auth failed: {}", e));
            stream.write_all(&err).await?;
            bail!("Backend connection failed: {}", e);
        }
    };

    // Register the DB size limit if specified
    if let Some(max_size) = startup_info.max_db_size {
        size_monitor.register_limit(&pool_key.database, max_size);
    }

    // Send AuthenticationOk to client
    let auth_ok = build_auth_ok();
    stream.write_all(&auth_ok).await?;

    // Send parameter statuses to client
    let params_to_send = [
        ("server_version", "16.0"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", "UTC"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
    ];
    for (name, value) in &params_to_send {
        let ps = build_parameter_status(name, value);
        stream.write_all(&ps).await?;
    }

    // Send BackendKeyData
    let bkd = build_backend_key_data(std::process::id() as i32, rand::random::<i32>());
    stream.write_all(&bkd).await?;

    // Send ReadyForQuery
    let rfq = build_ready_for_query(b'I');
    stream.write_all(&rfq).await?;
    stream.flush().await?;

    info!(
        user = %startup_info.user,
        db = %startup_info.database,
        addr = %addr,
        "Client authenticated, starting proxy"
    );

    // Phase 4: Proxy loop — relay messages between client and backend
    proxy_loop(
        &mut stream,
        &mut server_conn,
        &pool_key,
        &size_monitor,
        &cfg,
        &startup_info,
    )
    .await?;

    // Return connection to pool
    pool_manager.release(pool_key, server_conn).await;

    Ok(())
}

/// Fast proxy loop: forward raw bytes without message parsing.
/// Scans server→client stream for ReadyForQuery to track transaction state.
/// Returns SwitchToSlow when the over_limit flag is set and we're at an idle boundary.
async fn fast_proxy_loop(
    client: &mut TcpStream,
    server: &mut TcpStream,
    over_limit: &Arc<AtomicBool>,
    scanner: &mut MessageBoundaryScanner,
    bufs: &mut ProxyBuffers,
) -> Result<ProxyLoopExit> {
    let mut transition_pending = false;

    loop {
        tokio::select! {
            result = client.read_buf(&mut bufs.client_buf) => {
                let n = result?;
                if n == 0 {
                    return Ok(ProxyLoopExit::ClientDisconnected);
                }

                // Check the over-limit flag
                if !transition_pending && over_limit.load(Ordering::Relaxed) {
                    transition_pending = true;
                }

                // Forward raw bytes to server
                server.write_all(&bufs.client_buf).await?;
                bufs.client_buf.clear();
                server.flush().await?;
            }

            result = server.read_buf(&mut bufs.server_buf) => {
                let n = result?;
                if n == 0 {
                    return Ok(ProxyLoopExit::ClientDisconnected);
                }

                // Scan for ReadyForQuery to track transaction state
                let saw_idle = scanner.scan(&bufs.server_buf);

                // Forward raw bytes to client
                client.write_all(&bufs.server_buf).await?;
                bufs.server_buf.clear();
                client.flush().await?;

                // Transition to slow path at idle boundary
                if transition_pending && saw_idle {
                    return Ok(ProxyLoopExit::SwitchToSlow);
                }
            }
        }
    }
}

/// Main proxy loop: relay messages between client and server.
async fn proxy_loop(
    client: &mut TcpStream,
    server: &mut TcpStream,
    pool_key: &PoolKey,
    size_monitor: &DbSizeMonitor,
    cfg: &Config,
    startup_info: &ClientStartupInfo,
) -> Result<()> {
    let mut client_buf = BytesMut::with_capacity(8192);
    let mut server_buf = BytesMut::with_capacity(8192);
    let mut _in_transaction = false;
    let mut transaction_read_only_injected = false;

    loop {
        tokio::select! {
            // Read from client
            result = client.read_buf(&mut client_buf) => {
                let n = result?;
                if n == 0 {
                    return Ok(()); // Client disconnected
                }

                // Process client messages
                while let Some(msg) = try_read_message(&mut client_buf, false)? {
                    let should_inject_read_only = check_should_enforce_read_only(
                        &msg, pool_key, size_monitor, cfg, startup_info,
                    );

                    if should_inject_read_only {
                        // Check if this is a shrink operation that should be allowed
                        let is_shrink = extract_query_text(&msg)
                            .map(|q| is_shrink_operation(&q))
                            .unwrap_or(false);

                        if is_shrink && cfg.monitor.allow_shrink_operations_when_overlimit {
                            // Allow shrink operations through
                            let mut forward_buf = BytesMut::new();
                            msg.encode(&mut forward_buf);
                            server.write_all(&forward_buf).await?;
                        } else if msg.msg_type == b'Q' {
                            // Simple query — wrap in read-only transaction
                            if let Some(query_text) = extract_query_text(&msg) {
                                // Send a notice to the client
                                if !transaction_read_only_injected {
                                    let notice = build_notice_response(
                                        "WARNING",
                                        "53400",
                                        &format!(
                                            "Database '{}' has exceeded its size limit. Write operations are restricted. DELETE/TRUNCATE are still allowed.",
                                            pool_key.database
                                        ),
                                    );
                                    client.write_all(&notice).await?;
                                    transaction_read_only_injected = true;
                                }

                                // Inject SET TRANSACTION READ ONLY before the query
                                let wrapped = format!("SET TRANSACTION READ ONLY; {}", query_text);
                                let mut payload = BytesMut::new();
                                payload.extend_from_slice(wrapped.as_bytes());
                                payload.put_u8(0);
                                let wrapped_msg = PgMessage::new(b'Q', payload);
                                let mut forward_buf = BytesMut::new();
                                wrapped_msg.encode(&mut forward_buf);
                                server.write_all(&forward_buf).await?;
                            }
                        } else {
                            // Extended query protocol — forward as-is for now
                            // TODO: intercept Parse messages to inject read-only
                            let mut forward_buf = BytesMut::new();
                            msg.encode(&mut forward_buf);
                            server.write_all(&forward_buf).await?;
                        }
                    } else {
                        // Forward message as-is
                        let mut forward_buf = BytesMut::new();
                        msg.encode(&mut forward_buf);
                        server.write_all(&forward_buf).await?;
                        transaction_read_only_injected = false;
                    }
                }
                server.flush().await?;
            }

            // Read from server
            result = server.read_buf(&mut server_buf) => {
                let n = result?;
                if n == 0 {
                    return Ok(()); // Server disconnected
                }

                // Process server messages for transaction state tracking
                while let Some(msg) = try_read_message(&mut server_buf, false)? {
                    // Track transaction state from ReadyForQuery
                    if msg.msg_type == b'Z' && !msg.payload.is_empty() {
                        match msg.payload[0] {
                            b'I' => _in_transaction = false,
                            b'T' => _in_transaction = true,
                            b'E' => _in_transaction = true, // error in transaction
                            _ => {}
                        }
                    }

                    // Forward to client
                    let mut forward_buf = BytesMut::new();
                    msg.encode(&mut forward_buf);
                    client.write_all(&forward_buf).await?;
                }
                client.flush().await?;
            }
        }
    }
}

/// Check whether we should enforce read-only mode for this query.
fn check_should_enforce_read_only(
    msg: &PgMessage,
    pool_key: &PoolKey,
    size_monitor: &DbSizeMonitor,
    cfg: &Config,
    startup_info: &ClientStartupInfo,
) -> bool {
    // Only applies to query messages
    if msg.msg_type != b'Q' && msg.msg_type != b'P' {
        return false;
    }

    let max_size = startup_info
        .max_db_size
        .or(if cfg.monitor.default_max_db_size_bytes > 0 {
            Some(cfg.monitor.default_max_db_size_bytes)
        } else {
            None
        });

    if let Some(limit) = max_size {
        if let Some(current_size) = size_monitor.get_db_size(&pool_key.database) {
            return current_size > limit;
        }
    }

    false
}
