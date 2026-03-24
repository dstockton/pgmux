# Two-Tier Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce proxy overhead from ~2ms to near-zero by forwarding raw bytes for tenants under their size limit, switching to full message parsing only when enforcement is needed.

**Architecture:** Two proxy loops — `fast_proxy_loop` (raw byte forwarding with message-boundary-aware Z-scanning) and `slow_proxy_loop` (optimized version of current parsing loop). An outer loop in `handle_client` drives transitions between them based on a per-session `AtomicBool` flag set by the `DbSizeMonitor`. Shared `ProxyBuffers` struct is passed between loops to prevent data loss on transitions.

**Tech Stack:** Rust, tokio, bytes

**Spec:** `docs/superpowers/specs/2026-03-24-two-tier-proxy-design.md`

---

## File Map

### Modified
- `src/protocol/frontend.rs` — New `fast_proxy_loop`, refactored `slow_proxy_loop`, message-boundary scanner, outer transition loop in `handle_client`
- `src/protocol/messages.rs` — `try_read_message` returns original byte range for zero-copy forwarding
- `src/monitor/mod.rs` — Per-session limit registry with `AtomicBool` flags, `register_session`/`unregister_session`
- `bench/multi_tenant_bench.py` — Add `--over-limit` test mode

---

## Task 1: Per-Session Over-Limit Flags in DbSizeMonitor

Add the session registry that the fast path will check. This is independent of the proxy changes and can be built and tested first.

**Files:**
- Modify: `src/monitor/mod.rs`

- [ ] **Step 1: Add session registry types and fields**

Add to `src/monitor/mod.rs`, above the `DbSizeMonitor` struct:

```rust
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static SESSION_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

struct SessionLimit {
    database: String,
    limit_bytes: u64,
    over_limit: Arc<AtomicBool>,
}
```

Add a new field to `DbSizeMonitor`:
```rust
sessions: DashMap<u64, SessionLimit>,
```

Initialize it in `new()`:
```rust
sessions: DashMap::new(),
```

- [ ] **Step 2: Implement register_session / unregister_session**

Add to `impl DbSizeMonitor`:

```rust
/// Register a session with a size limit. Returns (session_id, over_limit_flag).
/// The flag is set/cleared by check_all_sizes on each monitor cycle.
/// Sessions without a limit (limit_bytes == 0) get a flag that is never set.
pub fn register_session(&self, database: &str, limit_bytes: u64) -> (u64, Arc<AtomicBool>) {
    let id = SESSION_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let flag = Arc::new(AtomicBool::new(false));
    // Check current state immediately so the flag is correct from the start
    if limit_bytes > 0 {
        if let Some(current_size) = self.get_db_size(database) {
            if current_size > limit_bytes {
                flag.store(true, Ordering::Relaxed);
            }
        }
    }
    self.sessions.insert(id, SessionLimit {
        database: database.to_string(),
        limit_bytes,
        over_limit: flag.clone(),
    });
    (id, flag)
}

/// Unregister a session when the connection closes.
pub fn unregister_session(&self, id: u64) {
    self.sessions.remove(&id);
}
```

- [ ] **Step 3: Update check_all_sizes to set/clear session flags**

At the end of `check_all_sizes`, after the existing per-database size check loop, add:

```rust
// Update per-session over-limit flags
for entry in self.sessions.iter() {
    let session = entry.value();
    if session.limit_bytes == 0 {
        continue;
    }
    if let Some(size) = self.db_sizes.get(&session.database) {
        session.over_limit.store(*size > session.limit_bytes, Ordering::Relaxed);
    }
}
```

- [ ] **Step 4: Add unit test for session registry**

Add to the `tests` module in `src/monitor/mod.rs`:

```rust
#[test]
fn test_session_registry() {
    use std::sync::atomic::Ordering;

    // Can't easily test with real DB, but can test the registry mechanics
    let monitor = DbSizeMonitor {
        cfg: Arc::new(crate::config::Config::default()),
        pool_manager: Arc::new(crate::pool::PoolManager::new(
            Arc::new(crate::config::Config::default()),
            Arc::new(crate::admin::metrics::Metrics::new()),
        )),
        metrics: Arc::new(crate::admin::metrics::Metrics::new()),
        db_sizes: DashMap::new(),
        db_limits: DashMap::new(),
        sessions: DashMap::new(),
    };

    // Insert a known size
    monitor.db_sizes.insert("testdb".to_string(), 500);

    // Register session with limit above current size
    let (id1, flag1) = monitor.register_session("testdb", 1000);
    assert!(!flag1.load(Ordering::Relaxed));

    // Register session with limit below current size
    let (id2, flag2) = monitor.register_session("testdb", 100);
    assert!(flag2.load(Ordering::Relaxed));

    // Unregister
    monitor.unregister_session(id1);
    monitor.unregister_session(id2);
    assert!(monitor.sessions.is_empty());
}
```

- [ ] **Step 5: Commit**

```
git add src/monitor/mod.rs
git commit -m "Add per-session over-limit flags to DbSizeMonitor"
```

---

## Task 2: Message-Boundary Scanner for Fast Path

A lightweight scanner that skips through PG wire protocol messages by reading type+length headers, only inspecting `ReadyForQuery` payloads. Handles messages split across TCP reads.

**Files:**
- Modify: `src/protocol/messages.rs` (add scanner at bottom of file)

- [ ] **Step 1: Define scanner state**

Add to `src/protocol/messages.rs`:

```rust
/// Lightweight message-boundary scanner for the fast proxy path.
/// Tracks position within the PG wire protocol stream without full message parsing.
/// Only inspects ReadyForQuery messages for transaction state.
pub struct MessageBoundaryScanner {
    /// Bytes remaining to skip in the current message payload.
    remaining_skip: usize,
    /// Partial header bytes carried from previous read.
    header_buf: [u8; 5],
    /// How many header bytes we have buffered.
    header_len: usize,
    /// Last observed transaction status from ReadyForQuery (b'I', b'T', b'E').
    pub transaction_status: u8,
}

impl MessageBoundaryScanner {
    pub fn new() -> Self {
        Self {
            remaining_skip: 0,
            header_buf: [0u8; 5],
            header_len: 0,
            transaction_status: b'I',
        }
    }

    /// Scan a buffer of bytes from the server→client direction.
    /// Updates transaction_status when ReadyForQuery messages are found.
    /// Returns true if the most recent ReadyForQuery had status 'I' (idle).
    pub fn scan(&mut self, data: &[u8]) -> bool {
        let mut pos = 0;
        let mut saw_idle = false;

        while pos < data.len() {
            // Phase 1: skip remaining payload bytes from previous message
            if self.remaining_skip > 0 {
                let skip = std::cmp::min(self.remaining_skip, data.len() - pos);
                // If this is a ReadyForQuery (remaining_skip == 1 and we have the status byte)
                // we already handled it when we parsed the header
                pos += skip;
                self.remaining_skip -= skip;
                continue;
            }

            // Phase 2: accumulate header bytes (type + 4-byte length = 5 bytes)
            if self.header_len < 5 {
                let need = 5 - self.header_len;
                let avail = std::cmp::min(need, data.len() - pos);
                self.header_buf[self.header_len..self.header_len + avail]
                    .copy_from_slice(&data[pos..pos + avail]);
                self.header_len += avail;
                pos += avail;

                if self.header_len < 5 {
                    break; // need more data
                }
            }

            // Phase 3: we have a full 5-byte header
            let msg_type = self.header_buf[0];
            let len = i32::from_be_bytes([
                self.header_buf[1],
                self.header_buf[2],
                self.header_buf[3],
                self.header_buf[4],
            ]) as usize;

            // Payload length = len - 4 (length field includes itself)
            let payload_len = if len >= 4 { len - 4 } else { 0 };

            if msg_type == b'Z' && payload_len == 1 {
                // ReadyForQuery — need 1 byte of payload for transaction status
                if pos < data.len() {
                    self.transaction_status = data[pos];
                    pos += 1;
                    self.remaining_skip = 0;
                    saw_idle = self.transaction_status == b'I';
                } else {
                    // Status byte will be in next read — set remaining_skip = 1
                    // but we need special handling. For simplicity, just
                    // treat it as a regular payload skip. The next scan will
                    // skip 1 byte but won't read the status. This is safe:
                    // we'll catch the status on the next ReadyForQuery.
                    self.remaining_skip = 1;
                }
            } else {
                self.remaining_skip = payload_len;
            }

            self.header_len = 0; // reset for next message
        }

        saw_idle
    }
}
```

- [ ] **Step 2: Add unit tests for the scanner**

Add to the `tests` module in `src/protocol/messages.rs`:

```rust
#[test]
fn test_scanner_single_ready_for_query() {
    let mut scanner = MessageBoundaryScanner::new();
    // ReadyForQuery: Z, len=5, status=I
    let data = [0x5A, 0x00, 0x00, 0x00, 0x05, b'I'];
    assert!(scanner.scan(&data));
    assert_eq!(scanner.transaction_status, b'I');
}

#[test]
fn test_scanner_transaction_state_tracking() {
    let mut scanner = MessageBoundaryScanner::new();
    // ReadyForQuery with T (in transaction)
    let data = [0x5A, 0x00, 0x00, 0x00, 0x05, b'T'];
    assert!(!scanner.scan(&data));
    assert_eq!(scanner.transaction_status, b'T');

    // ReadyForQuery with I (idle)
    let data = [0x5A, 0x00, 0x00, 0x00, 0x05, b'I'];
    assert!(scanner.scan(&data));
    assert_eq!(scanner.transaction_status, b'I');
}

#[test]
fn test_scanner_skips_non_z_messages() {
    let mut scanner = MessageBoundaryScanner::new();
    // DataRow message: D, len=10, 6 bytes payload, then ReadyForQuery
    let mut data = Vec::new();
    data.push(b'D');
    data.extend_from_slice(&8i32.to_be_bytes()); // len=8, payload=4
    data.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]); // 4 bytes payload
    // Then a ReadyForQuery
    data.extend_from_slice(&[0x5A, 0x00, 0x00, 0x00, 0x05, b'I']);
    assert!(scanner.scan(&data));
    assert_eq!(scanner.transaction_status, b'I');
}

#[test]
fn test_scanner_split_across_reads() {
    let mut scanner = MessageBoundaryScanner::new();
    // Split ReadyForQuery across two reads: header split
    let part1 = [0x5A, 0x00, 0x00]; // partial header
    let part2 = [0x00, 0x05, b'I']; // rest of header + status

    assert!(!scanner.scan(&part1)); // not enough data
    assert!(scanner.scan(&part2)); // completes the message
    assert_eq!(scanner.transaction_status, b'I');
}

#[test]
fn test_scanner_large_message_then_rfq() {
    let mut scanner = MessageBoundaryScanner::new();
    // A large message (100 bytes payload) followed by ReadyForQuery
    let mut data = Vec::new();
    data.push(b'T'); // RowDescription
    data.extend_from_slice(&104i32.to_be_bytes()); // len=104, payload=100
    data.extend_from_slice(&vec![0u8; 100]);
    data.extend_from_slice(&[0x5A, 0x00, 0x00, 0x00, 0x05, b'I']);

    assert!(scanner.scan(&data));
    assert_eq!(scanner.transaction_status, b'I');
}

#[test]
fn test_scanner_payload_split_across_reads() {
    let mut scanner = MessageBoundaryScanner::new();
    // Large message split: header + partial payload in read 1, rest + RFQ in read 2
    let mut part1 = Vec::new();
    part1.push(b'D');
    part1.extend_from_slice(&24i32.to_be_bytes()); // len=24, payload=20
    part1.extend_from_slice(&vec![0xAA; 10]); // only 10 of 20 payload bytes

    assert!(!scanner.scan(&part1));
    assert_eq!(scanner.remaining_skip, 10);

    let mut part2 = Vec::new();
    part2.extend_from_slice(&vec![0xBB; 10]); // remaining 10 payload bytes
    part2.extend_from_slice(&[0x5A, 0x00, 0x00, 0x00, 0x05, b'I']); // RFQ

    assert!(scanner.scan(&part2));
    assert_eq!(scanner.transaction_status, b'I');
}
```

- [ ] **Step 3: Commit**

```
git add src/protocol/messages.rs
git commit -m "Add message-boundary scanner for fast proxy path"
```

---

## Task 3: Zero-Copy Forwarding Support in try_read_message

Change `try_read_message` to return the original byte range so the slow path can forward unmodified messages without re-encoding.

**Files:**
- Modify: `src/protocol/messages.rs`

- [ ] **Step 1: Add try_read_message_with_raw variant**

Add a new function below the existing `try_read_message`. Do NOT modify the existing function (callers outside the proxy loop still use it).

```rust
/// Like try_read_message, but also returns the raw bytes of the message
/// (type + length + payload) for zero-copy forwarding.
/// The raw bytes are split from the front of buf before parsing.
pub fn try_read_message_with_raw(buf: &mut BytesMut) -> Result<Option<(PgMessage, BytesMut)>> {
    if buf.len() < 5 {
        return Ok(None);
    }
    let msg_type = buf[0];
    let len = (&buf[1..5]).get_i32() as usize;
    if !(4..=MAX_MESSAGE_SIZE).contains(&len) {
        bail!(
            "Invalid message length: {} for type '{}'",
            len,
            msg_type as char
        );
    }
    let total_len = 1 + len;
    if buf.len() < total_len {
        return Ok(None);
    }
    // Split off the raw bytes first
    let raw = buf.split_to(total_len);
    // Parse the message from the raw copy
    let payload = BytesMut::from(&raw[5..]);
    let msg = PgMessage { msg_type, payload };
    Ok(Some((msg, raw)))
}
```

- [ ] **Step 2: Add test**

```rust
#[test]
fn test_try_read_message_with_raw() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    let query = b"SELECT 1\0";
    buf.put_i32((query.len() + 4) as i32);
    buf.extend_from_slice(query);

    let original_bytes = buf.to_vec();
    let (msg, raw) = try_read_message_with_raw(&mut buf).unwrap().unwrap();
    assert_eq!(msg.msg_type, b'Q');
    assert_eq!(raw.as_ref(), &original_bytes[..]);
    assert!(buf.is_empty());
}
```

- [ ] **Step 3: Commit**

```
git add src/protocol/messages.rs
git commit -m "Add try_read_message_with_raw for zero-copy forwarding"
```

---

## Task 4: Fast Proxy Loop

The core performance change. Raw byte forwarding with message-boundary scanning.

**Files:**
- Modify: `src/protocol/frontend.rs`

- [ ] **Step 1: Add imports, shared buffers struct, and ProxyLoopExit enum**

At the top of `frontend.rs`, add:

```rust
use std::sync::atomic::{AtomicBool, Ordering};
```

Below the existing imports, add:

```rust
/// Shared buffers passed between fast and slow proxy loops to prevent
/// data loss when transitioning. TCP reads may deliver partial messages;
/// orphaning these bytes would corrupt the stream.
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

/// Result of a proxy loop — either the client disconnected or we need to switch modes.
enum ProxyLoopExit {
    ClientDisconnected,
    SwitchToSlow,
    SwitchToFast,
}
```

- [ ] **Step 2: Implement fast_proxy_loop**

Add a new function. Note: `over_limit` takes `&Arc<AtomicBool>` (not `&AtomicBool`) because the caller holds an `Arc<AtomicBool>` and `&Arc<AtomicBool>` does not auto-deref to `&AtomicBool`.

```rust
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
```

- [ ] **Step 3: Commit**

```
git add src/protocol/frontend.rs
git commit -m "Add fast_proxy_loop with raw byte forwarding"
```

---

## Task 5: Optimized Slow Proxy Loop

Refactor the existing `proxy_loop` with buffer reuse, zero-copy forwarding, and transition support. This is a full replacement — read the current `proxy_loop` (lines 240-382 of `frontend.rs`) before starting.

**Files:**
- Modify: `src/protocol/frontend.rs`

- [ ] **Step 1: Refactor proxy_loop into slow_proxy_loop**

Delete the current `proxy_loop` and `check_should_enforce_read_only` functions. Replace with the complete `slow_proxy_loop` below. Key changes vs the old code:
- Uses shared `ProxyBuffers` (not local buffers) to prevent data loss on transitions
- Takes `&Arc<AtomicBool>` for over-limit flag (matches fast loop)
- Uses `try_read_message_with_raw` for zero-copy forwarding of unmodified messages
- Single reusable `forward_buf` instead of per-message allocation
- Fast-exits for non-query message types (skips the entire enforcement check)
- Checks for slow→fast transition at idle `ReadyForQuery` boundaries
- Returns `ProxyLoopExit` instead of `Result<()>`

```rust
/// Slow proxy loop: full message parsing with read-only enforcement.
/// Used when a tenant's database exceeds its size limit.
async fn slow_proxy_loop(
    client: &mut TcpStream,
    server: &mut TcpStream,
    pool_key: &PoolKey,
    size_monitor: &DbSizeMonitor,
    cfg: &Config,
    startup_info: &ClientStartupInfo,
    over_limit: &Arc<AtomicBool>,
    scanner: &mut MessageBoundaryScanner,
    bufs: &mut ProxyBuffers,
) -> Result<ProxyLoopExit> {
    let mut forward_buf = BytesMut::with_capacity(8192);
    let mut transaction_read_only_injected = false;

    loop {
        tokio::select! {
            result = client.read_buf(&mut bufs.client_buf) => {
                let n = result?;
                if n == 0 {
                    return Ok(ProxyLoopExit::ClientDisconnected);
                }

                while let Some((msg, raw)) = try_read_message_with_raw(&mut bufs.client_buf)? {
                    // Fast exit for non-query types — forward raw, skip enforcement
                    if msg.msg_type != b'Q' && msg.msg_type != b'P' {
                        server.write_all(&raw).await?;
                        transaction_read_only_injected = false;
                        continue;
                    }

                    // Check if we need to enforce read-only
                    let max_size = startup_info.max_db_size
                        .or(if cfg.monitor.default_max_db_size_bytes > 0 {
                            Some(cfg.monitor.default_max_db_size_bytes)
                        } else {
                            None
                        });

                    let should_inject = if let Some(limit) = max_size {
                        size_monitor.get_db_size(&pool_key.database)
                            .map(|size| size > limit)
                            .unwrap_or(false)
                    } else {
                        false
                    };

                    if !should_inject {
                        server.write_all(&raw).await?;
                        transaction_read_only_injected = false;
                        continue;
                    }

                    // --- Read-only enforcement ---

                    // Check if this is a shrink operation that should be allowed
                    let is_shrink = extract_query_text(&msg)
                        .map(|q| is_shrink_operation(&q))
                        .unwrap_or(false);

                    if is_shrink && cfg.monitor.allow_shrink_operations_when_overlimit {
                        server.write_all(&raw).await?;
                    } else if msg.msg_type == b'Q' {
                        if let Some(query_text) = extract_query_text(&msg) {
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
                            let wrapped = format!("SET TRANSACTION READ ONLY; {}", query_text);
                            forward_buf.clear();
                            let mut payload = BytesMut::new();
                            payload.extend_from_slice(wrapped.as_bytes());
                            payload.put_u8(0);
                            let wrapped_msg = PgMessage::new(b'Q', payload);
                            wrapped_msg.encode(&mut forward_buf);
                            server.write_all(&forward_buf).await?;
                        }
                    } else {
                        // Extended query protocol — forward as-is (enforcement not yet implemented)
                        server.write_all(&raw).await?;
                    }
                }
                server.flush().await?;
            }

            result = server.read_buf(&mut bufs.server_buf) => {
                let n = result?;
                if n == 0 {
                    return Ok(ProxyLoopExit::ClientDisconnected);
                }

                while let Some((_msg, raw)) = try_read_message_with_raw(&mut bufs.server_buf)? {
                    // Track transaction state from ReadyForQuery
                    if raw[0] == b'Z' && raw.len() == 6 {
                        scanner.transaction_status = raw[5];
                        // Check for slow→fast transition at idle boundary
                        if raw[5] == b'I' && !over_limit.load(Ordering::Relaxed) {
                            // Forward this last message then switch
                            client.write_all(&raw).await?;
                            client.flush().await?;
                            // Drain any remaining parsed messages before returning
                            // (try_read_message_with_raw already consumed from server_buf,
                            // so any remaining bytes are partial — they stay in bufs.server_buf
                            // and will be picked up by the fast loop)
                            return Ok(ProxyLoopExit::SwitchToFast);
                        }
                    }
                    client.write_all(&raw).await?;
                }
                client.flush().await?;
            }
        }
    }
}
```

- [ ] **Step 2: Commit**

```
git add src/protocol/frontend.rs
git commit -m "Optimized slow_proxy_loop: buffer reuse, zero-copy, transitions"
```

---

## Task 6: Wire Up Transitions in handle_client

Connect fast and slow loops with the session registry and outer transition loop.

**Files:**
- Modify: `src/protocol/frontend.rs`

- [ ] **Step 1: Update handle_client to use session registry and proxy modes**

In `handle_client`, after registering the DB size limit (line ~183) and before sending AuthenticationOk, add session registration:

```rust
// Determine the effective size limit for this session
let effective_limit = startup_info.max_db_size
    .or(if cfg.monitor.default_max_db_size_bytes > 0 {
        Some(cfg.monitor.default_max_db_size_bytes)
    } else {
        None
    })
    .unwrap_or(0);

// Register session to get per-session over-limit flag
let (session_id, over_limit_flag) = size_monitor.register_session(
    &pool_key.database,
    effective_limit,
);
```

Replace the Phase 4 proxy_loop call with an outer transition loop. Note: `over_limit_flag` is `Arc<AtomicBool>` — both loops take `&Arc<AtomicBool>`. Shared `ProxyBuffers` are passed to both loops so no data is lost on transitions.

```rust
// Phase 4: Proxy — start in fast mode, transition as needed
let mut scanner = MessageBoundaryScanner::new();
let mut bufs = ProxyBuffers::new();
loop {
    // Start in fast mode (or stay in fast mode after slow→fast transition)
    match fast_proxy_loop(
        &mut stream, &mut server_conn, &over_limit_flag, &mut scanner, &mut bufs,
    ).await? {
        ProxyLoopExit::ClientDisconnected => break,
        ProxyLoopExit::SwitchToSlow => {
            debug!(db = %pool_key.database, "Switching to slow proxy (over limit)");
        }
        ProxyLoopExit::SwitchToFast => unreachable!("fast loop doesn't return SwitchToFast"),
    }

    // Run slow mode until back under limit
    match slow_proxy_loop(
        &mut stream, &mut server_conn, &pool_key, &size_monitor,
        &cfg, &startup_info, &over_limit_flag, &mut scanner, &mut bufs,
    ).await? {
        ProxyLoopExit::ClientDisconnected => break,
        ProxyLoopExit::SwitchToFast => {
            debug!(db = %pool_key.database, "Switching to fast proxy (under limit)");
        }
        ProxyLoopExit::SwitchToSlow => unreachable!("slow loop doesn't return SwitchToSlow"),
    }
}

// Unregister session and return connection to pool
size_monitor.unregister_session(session_id);
pool_manager.release(pool_key, server_conn).await;
```

Remove the old `proxy_loop` call and `pool_manager.release` at the bottom.

- [ ] **Step 2: Remove the old proxy_loop and check_should_enforce_read_only functions**

Delete both functions — `proxy_loop` is replaced by `slow_proxy_loop`, and `check_should_enforce_read_only` is inlined into `slow_proxy_loop`.

- [ ] **Step 3: Commit**

```
git add src/protocol/frontend.rs
git commit -m "Wire up fast/slow proxy transitions in handle_client"
```

---

## Task 7: Docker Build Verification

No local Rust toolchain. Use Docker to compile and test.

**Files:** None (verification only)

- [ ] **Step 1: Docker build**

Run: `docker build -t pgmux:test .`

Expected: `Compiling pgmux v0.1.0` followed by `Finished`. Fix any compilation errors.

- [ ] **Step 2: Docker compose smoke test**

```
docker compose down 2>/dev/null; docker compose up --build -d
sleep 3
curl -s http://localhost:19090/health
PGPASSWORD=postgres docker exec -e PGPASSWORD=postgres pg-multiplexer-postgres-1 \
  psql -h pg-multiplexer-pgmux-1 -p 5433 -U postgres -d postgres -c "SELECT 1"
```

Expected: health returns ok, SELECT 1 returns 1.

- [ ] **Step 3: Multi-query smoke test**

Run several queries through PgMux to verify fast path works for normal flow:

```
PGPASSWORD=alpha_pass docker exec -e PGPASSWORD=alpha_pass pg-multiplexer-postgres-1 \
  psql -h pg-multiplexer-pgmux-1 -p 5433 -U alpha_user -d tenant_alpha -c "
    SELECT count(*) FROM items;
    INSERT INTO items (name, value) VALUES ('test', 42);
    SELECT count(*) FROM items;
  "
```

Expected: Counts increment by 1, no errors.

- [ ] **Step 4: Commit any fixes**

```
git add -A
git commit -m "Fix compilation/runtime issues from two-tier proxy"
```

---

## Task 8: Re-run Benchmark and Update README

**Files:**
- Modify: `README.md`
- Modify: `bench/multi_tenant_bench.py`

- [ ] **Step 1: Run comparison benchmark**

Ensure docker-compose is up with the new build, then:

```
/tmp/pgmux-bench/bin/python3 bench/multi_tenant_bench.py --compare --duration 180
```

Record the results.

- [ ] **Step 2: Update README benchmarks section**

Replace the benchmark results table in `README.md` with the new numbers. Add a note about the fast path optimization. Keep the old numbers as a "before" reference if the improvement is significant.

- [ ] **Step 3: Commit**

```
git add README.md bench/multi_tenant_bench.py
git commit -m "Update benchmarks after two-tier proxy optimization"
```

---

## Dependency Graph

```
Task 1 (session flags)  ──┐
Task 2 (scanner)        ──┼── Task 4 (fast loop) ──┐
Task 3 (zero-copy msg)  ──┘                        ├── Task 6 (wire up) ── Task 7 (verify) ── Task 8 (bench)
                           Task 5 (slow loop) ──────┘
```

Tasks 1, 2, 3 can run in parallel.
Task 4 depends on 2.
Task 5 depends on 3.
Task 6 depends on 1, 4, 5.
Tasks 7 and 8 are sequential after 6.
