# Two-Tier Proxy: Fast Path Bypass

Reduce proxy overhead from ~2ms to near-zero for tenants under their size limit by forwarding raw bytes instead of parsing every Postgres wire protocol message.

## Problem

The current proxy loop parses every message into a `PgMessage` struct, checks size limits, re-encodes, and forwards. This costs ~1-2ms p50 overhead per query even when no enforcement is needed (the common case). Benchmark: 3,266 qps direct vs 1,319 qps through PgMux.

## Solution

Two-tier proxy: fast path (raw byte forwarding) for normal operation, slow path (full parsing) only when a tenant exceeds their size limit. Transitions happen mid-connection at clean message boundaries.

## 1. Fast Path — Raw Byte Forwarding

### Data flow

```
Client → read into buf → write buf to Server (no parsing)
Server → read into buf → scan for ReadyForQuery → write buf to Client
```

No `PgMessage` allocation. No encode/decode. The read buffer writes directly to the other socket.

### ReadyForQuery scanning

On the server→client direction, scan the raw byte stream for `ReadyForQuery` messages to track transaction state (needed for pool management).

**Message-boundary-aware scanning:** Rather than naive pattern matching (which can false-positive on data rows containing the `Z` byte pattern), the scanner tracks message boundaries by reading type+length headers. This is cheaper than full message parsing — we read 5 bytes (type + length), skip `length - 4` bytes of payload, and repeat. Only when the type byte is `Z` do we inspect the payload (1 byte: transaction status).

The wire format of ReadyForQuery:

| Offset | Value | Meaning |
|---|---|---|
| 0 | `0x5A` | Message type `Z` |
| 1-4 | `0x00000005` | Length (always 5 for ReadyForQuery) |
| 5 | `I`, `T`, or `E` | Transaction status |

**Cross-read state:** TCP reads are not message-aligned. The scanner retains a small state machine between reads tracking: (a) remaining bytes to skip in the current message, (b) partial header bytes from the end of the previous read. This handles ReadyForQuery messages split across read boundaries. The state is a simple enum — no allocation.

### Client disconnect handling

When the client sends a `Terminate` message (`X`) or the read returns 0 bytes, the fast path exits. The `Terminate` is forwarded as raw bytes to the server (correct behavior — the server closes the connection). The proxy then returns the backend connection to the pool. Since the scanner tracks transaction state, the pool knows whether the connection is idle and safe to reuse.

### Over-limit check

Once per client read cycle, load an `AtomicBool` flag for this session. Sub-nanosecond cost. The flag is session-local (see Section 3 for how per-connection limits work).

### Implementation

Replace the current `proxy_loop` with a `fast_proxy_loop` that:
1. Uses `tokio::select!` on client and server reads (same structure as current)
2. Client→server: read into buf, check AtomicBool, write buf to server. If flag set, mark transition pending.
3. Server→client: read into buf, scan message boundaries for `Z` to track transaction state, write buf to client. If transition pending and status is `I` (idle), break to switch to slow path.

No `try_read_message`, no `PgMessage`, no `encode`.

## 2. Slow Path Optimizations

When a tenant is over their size limit, fall back to full message parsing (current behavior) with these optimizations:

### Buffer reuse

Current code calls `BytesMut::new()` per message then grows it inside `encode`. Replace with a single pre-allocated `forward_buf` per session, cleared and reused each iteration. Avoids repeated allocation.

### Skip re-encode for unmodified messages

Most messages pass through unmodified even in slow path — only `Q` messages to over-limit tenants get rewritten. Before calling `try_read_message`, snapshot the buffer start position. After parsing, if the message needs no modification, write the original bytes from the snapshot instead of re-encoding from the parsed struct.

Concretely, change `try_read_message` to also return the byte range consumed. The caller can then choose: forward original bytes (fast) or encode from struct (when modified).

### Fast-exit for non-query messages

The current `check_should_enforce_read_only` already returns early for non-Q/P types, but the function is still called for every message. Hoist the type check to the loop body's top — skip the function call entirely for `D`, `C`, `T`, and other high-frequency message types. The cost savings are in avoiding the function call + DashMap lookups, not the type check itself.

### Extended query protocol

The current code has a TODO for extended query protocol (`P` / Parse) read-only enforcement — it forwards Parse messages as-is even in slow path. This limitation persists in the new design. The slow path handles simple query (`Q`) enforcement; extended protocol enforcement remains a roadmap item.

## 3. Mid-Connection Transitions

### Per-session over-limit flag

The over-limit decision depends on per-connection state: a connection may specify `max_db_size` in its startup params, overriding the global default. A shared per-database `AtomicBool` cannot represent multiple limits on the same database.

**Solution:** Each session gets its own `Arc<AtomicBool>` at connection start. The `DbSizeMonitor` maintains a registry of active sessions:

```rust
struct SessionLimit {
    database: String,
    limit_bytes: u64,
    over_limit: Arc<AtomicBool>,
}
```

The monitor's `check_all_sizes` method iterates active sessions, compares each session's limit against the current DB size, and sets/clears the per-session flag. This is O(active_sessions) per check cycle — cheap at the monitor's default 60s interval.

Public API on `DbSizeMonitor`:
```rust
fn register_session(&self, id: u64, database: &str, limit_bytes: u64) -> Arc<AtomicBool>
fn unregister_session(&self, id: u64)
```

Sessions without a size limit (no `max_db_size` and no global default) never get flagged and always stay on the fast path.

### Fast → Slow

The fast path checks the session's `AtomicBool` once per client read. When set:
1. Forward the current buffer (don't drop bytes mid-stream)
2. Set a `transition_pending` flag
3. Continue forwarding in fast-path mode until the scanner sees `ReadyForQuery` with status `I` (idle)
4. Switch to slow path loop

**Leakage window:** All queries between the flag being set and the idle `ReadyForQuery` pass through unmodified. This includes: the remainder of any in-flight transaction, any pipelined queries in the current read buffer, and any queries arriving before the transaction completes. This is acceptable: the size monitor runs on a 60s interval, the DB was already over limit for up to 60s before the flag was set, and the enforcement is a soft limit (prevents sustained writes, not individual queries).

### Slow → Fast

In the slow path, after each `ReadyForQuery` with status `I`, re-check the session flag. If cleared, switch to fast path immediately (we're at a clean message boundary).

### No mid-transaction transitions

Both transitions only happen at `ReadyForQuery` with status `I` (idle). If the connection is in a transaction (`T` or `E`), defer until the transaction completes. This prevents partial enforcement mid-transaction.

## 4. Files Changed

| File | Change |
|---|---|
| `src/protocol/frontend.rs` | New `fast_proxy_loop` with message-boundary scanner, refactored `proxy_loop` (slow path optimizations), outer loop in `handle_client` driving transitions |
| `src/protocol/messages.rs` | `try_read_message` returns byte range for zero-copy forwarding |
| `src/monitor/mod.rs` | Add per-session limit registry (`register_session` / `unregister_session`), update `check_all_sizes` to set/clear per-session flags |
| `bench/multi_tenant_bench.py` | Add `--over-limit` flag for testing slow path under load |

## 5. Testing

### Unit tests

- **Message-boundary scanner**: Byte buffers with embedded ReadyForQuery at various positions, split across reads, large DataRow payloads that happen to contain `0x5A` bytes. Verify correct transaction state extraction and no false positives.
- **Fast→slow transition**: Mock the over-limit flag, verify transition happens only at idle ReadyForQuery boundary and not mid-transaction.
- **Cross-read splits**: ReadyForQuery header split across two reads (e.g., first read ends with `5A 00`, next starts with `00 00 05 49`). Verify scanner carries state correctly.

### Benchmark

Re-run `--compare` after implementation. Expected: fast path overhead within 5-10% of direct Postgres (down from ~150% current overhead).

Add `--over-limit` mode to bench script: register a size limit on one tenant mid-test, verify slow path still works under load, measure slow path throughput.

### Integration tests

Existing `tests/integration_test.rs` tests exercise queries through PgMux. They validate the fast path end-to-end without changes.

## 6. Expected Performance

| Metric | Current PgMux | Expected (fast path) |
|---|---|---|
| p50 latency overhead | +1.05 ms | < 0.1 ms |
| Throughput | 1,319 qps | > 2,500 qps |
| Fast path cost per query | ~2ms parse+encode | 1 atomic load + message-boundary skip + memcpy |
