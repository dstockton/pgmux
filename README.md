# PgMux

Multi-tenant connection multiplexing for Postgres.

PgMux sits between your application and Postgres, allowing you to safely
run large numbers of isolated tenants on a single database cluster.

---

## Why PgMux?

Modern SaaS applications often need to support hundreds or thousands of
tenants, each with their own database or credentials.

Postgres itself doesn't provide strong controls for:
- limiting database size per tenant
- isolating noisy neighbours
- managing connection pressure across many tenants

PgMux solves this by acting as a lightweight, tenant-aware gateway.

---

## Key Features

- **Two-tier proxy** — raw byte forwarding for normal traffic, full message parsing only when enforcement is needed
- Connection multiplexing across many databases and users
- Tenant-aware routing and isolation
- Per-tenant database size limits with automatic write restriction
- Connection pool limits per tenant and globally
- Admin dashboard with real-time metrics
- Prometheus-compatible metrics endpoint (OpenMetrics)
- Auth delegation via `postgres-protocol` (supports cleartext, MD5, SCRAM-SHA-256)

---

## Use Cases

- SaaS platforms running one database per tenant
- Serverless Postgres providers
- Platforms with untrusted or semi-trusted tenants
- High-density multi-tenant systems

---

## How It Works

PgMux accepts Postgres client connections and routes them to upstream
Postgres based on the database and user provided at connect time.

For normal traffic, PgMux operates in **fast path** mode — raw bytes are
forwarded between client and server with no message parsing. A lightweight
boundary scanner tracks transaction state for connection pool management.

When a tenant exceeds their database size limit, PgMux switches to
**slow path** mode for that session — full message parsing with automatic
read-only enforcement. Shrink operations (DELETE, TRUNCATE, DROP) are
still permitted so tenants can reduce their usage.

Other capabilities:
- pool and reuse backend connections across tenant sessions
- expose pool stats, database sizes, and health via HTTP API and dashboard
- per-connection size limits via `max_db_size` startup parameter

---

## Roadmap

The following are natural next steps, not yet implemented:

- **Rate limiting / QPS throttling** per tenant
- **Query-level isolation** (resource quotas beyond connection and size limits)
- **Redis-backed shared state** for running multiple PgMux instances
- **Full client-side TLS termination** (currently responds to SSL requests
  but does not complete the TLS handshake)
- **Extended query protocol interception** for read-only enforcement
  (currently only simple query protocol is intercepted)
- **Configurable admin credentials** for the DB size monitor
  (currently hardcoded to postgres/postgres)
- **Graceful shutdown** with connection draining
- **Hot config reload** without restart

---

## Getting Started

### Docker Compose (quickest)

```sh
docker compose up
```

This starts Postgres on port 15432 and PgMux on port 15433
(admin dashboard on port 19090).

Connect through PgMux:

```sh
psql -h localhost -p 15433 -U postgres -d postgres
```

View the dashboard at http://localhost:19090

### Docker

```sh
docker pull ghcr.io/dstockton/pgmux:latest
docker run -p 5433:5433 -p 9090:9090 \
  -e PG_MUX_UPSTREAM_HOST=host.docker.internal \
  pgmux:latest
```

### From Source

```sh
cargo build --release
./target/release/pgmux --config config.toml
```

---

## Configuration

See `config.toml` for all options with defaults and documentation.

Key environment variable overrides:
- `PG_MUX_UPSTREAM_HOST` — upstream Postgres host
- `PG_MUX_UPSTREAM_PORT` — upstream Postgres port
- `PG_MUX_LISTEN` — PgMux listen address
- `PG_MUX_ADMIN_LISTEN` — admin HTTP listen address
- `PG_MUX_TLS_CERT` / `PG_MUX_TLS_KEY` — enable client-facing TLS
- `PG_MUX_UPSTREAM_TLS` — use TLS to upstream (default: true)

---

## Benchmarks

Multi-tenant benchmark comparing direct Postgres connections vs routing
through PgMux. Measures the overhead of protocol-level proxying, message
parsing, and connection pooling.

### Test Configuration

| Parameter | Value |
|---|---|
| Hardware | Apple M5, 24 GB RAM |
| Postgres | 17.9, Docker container |
| PgMux | Docker container (same host) |
| Tenants | 5 (separate database + user each) |
| Connections/tenant | 4 (20 total) |
| Duration | 180 seconds |
| Workload | 80% reads / 20% writes |
| Query mix | COUNT, aggregations, range scans, INSERT, UPDATE |

### Results

| Metric | Direct Postgres | PgMux (fast path) | PgMux (throttled) | Fast path overhead |
|---|---|---|---|---|
| Throughput | 3,743 qps | 1,891 qps | 1,319 qps | -49.5% |
| Latency avg | 1.33 ms | 2.64 ms | 3.78 ms | +1.31 ms |
| Latency p50 | 0.97 ms | 1.99 ms | 2.23 ms | +1.02 ms |
| Latency p95 | 3.65 ms | 6.58 ms | 11.49 ms | +2.93 ms |
| Latency p99 | 5.62 ms | 9.24 ms | 19.03 ms | +3.62 ms |
| Errors | 0 | 0 | 0 | — |

**Fast path** (normal operation): raw byte forwarding with no message
parsing. A lightweight boundary scanner tracks transaction state for
pool management. This is what >99% of traffic uses.

**Throttled** (tenant over size limit): full message parsing with
read-only enforcement. PgMux automatically switches to this mode
per-session when the database exceeds its configured size limit, and
switches back when the tenant is under limit again.

The fast path adds approximately **1 ms at p50** in this all-local
Docker-to-Docker configuration. For production workloads where queries
typically take 10-100+ ms, the overhead is negligible.

### Reproduce

```sh
docker compose up -d
pip install psycopg2-binary
python3 bench/multi_tenant_bench.py --setup
python3 bench/multi_tenant_bench.py --compare --duration 180
```

---

## Status

Early stage — feedback and contributions welcome.

---

## License

Apache 2.0

---

## Author

Created and maintained by David Stockton.

If you're using PgMux in production, a star on GitHub is appreciated.
