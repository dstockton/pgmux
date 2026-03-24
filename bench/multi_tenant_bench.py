#!/usr/bin/env python3
"""
PgMux multi-tenant performance benchmark.

Simulates concurrent tenants hitting PgMux with mixed read/write workloads.
Each tenant has its own database, user, and connection pool.

Usage:
    python3 bench/multi_tenant_bench.py [--duration 180] [--host localhost] [--port 15433]

Prerequisites:
    pip install psycopg2-binary

    # Start the stack:
    docker compose up -d

    # Set up test tenants (run once):
    python3 bench/multi_tenant_bench.py --setup
"""

import argparse
import json
import os
import random
import statistics
import string
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2-binary required. Install: pip install psycopg2-binary")
    sys.exit(1)

# Tenant definitions
TENANTS = [
    {"db": "tenant_alpha", "user": "alpha_user", "password": "alpha_pass"},
    {"db": "tenant_beta", "user": "beta_user", "password": "beta_pass"},
    {"db": "tenant_gamma", "user": "gamma_user", "password": "gamma_pass"},
    {"db": "tenant_delta", "user": "delta_user", "password": "delta_pass"},
    {"db": "tenant_epsilon", "user": "epsilon_user", "password": "epsilon_pass"},
]


@dataclass
class TenantStats:
    queries: int = 0
    errors: int = 0
    latencies_ms: list = field(default_factory=list)
    reads: int = 0
    writes: int = 0
    connections_opened: int = 0

    @property
    def p50(self):
        if not self.latencies_ms:
            return 0
        s = sorted(self.latencies_ms)
        return s[len(s) // 2]

    @property
    def p95(self):
        if not self.latencies_ms:
            return 0
        s = sorted(self.latencies_ms)
        return s[int(len(s) * 0.95)]

    @property
    def p99(self):
        if not self.latencies_ms:
            return 0
        s = sorted(self.latencies_ms)
        return s[int(len(s) * 0.99)]

    @property
    def avg(self):
        if not self.latencies_ms:
            return 0
        return statistics.mean(self.latencies_ms)


def setup_tenants(host, admin_port):
    """Create tenant databases, users, tables, and seed data."""
    admin_conn = psycopg2.connect(
        host=host, port=admin_port, user="postgres",
        password="postgres", dbname="postgres",
    )
    admin_conn.autocommit = True
    cur = admin_conn.cursor()

    for t in TENANTS:
        print(f"  Setting up {t['db']}...")
        try:
            cur.execute(f"CREATE DATABASE {t['db']}")
        except psycopg2.errors.DuplicateDatabase:
            pass
        try:
            cur.execute(
                f"CREATE USER {t['user']} WITH PASSWORD '{t['password']}'"
            )
        except psycopg2.errors.DuplicateObject:
            pass
        cur.execute(
            f"GRANT ALL PRIVILEGES ON DATABASE {t['db']} TO {t['user']}"
        )

    cur.close()
    admin_conn.close()

    for t in TENANTS:
        conn = psycopg2.connect(
            host=host, port=admin_port, user="postgres",
            password="postgres", dbname=t["db"],
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"GRANT ALL ON SCHEMA public TO {t['user']}")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value NUMERIC(10,2),
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute(f"GRANT ALL ON TABLE items TO {t['user']}")
        cur.execute(f"GRANT USAGE, SELECT ON SEQUENCE items_id_seq TO {t['user']}")
        cur.execute("""
            INSERT INTO items (name, value)
            SELECT 'item_' || i, random() * 1000
            FROM generate_series(1, 100) AS i
            ON CONFLICT DO NOTHING
        """)
        cur.close()
        conn.close()

    print("  Setup complete.")


# Query workload mix
READ_QUERIES = [
    "SELECT count(*) FROM items",
    "SELECT * FROM items ORDER BY value DESC LIMIT 10",
    "SELECT avg(value), min(value), max(value) FROM items",
    "SELECT name, value FROM items WHERE value > 500",
    "SELECT * FROM items WHERE id = {id}",
]

WRITE_QUERIES = [
    "INSERT INTO items (name, value) VALUES ('{name}', {value})",
    "UPDATE items SET value = {value} WHERE id = {id}",
]


def random_name():
    return "bench_" + "".join(random.choices(string.ascii_lowercase, k=6))


def run_query(conn, stats, read_pct=80):
    """Execute a single random query, track latency."""
    is_read = random.randint(1, 100) <= read_pct
    try:
        cur = conn.cursor()
        start = time.monotonic()

        if is_read:
            q = random.choice(READ_QUERIES)
            q = q.format(id=random.randint(1, 200))
            cur.execute(q)
            cur.fetchall()
            stats.reads += 1
        else:
            q = random.choice(WRITE_QUERIES)
            q = q.format(
                name=random_name(),
                value=round(random.uniform(0, 1000), 2),
                id=random.randint(1, 200),
            )
            cur.execute(q)
            conn.commit()
            stats.writes += 1

        elapsed_ms = (time.monotonic() - start) * 1000
        stats.latencies_ms.append(elapsed_ms)
        stats.queries += 1
        cur.close()
    except Exception as e:
        stats.errors += 1
        try:
            conn.rollback()
        except Exception:
            pass


def tenant_worker(tenant, host, port, duration_secs, conns_per_tenant, read_pct):
    """Run workload for one tenant with multiple connections."""
    stats = TenantStats()
    deadline = time.monotonic() + duration_secs
    connections = []

    for _ in range(conns_per_tenant):
        try:
            c = psycopg2.connect(
                host=host, port=port,
                user=tenant["user"], password=tenant["password"],
                dbname=tenant["db"], connect_timeout=5,
            )
            connections.append(c)
            stats.connections_opened += 1
        except Exception as e:
            stats.errors += 1

    if not connections:
        return tenant["db"], stats

    while time.monotonic() < deadline:
        conn = random.choice(connections)
        run_query(conn, stats, read_pct)

    for c in connections:
        try:
            c.close()
        except Exception:
            pass

    return tenant["db"], stats


def run_benchmark(host, port, duration, conns_per_tenant, read_pct):
    """Run the full multi-tenant benchmark."""
    print(f"\n{'=' * 60}")
    print(f" PgMux Multi-Tenant Benchmark")
    print(f"{'=' * 60}")
    print(f" Target:              {host}:{port}")
    print(f" Tenants:             {len(TENANTS)}")
    print(f" Connections/tenant:  {conns_per_tenant}")
    print(f" Total connections:   {len(TENANTS) * conns_per_tenant}")
    print(f" Duration:            {duration}s")
    print(f" Read/Write mix:      {read_pct}% reads / {100-read_pct}% writes")
    print(f"{'=' * 60}\n")

    start = time.monotonic()

    with ThreadPoolExecutor(max_workers=len(TENANTS)) as pool:
        futures = {
            pool.submit(
                tenant_worker, t, host, port, duration, conns_per_tenant, read_pct
            ): t["db"]
            for t in TENANTS
        }

        results = {}
        for future in as_completed(futures):
            db, stats = future.result()
            results[db] = stats

    wall_time = time.monotonic() - start

    # Aggregate
    total_queries = sum(s.queries for s in results.values())
    total_errors = sum(s.errors for s in results.values())
    total_reads = sum(s.reads for s in results.values())
    total_writes = sum(s.writes for s in results.values())
    all_latencies = []
    for s in results.values():
        all_latencies.extend(s.latencies_ms)

    all_latencies.sort()
    global_p50 = all_latencies[len(all_latencies) // 2] if all_latencies else 0
    global_p95 = all_latencies[int(len(all_latencies) * 0.95)] if all_latencies else 0
    global_p99 = all_latencies[int(len(all_latencies) * 0.99)] if all_latencies else 0
    global_avg = statistics.mean(all_latencies) if all_latencies else 0
    qps = total_queries / wall_time if wall_time > 0 else 0

    print(f"\n{'=' * 60}")
    print(f" RESULTS")
    print(f"{'=' * 60}")
    print(f" Wall time:     {wall_time:.1f}s")
    print(f" Total queries: {total_queries:,}")
    print(f" Total errors:  {total_errors:,}")
    print(f" Throughput:    {qps:,.0f} queries/sec")
    print(f" Reads:         {total_reads:,}  Writes: {total_writes:,}")
    print(f"")
    print(f" Latency (ms):")
    print(f"   avg:  {global_avg:.2f}")
    print(f"   p50:  {global_p50:.2f}")
    print(f"   p95:  {global_p95:.2f}")
    print(f"   p99:  {global_p99:.2f}")

    print(f"\n Per-Tenant Breakdown:")
    print(f" {'Tenant':<20} {'Queries':>8} {'Errors':>7} {'QPS':>8} {'p50ms':>7} {'p95ms':>7} {'p99ms':>7}")
    print(f" {'-'*20} {'-'*8} {'-'*7} {'-'*8} {'-'*7} {'-'*7} {'-'*7}")
    for db in sorted(results.keys()):
        s = results[db]
        t_qps = s.queries / wall_time if wall_time > 0 else 0
        print(
            f" {db:<20} {s.queries:>8,} {s.errors:>7,} {t_qps:>8,.0f}"
            f" {s.p50:>7.2f} {s.p95:>7.2f} {s.p99:>7.2f}"
        )
    print(f"{'=' * 60}\n")

    return {
        "wall_time_secs": round(wall_time, 1),
        "total_queries": total_queries,
        "total_errors": total_errors,
        "throughput_qps": round(qps),
        "reads": total_reads,
        "writes": total_writes,
        "latency_ms": {
            "avg": round(global_avg, 2),
            "p50": round(global_p50, 2),
            "p95": round(global_p95, 2),
            "p99": round(global_p99, 2),
        },
        "tenants": len(TENANTS),
        "connections_per_tenant": conns_per_tenant,
        "total_connections": len(TENANTS) * conns_per_tenant,
        "read_pct": read_pct,
        "duration_target_secs": duration,
    }


def print_comparison(direct, pgmux):
    """Print side-by-side comparison with overhead calculation."""
    print(f"\n{'=' * 60}")
    print(f" COMPARISON: Direct Postgres vs PgMux")
    print(f"{'=' * 60}")
    print(f" {'Metric':<25} {'Direct':>12} {'PgMux':>12} {'Overhead':>12}")
    print(f" {'-'*25} {'-'*12} {'-'*12} {'-'*12}")

    d_qps = direct["throughput_qps"]
    p_qps = pgmux["throughput_qps"]
    qps_overhead = ((d_qps - p_qps) / d_qps * 100) if d_qps > 0 else 0

    d_avg = direct["latency_ms"]["avg"]
    p_avg = pgmux["latency_ms"]["avg"]
    avg_overhead = ((p_avg - d_avg) / d_avg * 100) if d_avg > 0 else 0

    d_p50 = direct["latency_ms"]["p50"]
    p_p50 = pgmux["latency_ms"]["p50"]
    p50_overhead = ((p_p50 - d_p50) / d_p50 * 100) if d_p50 > 0 else 0

    d_p95 = direct["latency_ms"]["p95"]
    p_p95 = pgmux["latency_ms"]["p95"]
    p95_overhead = ((p_p95 - d_p95) / d_p95 * 100) if d_p95 > 0 else 0

    d_p99 = direct["latency_ms"]["p99"]
    p_p99 = pgmux["latency_ms"]["p99"]
    p99_overhead = ((p_p99 - d_p99) / d_p99 * 100) if d_p99 > 0 else 0

    print(f" {'Throughput (qps)':<25} {d_qps:>12,} {p_qps:>12,} {qps_overhead:>+11.2f}%")
    print(f" {'Latency avg (ms)':<25} {d_avg:>12.2f} {p_avg:>12.2f} {avg_overhead:>+11.2f}%")
    print(f" {'Latency p50 (ms)':<25} {d_p50:>12.2f} {p_p50:>12.2f} {p50_overhead:>+11.2f}%")
    print(f" {'Latency p95 (ms)':<25} {d_p95:>12.2f} {p_p95:>12.2f} {p95_overhead:>+11.2f}%")
    print(f" {'Latency p99 (ms)':<25} {d_p99:>12.2f} {p_p99:>12.2f} {p99_overhead:>+11.2f}%")
    print(f" {'Total queries':<25} {direct['total_queries']:>12,} {pgmux['total_queries']:>12,}")
    print(f" {'Errors':<25} {direct['total_errors']:>12,} {pgmux['total_errors']:>12,}")
    print(f"{'=' * 60}\n")


def main():
    parser = argparse.ArgumentParser(description="PgMux multi-tenant benchmark")
    parser.add_argument("--host", default="localhost", help="PgMux host")
    parser.add_argument("--port", type=int, default=15433, help="PgMux port")
    parser.add_argument("--pg-port", type=int, default=15432, help="Direct PG port (for --setup)")
    parser.add_argument("--duration", type=int, default=180, help="Test duration in seconds")
    parser.add_argument("--conns-per-tenant", type=int, default=4, help="Connections per tenant")
    parser.add_argument("--read-pct", type=int, default=80, help="Percentage of read queries")
    parser.add_argument("--setup", action="store_true", help="Set up tenant databases and exit")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    parser.add_argument(
        "--direct", action="store_true",
        help="Bypass PgMux, connect directly to Postgres (for comparison only)"
    )
    parser.add_argument(
        "--compare", action="store_true",
        help="Run both direct and PgMux benchmarks back-to-back, then compare"
    )
    args = parser.parse_args()

    if args.setup:
        print("Setting up tenant databases...")
        setup_tenants(args.host, args.pg_port)
        return

    if args.compare:
        print("Phase 1/2: Benchmarking DIRECT to Postgres...")
        direct_result = run_benchmark(
            args.host, args.pg_port, args.duration,
            args.conns_per_tenant, args.read_pct,
        )
        direct_result["mode"] = "direct"

        print("\n\nPhase 2/2: Benchmarking through PgMux...")
        pgmux_result = run_benchmark(
            args.host, args.port, args.duration,
            args.conns_per_tenant, args.read_pct,
        )
        pgmux_result["mode"] = "pgmux"

        print_comparison(direct_result, pgmux_result)

        if args.json:
            print(json.dumps({
                "direct": direct_result,
                "pgmux": pgmux_result,
            }, indent=2))
        return

    port = args.pg_port if args.direct else args.port
    mode = "DIRECT to Postgres" if args.direct else "through PgMux"
    print(f"Running benchmark {mode}...")

    result = run_benchmark(
        args.host, port, args.duration,
        args.conns_per_tenant, args.read_pct,
    )

    if args.json:
        result["mode"] = "direct" if args.direct else "pgmux"
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
