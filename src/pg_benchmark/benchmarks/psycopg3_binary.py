"""
Benchmark: psycopg3 binary protocol → pd.DataFrame()

psycopg3 can receive data in Postgres binary wire format, which is faster
for numeric types because it skips text parsing on the server side and
uses more efficient type conversion on the client.
"""

from __future__ import annotations

import psycopg
import psycopg_pool
import pandas as pd

from pg_benchmark.config import cfg

_pool: psycopg_pool.ConnectionPool | None = None

QUERY = (
    "SELECT id, user_id, amount, label, score, created_at, is_active, category "
    "FROM benchmark_rows LIMIT %s"
)


def _get_pool() -> psycopg_pool.ConnectionPool:
    global _pool
    if _pool is None:
        _pool = psycopg_pool.ConnectionPool(
            cfg.psycopg3_conninfo, min_size=1, max_size=3, open=True
        )
    return _pool


def fetch(n: int) -> pd.DataFrame:
    pool = _get_pool()
    with pool.connection() as conn:
        # binary=True → Postgres sends raw binary wire format,
        # psycopg3 converts directly to Python types without text parsing
        with conn.cursor(binary=True) as cur:
            cur.execute(QUERY, (n,))
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=cols)
