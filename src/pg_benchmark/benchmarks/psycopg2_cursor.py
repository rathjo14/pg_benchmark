"""
Benchmark: raw psycopg2 cursor → pd.DataFrame()
Skips SQLAlchemy overhead but still goes through Python tuple-per-row.
"""

from __future__ import annotations

import psycopg2
import psycopg2.pool
import pandas as pd

from pg_benchmark.config import cfg

_pool: psycopg2.pool.SimpleConnectionPool | None = None

QUERY = (
    "SELECT id, user_id, amount, label, score, created_at, is_active, category "
    "FROM benchmark_rows LIMIT %s"
)


def _get_pool() -> psycopg2.pool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.SimpleConnectionPool(1, 3, dsn=cfg.psycopg2_dsn)
    return _pool


def fetch(n: int) -> pd.DataFrame:
    pool = _get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(QUERY, (n,))
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        pool.putconn(conn)
