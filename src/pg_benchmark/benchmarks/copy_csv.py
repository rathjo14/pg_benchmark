"""
Benchmark: COPY TO STDOUT (CSV) → pd.read_csv()

Postgres can stream the result set as a CSV directly to the client.
pd.read_csv() is heavily optimised (written in C) and builds the DataFrame
from raw bytes, avoiding row-by-row Python object creation entirely.
This is a surprisingly competitive approach for analytics workloads.
"""

from __future__ import annotations

import io

import psycopg2
import psycopg2.pool
import pandas as pd

from pg_benchmark.config import cfg

_pool: psycopg2.pool.SimpleConnectionPool | None = None

COPY_SQL = (
    "COPY ("
    "  SELECT id, user_id, amount, label, score, created_at, is_active, category"
    "  FROM benchmark_rows LIMIT {n}"
    ") TO STDOUT WITH (FORMAT CSV, HEADER TRUE)"
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
        buf = io.StringIO()
        with conn.cursor() as cur:
            cur.copy_expert(COPY_SQL.format(n=n), buf)
        buf.seek(0)
        return pd.read_csv(buf)
    finally:
        pool.putconn(conn)
