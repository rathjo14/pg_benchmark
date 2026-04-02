"""
Benchmark: asyncpg → pd.DataFrame()

asyncpg always uses Postgres binary protocol and is written in Cython,
making it one of the fastest Python Postgres drivers available.
We use asyncio.run() to keep the benchmark interface synchronous.
"""

from __future__ import annotations

import asyncio

import asyncpg
import pandas as pd

from pg_benchmark.config import cfg

_pool: asyncpg.Pool | None = None

QUERY = (
    "SELECT id, user_id, amount, label, score, created_at, is_active, category "
    "FROM benchmark_rows LIMIT $1"
)


async def _get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(cfg.asyncpg_dsn, min_size=1, max_size=3)
    return _pool


async def _fetch_async(n: int) -> pd.DataFrame:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        records = await conn.fetch(QUERY, n)
    if not records:
        return pd.DataFrame()
    # asyncpg Record objects support dict-like access; columns from first record
    cols = list(records[0].keys())
    # Build column-by-column for better memory efficiency
    data = {col: [r[col] for r in records] for col in cols}
    return pd.DataFrame(data)


def fetch(n: int) -> pd.DataFrame:
    return asyncio.run(_fetch_async(n))
