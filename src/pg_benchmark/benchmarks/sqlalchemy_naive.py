"""
Benchmark: SQLAlchemy (psycopg2) + pd.read_sql
The classic slow baseline: every cell becomes a Python object.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text

from pg_benchmark.config import cfg

_engine = None


def _get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(cfg.sqlalchemy_psycopg2_url, pool_pre_ping=True)
    return _engine


QUERY = "SELECT id, user_id, amount, label, score, created_at, is_active, category FROM benchmark_rows LIMIT :n"


def fetch(n: int) -> pd.DataFrame:
    engine = _get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(QUERY), conn, params={"n": n})
