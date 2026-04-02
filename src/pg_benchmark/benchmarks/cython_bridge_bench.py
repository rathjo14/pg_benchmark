"""
Benchmark: Cython/libpq binary → numpy arrays → pd.DataFrame

This driver:
  1. Calls libpq PQexecParams() with resultFormat=1 (binary wire format)
  2. For each numeric column, byte-swaps directly into a pre-allocated
     numpy array buffer — no Python object ever created per cell
  3. GIL is released for the inner extraction loop on numeric columns
  4. Text/varchar columns still require Python objects (unavoidable for
     variable-length data), but skip the SQLAlchemy type-engine overhead

If the Cython extension hasn't been compiled yet, importing this module
raises ImportError with a helpful message — the runner handles this
gracefully by marking the benchmark as skipped.
"""

from __future__ import annotations

import pandas as pd

from pg_benchmark.config import cfg

try:
    from pg_benchmark.cython_bridge import bridge as _bridge
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False

QUERY_TEMPLATE = (
    "SELECT id, user_id, amount, label, score, created_at, is_active, category "
    "FROM benchmark_rows LIMIT {n}"
)


def fetch(n: int) -> pd.DataFrame:
    if not _AVAILABLE:
        raise ImportError(
            "Cython bridge not compiled. Run:  python build_cython.py build_ext --inplace"
        )
    query = QUERY_TEMPLATE.format(n=n)
    return _bridge.fetch_to_dataframe(cfg.psycopg2_dsn, query)
