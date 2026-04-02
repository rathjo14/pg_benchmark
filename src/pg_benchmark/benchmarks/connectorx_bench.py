"""
Benchmark: connectorx → pd.DataFrame()

connectorx is written in Rust and streams data directly into Apache Arrow
buffers, completely bypassing Python object creation for each cell.
It is typically the fastest option for large result sets.
"""

from __future__ import annotations

import connectorx as cx
import pandas as pd

from pg_benchmark.config import cfg

QUERY_TEMPLATE = (
    "SELECT id, user_id, amount, label, score, created_at, is_active, category "
    "FROM benchmark_rows LIMIT {n}"
)


def fetch(n: int) -> pd.DataFrame:
    query = QUERY_TEMPLATE.format(n=n)
    # return_type="pandas" converts the Arrow record batch to a DataFrame
    # protocol="binary" uses Postgres binary wire format
    return cx.read_sql(
        cfg.connectorx_url,
        query,
        return_type="pandas",
        protocol="binary",
    )
