"""
config.py — connection strings and benchmark parameters.
Copy .env.example to .env and fill in your Postgres credentials.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Connection strings ────────────────────────────────────────────────────────
PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "benchmark_db")
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

# SQLAlchemy URL (used by read_sql and connectorx)
SQLALCHEMY_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

# Raw DSN (used by psycopg2, psycopg3, asyncpg)
DSN = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASSWORD}"

# connectorx uses its own URL format
CX_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# ── Benchmark parameters ──────────────────────────────────────────────────────

# Row counts to test at each size
ROW_COUNTS = [10_000, 100_000, 500_000, 1_000_000]

# Number of timed runs per (strategy, row_count) combination
RUNS_PER_CELL = 3

# Table that will be created and seeded for the benchmark
BENCH_TABLE = "bench_data"

# Which strategies to run (comment out any you haven't installed)
ENABLED_STRATEGIES = [
    "read_sql_sqlalchemy",   # pd.read_sql — the slow baseline
    "psycopg2_cursor",       # raw psycopg2 cursor -> pd.DataFrame
    "psycopg3_binary",       # psycopg3 with binary protocol
    "asyncpg_fetch",         # asyncpg async fetch -> pd.DataFrame
    "connectorx",            # connectorx (Rust, Arrow-backed)
    "copy_binary",           # COPY TO STDOUT binary -> numpy
    "cython_bridge",         # Cython/libpq binary -> numpy (GIL released)
]
