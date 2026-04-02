"""
Seed the benchmark database.

Creates `benchmark_rows` and bulk-loads 1 000 000 rows using
COPY FROM STDIN so seeding itself is fast (usually < 10 s).
"""

from __future__ import annotations

import io
import random
import string
import time
from datetime import datetime, timedelta

import psycopg2

from pg_benchmark.config import cfg

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS benchmark_rows (
    id          BIGSERIAL PRIMARY KEY,
    user_id     INTEGER       NOT NULL,
    amount      DOUBLE PRECISION NOT NULL,
    label       TEXT          NOT NULL,
    score       REAL          NOT NULL,
    created_at  TIMESTAMP     NOT NULL,
    is_active   BOOLEAN       NOT NULL,
    category    VARCHAR(20)   NOT NULL
);
"""

INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_benchmark_rows_user_id
    ON benchmark_rows (user_id);
"""

CATEGORIES = ["alpha", "beta", "gamma", "delta", "epsilon"]
TOTAL_ROWS = 1_000_000
CHUNK_SIZE = 50_000


def _random_label(rng: random.Random) -> str:
    return "".join(rng.choices(string.ascii_lowercase, k=rng.randint(6, 20)))


def _generate_chunk(start: int, size: int, rng: random.Random) -> io.StringIO:
    """Return a CSV-formatted StringIO buffer for one chunk."""
    buf = io.StringIO()
    base_dt = datetime(2020, 1, 1)
    for i in range(start, start + size):
        user_id = rng.randint(1, 100_000)
        amount = round(rng.uniform(-10_000.0, 10_000.0), 6)
        label = _random_label(rng)
        score = round(rng.uniform(0.0, 1.0), 6)
        created_at = base_dt + timedelta(seconds=rng.randint(0, 5 * 365 * 86400))
        is_active = rng.choice(["t", "f"])
        category = rng.choice(CATEGORIES)
        buf.write(
            f"{user_id}\t{amount}\t{label}\t{score}\t"
            f"{created_at.isoformat()}\t{is_active}\t{category}\n"
        )
    buf.seek(0)
    return buf


def seed(force: bool = False) -> None:
    conn = psycopg2.connect(cfg.psycopg2_dsn)
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(TABLE_DDL)
        cur.execute("SELECT COUNT(*) FROM benchmark_rows;")
        (existing,) = cur.fetchone()

    if existing >= TOTAL_ROWS and not force:
        print(
            f"[seed] Table already has {existing:,} rows — skipping. "
            "Use --force-seed to re-seed."
        )
        conn.close()
        return

    if existing > 0:
        print(f"[seed] Truncating existing {existing:,} rows …")
        with conn.cursor() as cur:
            cur.execute("TRUNCATE benchmark_rows RESTART IDENTITY;")

    rng = random.Random(42)
    t0 = time.perf_counter()
    loaded = 0
    copy_sql = (
        "COPY benchmark_rows (user_id, amount, label, score, "
        "created_at, is_active, category) FROM STDIN"
    )

    with conn.cursor() as cur:
        while loaded < TOTAL_ROWS:
            chunk_size = min(CHUNK_SIZE, TOTAL_ROWS - loaded)
            buf = _generate_chunk(loaded, chunk_size, rng)
            cur.copy_expert(copy_sql, buf)
            loaded += chunk_size
            elapsed = time.perf_counter() - t0
            print(
                f"\r[seed] {loaded:>10,} / {TOTAL_ROWS:,} rows  "
                f"({elapsed:.1f}s)",
                end="",
                flush=True,
            )

    print()

    with conn.cursor() as cur:
        cur.execute(INDEX_DDL)
        cur.execute("ANALYZE benchmark_rows;")

    elapsed = time.perf_counter() - t0
    print(f"[seed] Done — {TOTAL_ROWS:,} rows loaded in {elapsed:.1f}s.")
    conn.close()


if __name__ == "__main__":
    seed()
