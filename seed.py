"""
seed.py — creates and seeds bench_data with a realistic mixed-type schema.

Run once before benchmarking:
    python seed.py --rows 1000000
"""
import argparse
import time
import psycopg2
import psycopg2.extras
from config import DSN, BENCH_TABLE


CREATE_TABLE = f"""
DROP TABLE IF EXISTS {BENCH_TABLE};
CREATE TABLE {BENCH_TABLE} (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER       NOT NULL,
    score       DOUBLE PRECISION NOT NULL,
    label       TEXT          NOT NULL,
    is_active   BOOLEAN       NOT NULL,
    created_at  TIMESTAMP     NOT NULL,
    amount      NUMERIC(12,4) NOT NULL
);
"""

INSERT_BATCH = f"""
INSERT INTO {BENCH_TABLE} (user_id, score, label, is_active, created_at, amount)
SELECT
    (random() * 1e6)::int,
    random() * 1000,
    md5(random()::text),
    random() > 0.5,
    NOW() - (random() * INTERVAL '365 days'),
    (random() * 1e6)::numeric(12,4)
FROM generate_series(1, %s);
"""


def seed(n_rows: int) -> None:
    print(f"Connecting to Postgres...")
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()

    print(f"Creating table {BENCH_TABLE!r}...")
    cur.execute(CREATE_TABLE)

    print(f"Inserting {n_rows:,} rows (this may take a moment)...")
    t0 = time.perf_counter()
    cur.execute(INSERT_BATCH, (n_rows,))
    elapsed = time.perf_counter() - t0

    cur.execute(f"ANALYZE {BENCH_TABLE};")
    cur.close()
    conn.close()
    print(f"Done — {n_rows:,} rows inserted in {elapsed:.1f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    args = parser.parse_args()
    seed(args.rows)
