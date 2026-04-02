# pg_benchmark — Postgres → pandas strategy gauntlet

Races six different strategies for pulling Postgres data into a pandas DataFrame, from the naive SQLAlchemy baseline down to a hand-rolled binary COPY parser.

## Strategies (slowest → fastest, roughly)

| # | Strategy | How it works |
|---|----------|-------------|
| 1 | `read_sql_sqlalchemy` | `pd.read_sql` via SQLAlchemy — baseline |
| 2 | `psycopg2_cursor` | Raw psycopg2 cursor, skips SA overhead |
| 3 | `psycopg3_binary` | psycopg3 with binary wire protocol |
| 4 | `asyncpg_fetch` | asyncpg async binary fetch |
| 5 | `connectorx` | Rust-based, Postgres → Arrow → pandas |
| 6 | `copy_binary` | `COPY TO STDOUT` binary → numpy arrays |

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure your Postgres connection
cp .env.example .env
# edit .env with your credentials

# 3. Create and seed the benchmark table (1M rows takes ~10-20s)
python seed.py --rows 1000000

# 4. Run the benchmark
python benchmark.py
```

## Options

```bash
# Specific row counts and more runs
python benchmark.py --rows 10000 100000 500000 --runs 5

# Only compare a subset of strategies
python benchmark.py --strategies read_sql_sqlalchemy connectorx copy_binary

# Skip the chart
python benchmark.py --no-plot
```

## What you'll see

- An ASCII table of mean latencies per (strategy, row_count)
- Speedup multiples vs the SQLAlchemy baseline
- A horizontal bar chart saved as `benchmark_results.png`

## The interesting part — why is `copy_binary` fast?

`COPY TO STDOUT (FORMAT binary)` streams Postgres's internal binary representation directly over the wire. Each column is a typed byte sequence (int4 is 4 big-endian bytes, float8 is IEEE 754, etc.). By parsing these bytes directly into pre-allocated numpy arrays, you skip the entire Python object allocation that happens in the normal query path:

```
Normal:  libpq → text decode → Python int/float/str object → pandas
Binary:  libpq → raw bytes → numpy array → pandas (zero Python objects)
```

This is essentially what `connectorx` does in Rust, but in pure Python so you can actually read the code.
