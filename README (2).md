# pg-benchmark

A rigorous benchmark gauntlet for the **Postgres → pandas** pipeline.

Races every major driver/strategy against each other across configurable row counts, then outputs a Rich terminal table, a matplotlib chart, and a CSV.

## What's being benchmarked

| Driver | Description |
|---|---|
| `sqlalchemy_naive` | `pd.read_sql()` via SQLAlchemy + psycopg2 — the classic baseline |
| `psycopg2_cursor` | Raw psycopg2 cursor → `pd.DataFrame()`, skips SQLAlchemy overhead |
| `psycopg3_binary` | psycopg3 with **binary protocol** — skips server-side text serialization |
| `asyncpg` | Cython-written driver, always uses binary wire format |
| `connectorx` | **Rust + Apache Arrow** — streams directly into numpy arrays, no Python objects per cell |
| `copy_csv` | `COPY TO STDOUT CSV` → `pd.read_csv()` — uses Postgres's bulk export + C CSV parser |

### Why connectorx is fast (the whole point)

The standard path is:

```
Postgres → libpq (text) → SQLAlchemy type engine → Python object per cell → pandas
```

connectorx short-circuits this entirely:

```
Postgres → libpq (binary) → Rust type conversion → Apache Arrow buffer → numpy array → pandas
```

No Python object is ever created per cell. For a 1M row × 8 column result that's **8 million Python object allocations avoided**.

---

## Setup

### 1. Start Postgres

```bash
docker compose up -d
```

### 2. Create your `.env`

```bash
cp .env.example .env
# edit if your DB settings differ from the defaults
```

### 3. Install the project

```bash
pip install -e ".[dev]"
# or with uv:
uv pip install -e .
```

---

## Usage

### Full run (default: 1k / 10k / 100k / 1M rows, 3 iterations)

```bash
pg-benchmark
```

### Custom row counts

```bash
pg-benchmark --row-counts 1000 50000 500000
```

### Skip slow drivers for quick iteration

```bash
pg-benchmark --skip sqlalchemy_naive psycopg2_cursor --row-counts 100000 1000000
```

### Just seed the DB

```bash
pg-benchmark --seed-only
```

### Re-seed (truncates and reloads)

```bash
pg-benchmark --force-seed
```

### All options

```
--row-counts N [N ...]   Row counts to test (default: 1000 10000 100000 1000000)
--iterations N           Timed iterations per benchmark (default: 3)
--warmup N               Warm-up iterations excluded from timing (default: 1)
--skip DRIVER [...]      Drivers to skip
--output-dir PATH        Output directory for chart + CSV (default: ./results)
--force-seed             Re-seed even if data already exists
--seed-only              Only seed, then exit
--no-seed                Skip seeding entirely
```

---

## Output

After a run you'll find in `./results/`:

- `benchmark_results.png` — grouped bar chart, one panel per row count
- `benchmark_results.csv` — raw numbers for further analysis

And a Rich table printed to the terminal, e.g.:

```
╭──────────────────────────────┬───────────┬──────────┬──────────┬──────────────┬─────────────╮
│ Driver                       │      Mean │      Min │      Max │     Rows/sec │ vs baseline │
├──────────────────────────────┼───────────┼──────────┼──────────┼──────────────┼─────────────┤
│ SQLAlchemy (psycopg2)        │  4,210ms  │ 4,102ms  │ 4,318ms  │    237.5K/s  │ —           │
│ psycopg2 raw cursor          │  3,180ms  │ 3,091ms  │ 3,269ms  │    314.5K/s  │ 1.3×        │
│ psycopg3 binary              │  1,950ms  │ 1,891ms  │ 2,009ms  │    512.8K/s  │ 2.2×        │
│ asyncpg                      │  1,620ms  │ 1,584ms  │ 1,656ms  │    617.3K/s  │ 2.6×        │
│ connectorx (Rust/Arrow)      │    310ms  │   298ms  │   322ms  │      3.2M/s  │ 13.6×       │
│ COPY CSV + read_csv          │    480ms  │   471ms  │   489ms  │      2.1M/s  │  8.8×       │
╰──────────────────────────────┴───────────┴──────────┴──────────┴──────────────┴─────────────╯
```
*(numbers illustrative — actual results vary by hardware)*

---

## Project structure

```
pg-benchmark/
├── docker-compose.yml
├── .env.example
├── pyproject.toml
└── src/
    └── pg_benchmark/
        ├── config.py              # DB connection settings from .env
        ├── seed.py                # Seeds 1M rows via COPY FROM STDIN
        ├── runner.py              # CLI entry point
        ├── benchmarks/
        │   ├── base.py            # BenchmarkResult + timing harness
        │   ├── sqlalchemy_naive.py
        │   ├── psycopg2_cursor.py
        │   ├── psycopg3_binary.py
        │   ├── asyncpg_bench.py
        │   ├── connectorx_bench.py
        │   └── copy_csv.py
        └── reporting/
            ├── table.py           # Rich terminal table
            ├── chart.py           # Matplotlib bar chart
            └── csv_export.py      # CSV export
```

---

## Extending

Adding a new driver takes ~15 lines. Create `src/pg_benchmark/benchmarks/my_driver.py` with a `fetch(n: int) -> pd.DataFrame` function, then register it in `runner.py`:

```python
BENCHMARKS = [
    ...
    ("my_driver", "pg_benchmark.benchmarks.my_driver", "fetch"),
]
```

That's it — the harness handles timing, warm-up, error handling, and reporting automatically.

---

## Ideas for follow-up experiments

- **Type breakdown**: re-run with integer-only, text-only, and timestamp-heavy schemas to see how type conversion cost varies per driver
- **Network conditions**: add artificial latency with `tc netem` to simulate a remote DB and see if the rankings change
- **Parallelism**: test `connectorx`'s partition mode, which fetches chunks in parallel across multiple threads
- **Arrow-first**: try `adbc` (Arrow Database Connectivity) and skip the Arrow → pandas copy step entirely
- **Write benchmarks**: flip it around — how fast can each approach do bulk inserts?
