# pg_benchmark — Postgres -> pandas strategy gauntlet

Races seven different strategies for pulling Postgres data into a pandas DataFrame, from the naive SQLAlchemy baseline down to a Cython/libpq binary bridge that beats Rust.

## Strategies (slowest -> fastest, roughly)

| # | Strategy | How it works |
|---|----------|-------------|
| 1 | `read_sql_sqlalchemy` | `pd.read_sql` via SQLAlchemy — baseline |
| 2 | `psycopg2_cursor` | Raw psycopg2 cursor, skips SA overhead |
| 3 | `psycopg3_binary` | psycopg3 with binary wire protocol |
| 4 | `asyncpg_fetch` | asyncpg async binary fetch |
| 5 | `connectorx` | Rust-based, Postgres -> Arrow -> pandas |
| 6 | `copy_binary` | `COPY TO STDOUT` binary -> numpy (pure Python) |
| 7 | `cython_bridge` | libpq binary -> numpy via Cython (GIL released) |

## Requirements

### System dependencies (install in this order)

**1. Docker Desktop**
Download from https://www.docker.com/products/docker-desktop/ — used to run the Postgres container. After installing, launch Docker Desktop and wait for it to fully start before proceeding.

**2. Microsoft C++ Build Tools** (Windows only)
Required to compile the Cython extension. Download from https://visualstudio.microsoft.com/visual-cpp-build-tools/ and check "Desktop development with C++".

**3. PostgreSQL client binaries**
Needed for `libpq.dll` (runtime) and headers (build time). You do NOT need to install the full Postgres server.

- Download the zip archive from https://www.enterprisedb.com/download-postgresql-binaries (grab PG 16)
- Extract to `C:\pgsql`
- Add `C:\pgsql\bin` to your PATH

> If you install to a different location, set the `PG_BIN_DIR` environment variable to your bin directory (e.g. `PG_BIN_DIR=D:\postgres\bin`). The build step also reads `PG_INCLUDE_DIR` and `PG_LIB_DIR` if you need to override those.

### Python dependencies

```bash
pip install cython
pip install -r requirements.txt
```

## Setup

```bash
# 1. Build the Cython extension
PG_INCLUDE_DIR="C:/pgsql/include" PG_LIB_DIR="C:/pgsql/lib" python build_cython.py build_ext --inplace

# 2. Start Postgres
docker compose up -d

# 3. Configure your connection
cp .env.example .env
# .env is pre-configured for the docker-compose defaults, no edits needed unless
# you're connecting to your own Postgres instance

# 4. Seed the benchmark table
python seed.py --rows 500000

# 5. Run the benchmark
python benchmark.py
```

## Options

```bash
# Specific row counts and more runs
python benchmark.py --rows 10000 100000 500000 --runs 5

# Only compare a subset of strategies
python benchmark.py --strategies read_sql_sqlalchemy connectorx cython_bridge

# Skip the chart
python benchmark.py --no-plot
```

## What you'll see

- An ASCII table of mean latencies per (strategy, row_count)
- Speedup multiples vs the SQLAlchemy baseline
- A horizontal bar chart saved as `benchmark_results.png`

## Why is `cython_bridge` the fastest?

Every other method touches Python for at least one of: type conversion, object allocation, or result iteration. The Cython bridge eliminates Python from the hot path entirely.

```
SQLAlchemy:    libpq -> text -> Python str -> SA type engine -> Python object -> pandas
psycopg2:      libpq -> text -> Python tuple of objects -> pd.DataFrame()
connectorx:    libpq -> binary -> Rust -> Apache Arrow -> pandas
cython_bridge: libpq -> binary -> C byte-swap -> numpy array buffer -> pandas
```

Key implementation details in `src/pg_benchmark/cython_bridge/bridge.pyx`:
- Calls `PQexecParams` with `resultFormat=1` (binary wire format)
- Pre-allocates numpy arrays of the correct dtype before iterating rows
- `PQgetvalue()` returns a `char*` directly into libpq's result buffer
- Byte-swaps and writes straight into `arr.data` — no Python objects on the hot path
- The GIL is released for the entire extraction loop on numeric columns

`connectorx` does something similar in Rust but routes through Apache Arrow as an intermediate format. The Cython bridge writes directly to numpy, skipping that hop, which is why it edges out Rust at large row counts (~10-15% faster at 500k+ rows).

The pure-Python `copy_binary` strategy uses the same idea but executes in a Python `while` loop with `struct.unpack()` per cell — the concept is right but without C-level execution it ends up slower than everything.
