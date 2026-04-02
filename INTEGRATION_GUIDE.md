# Integrating pg_fast into an existing SQLAlchemy codebase

This guide walks through adding the Cython/libpq binary bridge to a project that already uses SQLAlchemy + pandas for Postgres queries. By the end you'll have a single `fast_query()` function that's a drop-in replacement for `pd.read_sql` — typically 3-4x faster.

## Who this is for

You have code that looks like this:

```python
df = pd.read_sql(session.query(Order).filter(Order.status == "shipped").statement, engine)
# or
df = pd.read_sql("SELECT * FROM orders WHERE status = 'shipped'", engine)
# or
df = pd.DataFrame(session.query(Order).filter(...).all())
```

And it's slow at scale (100k+ rows). The bottleneck is not Postgres — it's the Python type-conversion layer between the wire and pandas.

## What you're adding

A compiled Cython extension that:

1. Calls libpq's `PQexecParams()` with `resultFormat=1` (binary wire protocol)
2. Pre-allocates numpy arrays of the correct dtype per column
3. Byte-swaps Postgres binary data directly into array memory
4. Releases the GIL for numeric column extraction loops
5. Hands the arrays to `pd.DataFrame()` — no Python objects ever created per cell

## Prerequisites

### Windows

1. **Microsoft C++ Build Tools**
   - Download: https://visualstudio.microsoft.com/visual-cpp-build-tools/
   - Check "Desktop development with C++" during install

2. **PostgreSQL client binaries** (headers + libpq)
   - Download the zip from https://www.enterprisedb.com/download-postgresql-binaries
   - Extract to e.g. `C:\pgsql`
   - Add `C:\pgsql\bin` to your PATH (needed at runtime for `libpq.dll`)

3. **Cython**
   ```bash
   pip install cython numpy
   ```

### Linux / macOS

1. **C compiler** — already present on most systems (`gcc` / `clang`)

2. **libpq headers**
   ```bash
   # Ubuntu/Debian
   sudo apt install libpq-dev

   # macOS
   brew install libpq
   export LDFLAGS="-L$(brew --prefix libpq)/lib"
   export CPPFLAGS="-I$(brew --prefix libpq)/include"
   ```

3. **Cython**
   ```bash
   pip install cython numpy
   ```

## Step 1: Copy the source files

Copy these directories/files from this repo into your project:

```
your_project/
    pg_fast/
        __init__.py          # <-- from this repo's pg_fast/
    src/
        pg_benchmark/
            cython_bridge/
                __init__.py  # <-- empty file
                bridge.pyx   # <-- the Cython extension
    build_cython.py          # <-- the build script
```

Or, if you want a flatter layout, adjust the import paths in `pg_fast/__init__.py` accordingly.

## Step 2: Build the extension

```bash
# Windows
PG_INCLUDE_DIR="C:/pgsql/include" PG_LIB_DIR="C:/pgsql/lib" python build_cython.py build_ext --inplace

# Linux
python build_cython.py build_ext --inplace

# macOS (Homebrew)
export LDFLAGS="-L$(brew --prefix libpq)/lib"
export CPPFLAGS="-I$(brew --prefix libpq)/include"
python build_cython.py build_ext --inplace
```

This produces a `.pyd` (Windows) or `.so` (Linux/macOS) file in `src/pg_benchmark/cython_bridge/`.

## Step 3: Use it

### Basic usage

```python
from pg_fast import fast_query

# Pass your existing engine + any query
df = fast_query(engine, "SELECT * FROM orders WHERE status = 'shipped'")
```

### With SQLAlchemy Core (recommended for bulk reads)

```python
from sqlalchemy import select
from pg_fast import fast_query

stmt = select(orders_table).where(orders_table.c.created_at >= '2024-01-01')
df = fast_query(engine, stmt)
```

### With SQLAlchemy ORM queries

```python
from pg_fast import fast_query

query = session.query(Order).filter(Order.total > 100, Order.status == "shipped")
df = fast_query(session, query)
```

### With a raw DSN (no engine needed)

```python
from pg_fast import fast_query

dsn = "host=localhost port=5432 dbname=myapp user=postgres password=secret"
df = fast_query(dsn, "SELECT * FROM orders LIMIT 100000")
```

## Step 4: Migrate existing code

The migration is mechanical. Find-and-replace patterns:

### pd.read_sql

```python
# Before
df = pd.read_sql("SELECT ...", engine)

# After
df = fast_query(engine, "SELECT ...")
```

### pd.read_sql with a statement

```python
# Before
stmt = select(users).where(users.c.active == True)
df = pd.read_sql(stmt, engine)

# After
df = fast_query(engine, stmt)
```

### pd.DataFrame(session.query(...))

```python
# Before
rows = session.query(User).filter(User.is_active == True).all()
df = pd.DataFrame([{"id": r.id, "name": r.name, ...} for r in rows])

# After
df = fast_query(session, session.query(User).filter(User.is_active == True))
```

### pd.read_sql_query

```python
# Before
df = pd.read_sql_query("SELECT ...", con=engine)

# After
df = fast_query(engine, "SELECT ...")
```

## What types are supported

The bridge handles these Postgres types natively in C (zero Python overhead):

| Postgres type | numpy dtype | Notes |
|---|---|---|
| `boolean` | `bool` | |
| `smallint` | `int16` | |
| `integer` / `serial` | `int32` | |
| `bigint` / `bigserial` | `int64` | |
| `real` | `float32` | |
| `double precision` | `float64` | |
| `numeric` / `decimal` | `float64` | Lossy — precision beyond float64 is truncated |
| `timestamp` | `datetime64[us]` | Microsecond precision |
| `text` / `varchar` | `object` (Python str) | Unavoidable — variable-length data |

Unknown types fall back to text extraction (still faster than the full SQLAlchemy path, just not zero-copy).

## Caveats and limitations

### SQL injection risk with literal_binds

`fast_query` compiles SQLAlchemy statements using `literal_binds=True`, which inlines parameter values directly into the SQL string. This is safe when:

- Parameters come from your application logic (dates, status enums, IDs from internal systems)
- You're building analytics/reporting queries

This is NOT safe when:

- Parameters come directly from user input (form fields, URL params, API bodies)

For user-facing queries, continue using `pd.read_sql` with proper parameter binding, or add parameterized query support to the bridge (the underlying `PQexecParams` supports `$1, $2` placeholders natively — it just needs wiring up).

### NULL handling

NULL values in numeric columns are stored as `0` (not NaN). This matches what most analytics code expects but differs from pandas' default of NaN for missing values. If you need NaN-for-NULL semantics, the extraction functions in `bridge.pyx` would need to use masked arrays or nullable integer dtypes.

### No connection pooling

Each call to `fast_query` opens a new libpq connection, executes the query, and closes it. For high-frequency small queries this connection overhead will dominate. The bridge is optimized for bulk reads (10k+ rows), not OLTP workloads.

For repeated queries, you could extend `bridge.pyx` to accept a persistent `PGconn*` — but at that point you're building a driver, and psycopg3 with a future `fetch_numpy()` mode would be the better path.

### ORM features don't apply

The bridge returns raw column data as a DataFrame. SQLAlchemy ORM features like:

- Relationships / lazy loading
- Column property transforms
- Hybrid properties
- Event hooks / validators

...are all bypassed. This is by design — those features require Python objects per row, which is exactly what makes the normal path slow.

## When NOT to use this

| Scenario | Use instead |
|---|---|
| Small queries (< 1k rows) | `pd.read_sql` — connection overhead dominates |
| User-facing input in WHERE clauses | `pd.read_sql` with parameterized queries |
| You need ORM model instances | `session.query(Model).all()` |
| INSERT / UPDATE / DELETE | SQLAlchemy Core or ORM as usual |
| Cross-database support needed | `connectorx` (supports MySQL, SQLite, etc.) |

## Performance expectations

Measured on a local Docker Postgres 16 instance, Windows 11:

| Row count | pd.read_sql (SA) | fast_query | Speedup |
|---|---|---|---|
| 10,000 | 0.13s | 0.03s | ~4x |
| 100,000 | 0.50s | 0.13s | ~4x |
| 500,000 | 2.35s | 0.57s | ~4x |

The speedup is consistent because the bottleneck (Python object allocation per cell) scales linearly with row count, and the bridge eliminates it entirely.

## Troubleshooting

### ImportError: Cython bridge not compiled

Run the build step from Step 2. Make sure `cython` and `numpy` are installed first.

### DLL load failed (Windows)

`libpq.dll` is not on the DLL search path. Either:
- Add `C:\pgsql\bin` to your system PATH, or
- Set `PG_BIN_DIR=C:\path\to\pgsql\bin` environment variable

### LNK1181: cannot open input file 'pq.lib' (Windows build)

The build script should use `libpq.lib` on Windows. Check that `build_cython.py` has:
```python
libraries=["libpq"] if sys.platform == "win32" else ["pq"],
```

### Calling gil-requiring function not allowed without gil (Cython 3)

The `libpq` function declarations in `bridge.pyx` need `nogil` annotations. Check that `PQgetisnull`, `PQgetvalue`, and `PQgetlength` are declared with `nogil`:
```cython
int   PQgetisnull(PGresult* res, int row, int col) nogil
char* PQgetvalue(PGresult* res, int row, int col) nogil
int   PQgetlength(PGresult* res, int row, int col) nogil
```

### UnicodeDecodeError on a NUMERIC column

The bridge needs an explicit NUMERIC handler. If `OID_NUMERIC = 1700` is missing from the OID dispatch in `fetch_to_dataframe`, NUMERIC columns fall through to the text extractor which can't decode binary NUMERIC data. Make sure the `extract_numeric` function exists and is wired into the column dispatch.
