"""
strategies/
Each strategy is a callable:  fn(n_rows) -> pd.DataFrame

All strategies run the same query against the same table, limited to n_rows.
"""
import struct
import io
import asyncio

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

from config import DSN, CX_URL, SQLALCHEMY_URL, BENCH_TABLE

# ---------------------------------------------------------------------------
# Shared SQL
# ---------------------------------------------------------------------------
QUERY = lambda n: f"SELECT * FROM {BENCH_TABLE} LIMIT {n}"


# ---------------------------------------------------------------------------
# 1. pd.read_sql via SQLAlchemy  (the slow baseline)
# ---------------------------------------------------------------------------
def read_sql_sqlalchemy(n_rows: int) -> pd.DataFrame:
    from sqlalchemy import create_engine, text
    engine = create_engine(SQLALCHEMY_URL)
    with engine.connect() as conn:
        return pd.read_sql(text(QUERY(n_rows)), conn)


# ---------------------------------------------------------------------------
# 2. Raw psycopg2 cursor → pd.DataFrame
#    Still goes through Python object allocation, but skips SQLAlchemy overhead
# ---------------------------------------------------------------------------
def psycopg2_cursor(n_rows: int) -> pd.DataFrame:
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute(QUERY(n_rows))
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# 3. psycopg3 binary protocol
#    Binary transfer avoids text parsing on the Python side.
# ---------------------------------------------------------------------------
def psycopg3_binary(n_rows: int) -> pd.DataFrame:
    import psycopg
    conn = psycopg.connect(DSN)
    cur = conn.cursor(binary=True)          # <-- binary mode
    cur.execute(QUERY(n_rows))
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    conn.close()
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# 4. asyncpg  (binary protocol, async, very fast fetch)
# ---------------------------------------------------------------------------
async def _asyncpg_fetch(n_rows: int) -> pd.DataFrame:
    import asyncpg
    conn = await asyncpg.connect(
        host=DSN.split("host=")[1].split()[0],
        port=int(DSN.split("port=")[1].split()[0]),
        database=DSN.split("dbname=")[1].split()[0],
        user=DSN.split("user=")[1].split()[0],
        password=DSN.split("password=")[1].split()[0],
    )
    rows = await conn.fetch(QUERY(n_rows))
    await conn.close()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=list(rows[0].keys()))


def asyncpg_fetch(n_rows: int) -> pd.DataFrame:
    return asyncio.run(_asyncpg_fetch(n_rows))


# ---------------------------------------------------------------------------
# 5. connectorx  (Rust → Arrow → pandas, skips Python type layer entirely)
# ---------------------------------------------------------------------------
def connectorx(n_rows: int) -> pd.DataFrame:
    import connectorx as cx
    return cx.read_sql(CX_URL, QUERY(n_rows), return_type="pandas")


# ---------------------------------------------------------------------------
# 6. COPY TO STDOUT binary → numpy
#    The lowest-level approach: Postgres binary COPY format parsed manually.
#    Only handles the numeric/bool columns to demonstrate the technique;
#    text columns fall back to a byte decode.
# ---------------------------------------------------------------------------

# Postgres binary COPY header is 19 bytes: signature(11) + flags(4) + ext(4)
_COPY_HEADER = b"PGCOPY\n\xff\r\n\x00" + b"\x00" * 8   # 19 bytes total
_INT2  = struct.Struct(">h")
_UINT2 = struct.Struct(">H")
_INT4  = struct.Struct(">i")
_INT8  = struct.Struct(">q")
_FLT8  = struct.Struct(">d")
_BOOL  = struct.Struct(">?")

# Postgres OIDs we handle in the fast path
_OID_INT4    = 23
_OID_INT8    = 20
_OID_FLOAT8  = 701
_OID_BOOL    = 16
_OID_NUMERIC = 1700
_OID_TEXT    = 25
_OID_TIMESTAMP = 1114


def _decode_numeric(raw: bytes) -> float:
    """Decode Postgres binary NUMERIC into a Python float."""
    ndigits = _INT2.unpack_from(raw, 0)[0]
    weight  = _INT2.unpack_from(raw, 2)[0]
    sign    = _UINT2.unpack_from(raw, 4)[0]
    if sign == 0xC000:
        return float("nan")
    result = 0.0
    for i in range(ndigits):
        result += _UINT2.unpack_from(raw, 8 + i * 2)[0] * (10000 ** (weight - i))
    return -result if sign == 0x4000 else result


def _parse_binary_copy(buf: bytes, col_names: list, col_oids: list):
    """Parse Postgres binary COPY stream into numpy arrays."""
    n_cols = len(col_names)
    arrays = {c: [] for c in col_names}

    # Header: sig(11) + flags(4) + ext_len_field(4) = 19 bytes; then ext_len bytes of data
    ext_len = _INT4.unpack_from(buf, 15)[0]
    pos = 19 + ext_len

    while pos < len(buf):
        n_fields = _INT2.unpack_from(buf, pos)[0]; pos += 2
        if n_fields == -1:   # file trailer tuple
            break
        assert n_fields == n_cols

        for i, col in enumerate(col_names):
            field_len = _INT4.unpack_from(buf, pos)[0]; pos += 4
            if field_len == -1:
                arrays[col].append(None)
                continue
            raw = buf[pos:pos + field_len]; pos += field_len
            oid = col_oids[i]
            if oid == _OID_INT4:
                arrays[col].append(_INT4.unpack(raw)[0])
            elif oid == _OID_INT8:
                arrays[col].append(_INT8.unpack(raw)[0])
            elif oid == _OID_FLOAT8:
                arrays[col].append(_FLT8.unpack(raw)[0])
            elif oid == _OID_BOOL:
                arrays[col].append(_BOOL.unpack(raw)[0])
            elif oid == _OID_TIMESTAMP:
                # microseconds since 2000-01-01 -> numpy datetime64
                us = _INT8.unpack(raw)[0]
                arrays[col].append(np.datetime64("2000-01-01", "us") + np.timedelta64(us, "us"))
            elif oid == _OID_NUMERIC:
                arrays[col].append(_decode_numeric(raw))
            else:
                # TEXT etc. -- decode as UTF-8
                arrays[col].append(raw.decode("utf-8"))

    return pd.DataFrame({c: np.asarray(arrays[c]) for c in col_names})


def copy_binary(n_rows: int) -> pd.DataFrame:
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    # Grab column names and OIDs from a dummy query
    cur.execute(f"SELECT * FROM {BENCH_TABLE} LIMIT 0")
    col_names = [d[0] for d in cur.description]
    col_oids  = [d[1] for d in cur.description]

    buf = io.BytesIO()
    cur.copy_expert(
        f"COPY (SELECT * FROM {BENCH_TABLE} LIMIT {n_rows}) TO STDOUT (FORMAT binary)",
        buf,
    )
    cur.close()
    conn.close()

    return _parse_binary_copy(buf.getvalue(), col_names, col_oids)


# ---------------------------------------------------------------------------
# 7. Cython/libpq binary -> numpy
#    PQexecParams with resultFormat=1; byte-swaps directly into pre-allocated
#    numpy arrays with the GIL released on the hot path.
# ---------------------------------------------------------------------------

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

# On Windows the .pyd needs libpq.dll; add the pg bin dir to the DLL search path
if sys.platform == "win32":
    _pg_bin = os.environ.get("PG_BIN_DIR", r"C:\pgsql\bin")
    if os.path.isdir(_pg_bin):
        os.add_dll_directory(_pg_bin)

from pg_benchmark.cython_bridge import bridge as _cython_bridge

def cython_bridge(n_rows: int) -> pd.DataFrame:
    query = f"SELECT * FROM {BENCH_TABLE} LIMIT {n_rows}"
    return _cython_bridge.fetch_to_dataframe(DSN, query)
