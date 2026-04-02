"""
pg_fast - SQLAlchemy + Cython/libpq binary bridge

Drop-in fast path for pd.read_sql / pd.DataFrame(session.query(...))
that bypasses the Python type-conversion layer entirely.

Usage
-----
    from pg_fast import fast_query

    # SQLAlchemy Core
    stmt = select(User).where(User.is_active == True)
    df = fast_query(engine, stmt)

    # SQLAlchemy ORM query
    df = fast_query(session, session.query(User).filter(User.is_active == True))

    # Raw SQL string
    df = fast_query(engine, "SELECT * FROM users LIMIT 10000")

    # Raw DSN string instead of engine/session
    df = fast_query("host=localhost dbname=mydb user=postgres", "SELECT 1")

Caveats
-------
- Parameters are inlined via literal_binds. Do NOT use with unsanitised
  user input.
- Requires the Cython extension to be compiled first:
      python build_cython.py build_ext --inplace
- Windows: set PG_BIN_DIR env var if PostgreSQL is not at C:\\pgsql\\bin.
"""

import os
import sys

import pandas as pd
from sqlalchemy.dialects import postgresql as _pg_dialect

# -- Windows: add libpq.dll to the DLL search path --
if sys.platform == "win32":
    _pg_bin = os.environ.get("PG_BIN_DIR", r"C:\pgsql\bin")
    if os.path.isdir(_pg_bin):
        os.add_dll_directory(_pg_bin)

# -- Import the compiled Cython extension from src/ --
_src = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

try:
    from pg_benchmark.cython_bridge.bridge import fetch_to_dataframe as _fetch
except ImportError as e:
    raise ImportError(
        "Cython bridge not compiled. Run:\n"
        "  python build_cython.py build_ext --inplace"
    ) from e


def _dsn_from_url(url) -> str:
    """Build a libpq DSN from a SQLAlchemy URL object."""
    parts = []
    if url.host:
        parts.append(f"host={url.host}")
    if url.port:
        parts.append(f"port={url.port}")
    if url.database:
        parts.append(f"dbname={url.database}")
    if url.username:
        parts.append(f"user={url.username}")
    if url.password:
        parts.append(f"password={url.password}")
    return " ".join(parts)


def _resolve_dsn(obj) -> str:
    if isinstance(obj, str):
        return obj
    # Engine
    if hasattr(obj, "url"):
        return _dsn_from_url(obj.url)
    # Session (bound)
    if hasattr(obj, "bind") and obj.bind is not None:
        return _dsn_from_url(obj.bind.url)
    raise TypeError(
        "First argument must be a DSN string, SQLAlchemy Engine, or bound Session."
    )


def _compile_sql(query_or_stmt) -> str:
    if isinstance(query_or_stmt, str):
        return query_or_stmt
    stmt = getattr(query_or_stmt, "statement", query_or_stmt)
    compiled = stmt.compile(
        dialect=_pg_dialect.dialect(),
        compile_kwargs={"literal_binds": True},
    )
    return str(compiled)


def fast_query(engine_or_session_or_dsn, query_or_stmt) -> pd.DataFrame:
    """
    Execute a query via the Cython/libpq binary bridge and return a DataFrame.

    Parameters
    ----------
    engine_or_session_or_dsn : Engine | Session | str
        A SQLAlchemy Engine, a bound Session, or a raw libpq DSN string.
    query_or_stmt : Select | Query | str
        A SQLAlchemy Core Select, an ORM Query, or a raw SQL string.

    Returns
    -------
    pd.DataFrame
    """
    dsn = _resolve_dsn(engine_or_session_or_dsn)
    sql = _compile_sql(query_or_stmt)
    return _fetch(dsn, sql)
