# bridge.pyx
#
# Cython bridge: libpq binary wire format → numpy arrays → pandas DataFrame
#
# Compile with:   python build.py build_ext --inplace
#
# What happens here that doesn't happen in normal drivers:
#   - We call PQexecParams with resultFormat=1 (binary), so Postgres sends
#     raw typed bytes instead of text representations.
#   - We pre-allocate numpy arrays of the correct dtype BEFORE iterating rows.
#   - PQgetvalue() returns a char* directly into libpq's result buffer.
#   - We byte-swap and write straight into arr.data — no Python objects on
#     the hot path for numeric types.
#   - The GIL is released for the entire extraction loop per numeric column.

import numpy as np
cimport numpy as cnp
cimport cython

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint32_t
from libc.string cimport memcpy
from libc.stdlib cimport malloc, free

cnp.import_array()

# --------------------------------------------------------------------------- #
# libpq declarations
# --------------------------------------------------------------------------- #

cdef extern from "libpq-fe.h":
    ctypedef void PGconn
    ctypedef void PGresult
    ctypedef unsigned int Oid

    # Connection
    PGconn* PQconnectdb(const char* conninfo)
    void    PQfinish(PGconn* conn)
    int     PQstatus(PGconn* conn)      # 0 = CONNECTION_OK

    # Execution (binary result format: paramFormats=NULL, resultFormat=1)
    PGresult* PQexecParams(
        PGconn* conn,
        const char* command,
        int nParams,
        const Oid* paramTypes,
        const char* const* paramValues,
        const int* paramLengths,
        const int* paramFormats,
        int resultFormat,               # 1 = binary
    )
    void PQclear(PGresult* res)
    int  PQresultStatus(PGresult* res)  # 2 = PGRES_TUPLES_OK

    # Result introspection
    int   PQntuples(PGresult* res) nogil
    int   PQnfields(PGresult* res) nogil
    char* PQfname(PGresult* res, int col)
    Oid   PQftype(PGresult* res, int col)
    int   PQgetisnull(PGresult* res, int row, int col) nogil
    char* PQgetvalue(PGresult* res, int row, int col) nogil
    int   PQgetlength(PGresult* res, int row, int col) nogil
    char* PQerrorMessage(PGconn* conn)
    char* PQresultErrorMessage(PGresult* res)

# --------------------------------------------------------------------------- #
# Postgres OIDs we care about
# --------------------------------------------------------------------------- #

DEF OID_BOOL      = 16
DEF OID_INT2      = 21
DEF OID_INT4      = 23
DEF OID_INT8      = 20
DEF OID_FLOAT4    = 700
DEF OID_FLOAT8    = 701
DEF OID_NUMERIC   = 1700
DEF OID_TEXT      = 25
DEF OID_VARCHAR   = 1043
DEF OID_TIMESTAMP = 1114    # microseconds since 2000-01-01 00:00:00

# --------------------------------------------------------------------------- #
# Byte-swap helpers (Postgres binary protocol is always big-endian)
# --------------------------------------------------------------------------- #

cdef inline int16_t bswap16(char* p) nogil:
    cdef unsigned char* b = <unsigned char*>p
    return <int16_t>((b[0] << 8) | b[1])

cdef inline int32_t bswap32(char* p) nogil:
    cdef unsigned char* b = <unsigned char*>p
    return <int32_t>(
        (<uint32_t>b[0] << 24) |
        (<uint32_t>b[1] << 16) |
        (<uint32_t>b[2] <<  8) |
         <uint32_t>b[3]
    )

cdef inline int64_t bswap64(char* p) nogil:
    cdef unsigned char* b = <unsigned char*>p
    return <int64_t>(
        (<int64_t>b[0] << 56) | (<int64_t>b[1] << 48) |
        (<int64_t>b[2] << 40) | (<int64_t>b[3] << 32) |
        (<int64_t>b[4] << 24) | (<int64_t>b[5] << 16) |
        (<int64_t>b[6] <<  8) |  <int64_t>b[7]
    )

cdef inline float bswap_float32(char* p) nogil:
    cdef int32_t bits = bswap32(p)
    cdef float f
    memcpy(&f, &bits, 4)
    return f

cdef inline double bswap_float64(char* p) nogil:
    cdef int64_t bits = bswap64(p)
    cdef double d
    memcpy(&d, &bits, 8)
    return d

# --------------------------------------------------------------------------- #
# Per-type column extractors
# All write directly into pre-allocated numpy array memory.
# The GIL is released for the tight inner loop.
# --------------------------------------------------------------------------- #

@cython.boundscheck(False)
@cython.wraparound(False)
cdef cnp.ndarray extract_bool(PGresult* res, int col, int nrows):
    cdef cnp.ndarray[cnp.uint8_t, ndim=1] arr = np.empty(nrows, dtype=np.bool_)
    cdef cnp.uint8_t* buf = <cnp.uint8_t*>arr.data
    cdef int row
    cdef char* val
    with nogil:
        for row in range(nrows):
            if PQgetisnull(res, row, col):
                buf[row] = 0
            else:
                val = PQgetvalue(res, row, col)
                buf[row] = val[0]
    return arr

@cython.boundscheck(False)
@cython.wraparound(False)
cdef cnp.ndarray extract_int16(PGresult* res, int col, int nrows):
    cdef cnp.ndarray[cnp.int16_t, ndim=1] arr = np.empty(nrows, dtype=np.int16)
    cdef cnp.int16_t* buf = <cnp.int16_t*>arr.data
    cdef int row
    with nogil:
        for row in range(nrows):
            if PQgetisnull(res, row, col):
                buf[row] = 0
            else:
                buf[row] = bswap16(PQgetvalue(res, row, col))
    return arr

@cython.boundscheck(False)
@cython.wraparound(False)
cdef cnp.ndarray extract_int32(PGresult* res, int col, int nrows):
    cdef cnp.ndarray[cnp.int32_t, ndim=1] arr = np.empty(nrows, dtype=np.int32)
    cdef cnp.int32_t* buf = <cnp.int32_t*>arr.data
    cdef int row
    with nogil:
        for row in range(nrows):
            if PQgetisnull(res, row, col):
                buf[row] = 0
            else:
                buf[row] = bswap32(PQgetvalue(res, row, col))
    return arr

@cython.boundscheck(False)
@cython.wraparound(False)
cdef cnp.ndarray extract_int64(PGresult* res, int col, int nrows):
    cdef cnp.ndarray[cnp.int64_t, ndim=1] arr = np.empty(nrows, dtype=np.int64)
    cdef cnp.int64_t* buf = <cnp.int64_t*>arr.data
    cdef int row
    with nogil:
        for row in range(nrows):
            if PQgetisnull(res, row, col):
                buf[row] = 0
            else:
                buf[row] = bswap64(PQgetvalue(res, row, col))
    return arr

@cython.boundscheck(False)
@cython.wraparound(False)
cdef cnp.ndarray extract_float32(PGresult* res, int col, int nrows):
    cdef cnp.ndarray[cnp.float32_t, ndim=1] arr = np.empty(nrows, dtype=np.float32)
    cdef cnp.float32_t* buf = <cnp.float32_t*>arr.data
    cdef int row
    with nogil:
        for row in range(nrows):
            if PQgetisnull(res, row, col):
                buf[row] = 0.0
            else:
                buf[row] = bswap_float32(PQgetvalue(res, row, col))
    return arr

@cython.boundscheck(False)
@cython.wraparound(False)
cdef cnp.ndarray extract_float64(PGresult* res, int col, int nrows):
    cdef cnp.ndarray[cnp.float64_t, ndim=1] arr = np.empty(nrows, dtype=np.float64)
    cdef cnp.float64_t* buf = <cnp.float64_t*>arr.data
    cdef int row
    with nogil:
        for row in range(nrows):
            if PQgetisnull(res, row, col):
                buf[row] = 0.0
            else:
                buf[row] = bswap_float64(PQgetvalue(res, row, col))
    return arr

@cython.boundscheck(False)
@cython.wraparound(False)
cdef cnp.ndarray extract_timestamp(PGresult* res, int col, int nrows):
    """
    Postgres TIMESTAMP binary = int64 microseconds since 2000-01-01.
    We store as int64 and let pandas convert to datetime64[us] afterward.
    """
    cdef cnp.ndarray[cnp.int64_t, ndim=1] arr = np.empty(nrows, dtype=np.int64)
    cdef cnp.int64_t* buf = <cnp.int64_t*>arr.data
    cdef int row
    with nogil:
        for row in range(nrows):
            if PQgetisnull(res, row, col):
                buf[row] = 0
            else:
                buf[row] = bswap64(PQgetvalue(res, row, col))
    return arr

cdef cnp.ndarray extract_numeric(PGresult* res, int col, int nrows):
    """
    Decode Postgres binary NUMERIC into a float64 array.
    Binary layout: ndigits(i16) weight(i16) sign(u16) dscale(i16) digits(ndigits*u16)
    Each digit is a base-10000 value; weight is the exponent of the first digit.
    """
    cdef cnp.ndarray[cnp.float64_t, ndim=1] arr = np.empty(nrows, dtype=np.float64)
    cdef cnp.float64_t* buf = <cnp.float64_t*>arr.data
    cdef int row, i, ndigits, weight
    cdef unsigned short sign_val
    cdef double result
    cdef char* val
    for row in range(nrows):
        if PQgetisnull(res, row, col):
            buf[row] = 0.0
        else:
            val = PQgetvalue(res, row, col)
            ndigits  = <int>bswap16(val)
            weight   = <int>bswap16(val + 2)
            sign_val = <unsigned short>bswap16(val + 4)
            if sign_val == 0xC000:  # NaN
                buf[row] = buf[row] / buf[row]  # 0/0 -> NaN (well-defined on IEEE 754)
            else:
                result = 0.0
                for i in range(ndigits):
                    result += (<unsigned short>bswap16(val + 8 + i * 2)) * (10000.0 ** (weight - i))
                buf[row] = -result if sign_val == 0x4000 else result
    return arr


cdef cnp.ndarray extract_text(PGresult* res, int col, int nrows):
    """
    Text/varchar: unavoidably Python objects — variable length, can't fit
    into a contiguous typed buffer without copying twice.
    At least we skip the SQLAlchemy type-engine overhead.
    """
    cdef cnp.ndarray arr = np.empty(nrows, dtype=object)
    cdef int row
    cdef int length
    cdef char* val
    for row in range(nrows):
        if PQgetisnull(res, row, col):
            arr[row] = None
        else:
            val = PQgetvalue(res, row, col)
            length = PQgetlength(res, row, col)
            arr[row] = val[:length].decode("utf-8")
    return arr

# --------------------------------------------------------------------------- #
# Postgres epoch offset: 2000-01-01 in numpy datetime64[us] terms
# --------------------------------------------------------------------------- #
# numpy datetime64 epoch is 1970-01-01; Postgres timestamp epoch is 2000-01-01.
# Difference in microseconds:
_PG_EPOCH_DELTA_US = 946684800000000  # microseconds between 1970-01-01 and 2000-01-01


# --------------------------------------------------------------------------- #
# Public Python-callable entry point
# --------------------------------------------------------------------------- #

def fetch_to_dataframe(str conninfo, str query):
    """
    Execute *query* against the Postgres instance described by *conninfo*
    using binary protocol, extract each column directly into a numpy array,
    and return a pandas DataFrame.

    Parameters
    ----------
    conninfo : str
        libpq connection string, e.g.
        "host=localhost dbname=benchmark user=postgres password=postgres"
    query : str
        SQL query to execute. Must be a SELECT.
    """
    import pandas as pd

    cdef bytes conninfo_b = conninfo.encode("utf-8")
    cdef bytes query_b    = query.encode("utf-8")

    # ---- connect --------------------------------------------------------
    cdef PGconn* conn = PQconnectdb(conninfo_b)
    if PQstatus(conn) != 0:  # CONNECTION_OK = 0
        msg = PQerrorMessage(conn).decode("utf-8", errors="replace")
        PQfinish(conn)
        raise ConnectionError(f"libpq connect failed: {msg}")

    # ---- execute (binary result format) ---------------------------------
    cdef PGresult* res = PQexecParams(
        conn,
        query_b,
        0,      # nParams
        NULL,   # paramTypes
        NULL,   # paramValues
        NULL,   # paramLengths
        NULL,   # paramFormats
        1,      # resultFormat: 1 = binary
    )

    if PQresultStatus(res) != 2:  # PGRES_TUPLES_OK = 2
        msg = PQresultErrorMessage(res).decode("utf-8", errors="replace")
        PQclear(res)
        PQfinish(conn)
        raise RuntimeError(f"Query failed: {msg}")

    cdef int nrows = PQntuples(res)
    cdef int ncols = PQnfields(res)

    # ---- extract columns ------------------------------------------------
    columns = {}
    cdef int col
    cdef Oid oid

    for col in range(ncols):
        name = PQfname(res, col).decode("utf-8")
        oid  = PQftype(res, col)

        if oid == OID_BOOL:
            arr = extract_bool(res, col, nrows)
        elif oid == OID_INT2:
            arr = extract_int16(res, col, nrows)
        elif oid == OID_INT4:
            arr = extract_int32(res, col, nrows)
        elif oid == OID_INT8:
            arr = extract_int64(res, col, nrows)
        elif oid == OID_FLOAT4:
            arr = extract_float32(res, col, nrows)
        elif oid == OID_FLOAT8:
            arr = extract_float64(res, col, nrows)
        elif oid == OID_TIMESTAMP:
            raw = extract_timestamp(res, col, nrows)
            # Convert Postgres epoch offset → numpy datetime64[us]
            arr = (raw + _PG_EPOCH_DELTA_US).astype("datetime64[us]")
        elif oid == OID_NUMERIC:
            arr = extract_numeric(res, col, nrows)
        elif oid in (OID_TEXT, OID_VARCHAR):
            arr = extract_text(res, col, nrows)
        else:
            # Unknown type — fall back to text extraction
            arr = extract_text(res, col, nrows)

        columns[name] = arr

    PQclear(res)
    PQfinish(conn)

    return pd.DataFrame(columns)
