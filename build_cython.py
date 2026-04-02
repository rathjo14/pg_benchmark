"""
Build the Cython bridge extension.

Usage (from the project root):
    python build_cython.py build_ext --inplace

Requirements:
    pip install cython numpy
    apt install libpq-dev     # Ubuntu/Debian
    brew install libpq         # macOS  (then set LDFLAGS/CFLAGS, see below)

macOS note
----------
Homebrew puts libpq in a non-standard location. Run:
    export LDFLAGS="-L$(brew --prefix libpq)/lib"
    export CPPFLAGS="-I$(brew --prefix libpq)/include"
    python build_cython.py build_ext --inplace

After a successful build you'll see:
    src/pg_benchmark/cython_bridge/bridge.cpython-3xx-*.so
"""

import os
import sys
import subprocess
from pathlib import Path

import numpy as np
from setuptools import setup, Extension

try:
    from Cython.Build import cythonize
except ImportError:
    print("Cython not found. Install it with:  pip install cython")
    sys.exit(1)

# ------------------------------------------------------------------ #
# Locate libpq headers and library
# ------------------------------------------------------------------ #

def _pg_config(flag: str) -> str:
    """Query pg_config for include/lib dirs."""
    try:
        result = subprocess.run(
            ["pg_config", flag], capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return ""


pg_includedir = _pg_config("--includedir") or r"C:\pgsql\include"
pg_libdir     = _pg_config("--libdir")     or r"C:\pgsql\lib"

# Allow env-var overrides for non-standard installs
pg_includedir = os.environ.get("PG_INCLUDE_DIR", pg_includedir)
pg_libdir     = os.environ.get("PG_LIB_DIR",     pg_libdir)

print(f"[build] libpq headers : {pg_includedir}")
print(f"[build] libpq lib dir : {pg_libdir}")

# ------------------------------------------------------------------ #
# Extension definition
# ------------------------------------------------------------------ #

bridge_ext = Extension(
    name="pg_benchmark.cython_bridge.bridge",
    sources=["src/pg_benchmark/cython_bridge/bridge.pyx"],
    include_dirs=[
        np.get_include(),       # numpy/arrayobject.h
        pg_includedir,          # libpq-fe.h
    ],
    library_dirs=[pg_libdir],
    libraries=["libpq"] if sys.platform == "win32" else ["pq"],
    extra_compile_args=["/O2"] if sys.platform == "win32" else ["-O3", "-ffast-math", "-march=native"],
    extra_link_args=[],
)

# ------------------------------------------------------------------ #
# Build
# ------------------------------------------------------------------ #

setup(
    name="pg-benchmark-bridge",
    ext_modules=cythonize(
        [bridge_ext],
        compiler_directives={
            "language_level": "3",
            "boundscheck":    False,
            "wraparound":     False,
            "cdivision":      True,
            "nonecheck":      False,
        },
        annotate=True,   # produces bridge.html for profiling inspection
    ),
    zip_safe=False,
)
