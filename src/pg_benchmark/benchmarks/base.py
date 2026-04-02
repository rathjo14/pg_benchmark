"""
Shared types and the timing harness used by every benchmark.
"""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass, field
from typing import Callable

import pandas as pd


@dataclass
class BenchmarkResult:
    name: str
    row_count: int
    # timing
    mean_ms: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0
    rows_per_second: float = 0.0
    # status
    success: bool = True
    error: str = ""

    @classmethod
    def failure(cls, name: str, row_count: int, exc: Exception) -> "BenchmarkResult":
        return cls(
            name=name,
            row_count=row_count,
            success=False,
            error=f"{type(exc).__name__}: {exc}",
        )


# Type alias for a benchmark function
BenchFn = Callable[[int], pd.DataFrame]


def run_benchmark(
    name: str,
    fn: BenchFn,
    row_count: int,
    *,
    warmup: int = 1,
    iterations: int = 3,
) -> BenchmarkResult:
    """
    Time *fn(row_count)* across *iterations* runs (after *warmup* warm-up
    calls that are excluded from timing).  Returns a BenchmarkResult with
    mean / min / max latency in milliseconds and rows-per-second throughput.
    """
    # --- warm-up --------------------------------------------------------
    for _ in range(warmup):
        try:
            fn(row_count)
        except Exception:
            pass  # swallow; we'll catch real errors during timed runs

    # --- timed runs -----------------------------------------------------
    elapsed: list[float] = []
    for _ in range(iterations):
        try:
            t0 = time.perf_counter()
            fn(row_count)
            elapsed.append((time.perf_counter() - t0) * 1_000)  # → ms
        except Exception as exc:
            traceback.print_exc()
            return BenchmarkResult.failure(name, row_count, exc)

    mean_ms = sum(elapsed) / len(elapsed)
    return BenchmarkResult(
        name=name,
        row_count=row_count,
        mean_ms=mean_ms,
        min_ms=min(elapsed),
        max_ms=max(elapsed),
        rows_per_second=row_count / (mean_ms / 1_000),
        success=True,
    )
