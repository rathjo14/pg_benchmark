"""
Export all benchmark results to a CSV file for further analysis.
"""

from __future__ import annotations

import csv
from pathlib import Path

from pg_benchmark.benchmarks.base import BenchmarkResult

FIELDS = [
    "name",
    "row_count",
    "mean_ms",
    "min_ms",
    "max_ms",
    "rows_per_second",
    "success",
    "error",
]


def save_csv(results: list[BenchmarkResult], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "benchmark_results.csv"

    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for r in results:
            writer.writerow(
                {
                    "name": r.name,
                    "row_count": r.row_count,
                    "mean_ms": f"{r.mean_ms:.3f}",
                    "min_ms": f"{r.min_ms:.3f}",
                    "max_ms": f"{r.max_ms:.3f}",
                    "rows_per_second": f"{r.rows_per_second:.0f}",
                    "success": r.success,
                    "error": r.error,
                }
            )

    print(f"[csv] Saved → {out_path}")
    return out_path
