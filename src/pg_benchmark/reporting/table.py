"""
Render benchmark results as a Rich table in the terminal.
Groups results by row count, one table per count.
"""

from __future__ import annotations

from rich.console import Console
from rich.table import Table
from rich import box

from pg_benchmark.benchmarks.base import BenchmarkResult

console = Console()

# Ordered list of benchmark names for consistent column ordering
BENCHMARK_ORDER = [
    "sqlalchemy_naive",
    "psycopg2_cursor",
    "psycopg3_binary",
    "asyncpg",
    "connectorx",
    "copy_csv",
    "cython_bridge",
]

DISPLAY_NAMES = {
    "sqlalchemy_naive": "SQLAlchemy (psycopg2)",
    "psycopg2_cursor":  "psycopg2 raw cursor",
    "psycopg3_binary":  "psycopg3 binary",
    "asyncpg":          "asyncpg",
    "connectorx":       "connectorx (Rust/Arrow)",
    "copy_csv":         "COPY CSV + read_csv",
    "cython_bridge":    "Cython/libpq → numpy",
}


def _ms(v: float) -> str:
    if v >= 1_000:
        return f"{v/1_000:.2f}s"
    return f"{v:.1f}ms"


def _rps(v: float) -> str:
    if v >= 1_000_000:
        return f"{v/1_000_000:.2f}M/s"
    if v >= 1_000:
        return f"{v/1_000:.1f}K/s"
    return f"{v:.0f}/s"


def print_results(results: list[BenchmarkResult]) -> None:
    row_counts = sorted({r.row_count for r in results})

    for rc in row_counts:
        subset = {r.name: r for r in results if r.row_count == rc}
        if not subset:
            continue

        # Find fastest successful result for highlighting
        fastest_ms = min(
            (r.mean_ms for r in subset.values() if r.success),
            default=None,
        )

        table = Table(
            title=f"[bold]{rc:,} rows[/bold]",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )
        table.add_column("Driver", style="bold", min_width=26)
        table.add_column("Mean", justify="right", min_width=10)
        table.add_column("Min", justify="right", min_width=10)
        table.add_column("Max", justify="right", min_width=10)
        table.add_column("Rows/sec", justify="right", min_width=12)
        table.add_column("vs baseline", justify="right", min_width=12)

        baseline_ms: float | None = None
        if "sqlalchemy_naive" in subset and subset["sqlalchemy_naive"].success:
            baseline_ms = subset["sqlalchemy_naive"].mean_ms

        for name in BENCHMARK_ORDER:
            r = subset.get(name)
            if r is None:
                continue

            display = DISPLAY_NAMES.get(name, name)

            if not r.success:
                table.add_row(display, "[red]ERROR[/red]", "", "", "", r.error[:40])
                continue

            is_fastest = fastest_ms is not None and abs(r.mean_ms - fastest_ms) < 0.001
            style = "bold green" if is_fastest else ""

            if baseline_ms and name != "sqlalchemy_naive":
                speedup = baseline_ms / r.mean_ms
                vs = f"[green]{speedup:.1f}×[/green]" if speedup > 1 else f"[red]{speedup:.1f}×[/red]"
            else:
                vs = "—"

            table.add_row(
                f"[{style}]{display}[/{style}]" if style else display,
                f"[{style}]{_ms(r.mean_ms)}[/{style}]" if style else _ms(r.mean_ms),
                _ms(r.min_ms),
                _ms(r.max_ms),
                _rps(r.rows_per_second),
                vs,
            )

        console.print()
        console.print(table)
