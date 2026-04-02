"""
pg-benchmark runner
====================

Entry point:  pg-benchmark  (installed via pyproject.toml)
Direct run:   python -m pg_benchmark.runner

Example usage
-------------
# Full run with defaults (1k / 10k / 100k / 1M rows, 3 iterations)
pg-benchmark

# Custom row counts and skip some drivers
pg-benchmark --row-counts 1000 50000 500000 --skip asyncpg connectorx

# Re-seed even if data already exists
pg-benchmark --force-seed

# Only seed, don't benchmark
pg-benchmark --seed-only
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from pg_benchmark.benchmarks.base import run_benchmark, BenchmarkResult
from pg_benchmark.reporting import print_results, save_chart, save_csv

console = Console()

# ------------------------------------------------------------------ #
# Registry: (display_name, module_path, fetch_fn_name)
# ------------------------------------------------------------------ #
BENCHMARKS: list[tuple[str, str, str]] = [
    ("sqlalchemy_naive", "pg_benchmark.benchmarks.sqlalchemy_naive",     "fetch"),
    ("psycopg2_cursor",  "pg_benchmark.benchmarks.psycopg2_cursor",      "fetch"),
    ("psycopg3_binary",  "pg_benchmark.benchmarks.psycopg3_binary",      "fetch"),
    ("asyncpg",          "pg_benchmark.benchmarks.asyncpg_bench",        "fetch"),
    ("connectorx",       "pg_benchmark.benchmarks.connectorx_bench",     "fetch"),
    ("copy_csv",         "pg_benchmark.benchmarks.copy_csv",             "fetch"),
    ("cython_bridge",    "pg_benchmark.benchmarks.cython_bridge_bench",  "fetch"),
]

DEFAULT_ROW_COUNTS = [1_000, 10_000, 100_000, 1_000_000]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="pg-benchmark",
        description="Postgres → pandas pipeline benchmark gauntlet",
    )
    p.add_argument(
        "--row-counts",
        nargs="+",
        type=int,
        default=DEFAULT_ROW_COUNTS,
        metavar="N",
        help="Row counts to test (default: 1000 10000 100000 1000000)",
    )
    p.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Timed iterations per benchmark (default: 3)",
    )
    p.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="Warm-up iterations excluded from timing (default: 1)",
    )
    p.add_argument(
        "--skip",
        nargs="*",
        default=[],
        metavar="DRIVER",
        help=(
            "Drivers to skip. Choices: "
            + ", ".join(n for n, _, _ in BENCHMARKS)
        ),
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("results"),
        help="Directory for chart + CSV output (default: ./results)",
    )
    p.add_argument(
        "--force-seed",
        action="store_true",
        help="Re-seed the database even if data already exists",
    )
    p.add_argument(
        "--seed-only",
        action="store_true",
        help="Only seed the database, then exit",
    )
    p.add_argument(
        "--no-seed",
        action="store_true",
        help="Skip seeding entirely (assume data already exists)",
    )
    return p.parse_args()


def _import_bench(module_path: str, fn_name: str):
    import importlib
    mod = importlib.import_module(module_path)
    return getattr(mod, fn_name)


def main() -> None:
    args = parse_args()

    console.rule("[bold cyan]pg-benchmark[/bold cyan]")
    console.print(
        f"  Row counts : {[f'{n:,}' for n in args.row_counts]}\n"
        f"  Iterations : {args.iterations}  |  Warm-up: {args.warmup}\n"
        f"  Skip       : {args.skip or '(none)'}\n"
        f"  Output dir : {args.output_dir}\n"
    )

    # ------------------------------------------------------------------ #
    # Seed
    # ------------------------------------------------------------------ #
    if not args.no_seed:
        from pg_benchmark.seed import seed
        seed(force=args.force_seed)

    if args.seed_only:
        console.print("[green]Seeding complete. Exiting.[/green]")
        sys.exit(0)

    # ------------------------------------------------------------------ #
    # Run benchmarks
    # ------------------------------------------------------------------ #
    active = [(n, mp, fn) for n, mp, fn in BENCHMARKS if n not in args.skip]
    total_runs = len(active) * len(args.row_counts)
    all_results: list[BenchmarkResult] = []

    t_start = time.perf_counter()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("Running benchmarks …", total=total_runs)

        for row_count in args.row_counts:
            for name, module_path, fn_name in active:
                progress.update(
                    task,
                    description=f"[cyan]{name}[/cyan]  ·  [yellow]{row_count:,}[/yellow] rows",
                )
                try:
                    fn = _import_bench(module_path, fn_name)
                except ImportError as exc:
                    if "cython" in module_path or "Cython" in str(exc):
                        console.print(
                            f"[yellow]⚠ {name} skipped — extension not compiled. "
                            f"Run: python build_cython.py build_ext --inplace[/yellow]"
                        )
                    else:
                        console.print(f"[red]Import failed for {name}: {exc}[/red]")
                    all_results.append(BenchmarkResult.failure(name, row_count, exc))
                    progress.advance(task)
                    continue
                except Exception as exc:
                    console.print(f"[red]Import failed for {name}: {exc}[/red]")
                    all_results.append(BenchmarkResult.failure(name, row_count, exc))
                    progress.advance(task)
                    continue

                result = run_benchmark(
                    name=name,
                    fn=fn,
                    row_count=row_count,
                    warmup=args.warmup,
                    iterations=args.iterations,
                )
                all_results.append(result)
                progress.advance(task)

    total_elapsed = time.perf_counter() - t_start
    console.print(f"\n[dim]Total wall time: {total_elapsed:.1f}s[/dim]")

    # ------------------------------------------------------------------ #
    # Report
    # ------------------------------------------------------------------ #
    print_results(all_results)
    save_chart(all_results, args.output_dir)
    save_csv(all_results, args.output_dir)

    console.print("\n[bold green]Done.[/bold green]")


if __name__ == "__main__":
    main()
