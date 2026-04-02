"""
benchmark.py -- runs all enabled strategies, collects timing, prints results.

Usage:
    python benchmark.py
    python benchmark.py --rows 10000 100000 --runs 5
    python benchmark.py --strategies connectorx copy_binary read_sql_sqlalchemy
"""
import argparse
import time
import traceback
from typing import Callable

import pandas as pd

import config
import strategies as strat
from results import print_table, plot_results

STRATEGY_MAP: dict[str, Callable] = {
    "read_sql_sqlalchemy": strat.read_sql_sqlalchemy,
    "psycopg2_cursor":     strat.psycopg2_cursor,
    "psycopg3_binary":     strat.psycopg3_binary,
    "asyncpg_fetch":       strat.asyncpg_fetch,
    "connectorx":          strat.connectorx,
    "copy_binary":         strat.copy_binary,
    "cython_bridge":       strat.cython_bridge,
}


def run_benchmark(
    strategies: list[str],
    row_counts: list[int],
    runs_per_cell: int,
) -> pd.DataFrame:
    records = []

    for n_rows in row_counts:
        print(f"\n-- {n_rows:,} rows ------------------------------")
        for name in strategies:
            fn = STRATEGY_MAP[name]
            times = []

            for run in range(runs_per_cell):
                try:
                    t0 = time.perf_counter()
                    df = fn(n_rows)
                    elapsed = time.perf_counter() - t0
                    times.append(elapsed)
                    print(f"  {name:<28} run {run+1}/{runs_per_cell}  {elapsed:.3f}s  ({len(df):,} rows)")
                except Exception as e:
                    print(f"  {name:<28} ERROR: {e}")
                    traceback.print_exc()
                    break

            if times:
                records.append({
                    "strategy":  name,
                    "n_rows":    n_rows,
                    "mean_s":    sum(times) / len(times),
                    "min_s":     min(times),
                    "max_s":     max(times),
                    "runs":      len(times),
                })

    return pd.DataFrame(records)


def main():
    parser = argparse.ArgumentParser(description="Postgres -> pandas benchmark")
    parser.add_argument(
        "--rows", type=int, nargs="+",
        default=config.ROW_COUNTS,
        help="Row counts to test",
    )
    parser.add_argument(
        "--runs", type=int,
        default=config.RUNS_PER_CELL,
        help="Timed runs per (strategy, row_count)",
    )
    parser.add_argument(
        "--strategies", nargs="+",
        default=config.ENABLED_STRATEGIES,
        choices=list(STRATEGY_MAP.keys()),
        help="Strategies to include",
    )
    parser.add_argument(
        "--no-plot", action="store_true",
        help="Skip the matplotlib chart",
    )
    args = parser.parse_args()

    print("Postgres -> pandas benchmark")
    print(f"Strategies : {args.strategies}")
    print(f"Row counts : {args.rows}")
    print(f"Runs/cell  : {args.runs}")

    results = run_benchmark(args.strategies, args.rows, args.runs)

    print("\n\n-- Results -------------------------------------------------")
    print_table(results)

    if not args.no_plot:
        plot_results(results)


if __name__ == "__main__":
    main()
