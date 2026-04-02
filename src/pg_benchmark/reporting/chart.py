"""
Save benchmark results as a grouped bar chart (one panel per row count).
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

from pg_benchmark.benchmarks.base import BenchmarkResult

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
    "sqlalchemy_naive": "SQLAlchemy\n(psycopg2)",
    "psycopg2_cursor":  "psycopg2\nraw cursor",
    "psycopg3_binary":  "psycopg3\nbinary",
    "asyncpg":          "asyncpg",
    "connectorx":       "connectorx\n(Rust/Arrow)",
    "copy_csv":         "COPY CSV\n+read_csv",
    "cython_bridge":    "Cython/libpq\n→numpy",
}

COLORS = [
    "#e07b54",  # sqlalchemy_naive
    "#f0a868",  # psycopg2_cursor
    "#f5d08a",  # psycopg3_binary
    "#8bc4a8",  # asyncpg
    "#4a9e7f",  # connectorx
    "#5b8fd4",  # copy_csv
    "#9b72cf",  # cython_bridge  (purple — the contender)
]


def save_chart(results: list[BenchmarkResult], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    row_counts = sorted({r.row_count for r in results})
    n_panels = len(row_counts)

    fig, axes = plt.subplots(
        1, n_panels, figsize=(6 * n_panels, 5), sharey=False
    )
    if n_panels == 1:
        axes = [axes]

    fig.suptitle(
        "Postgres → pandas: mean latency by driver",
        fontsize=14,
        fontweight="bold",
        y=1.02,
    )

    for ax, rc in zip(axes, row_counts):
        subset = {r.name: r for r in results if r.row_count == rc and r.success}
        names = [n for n in BENCHMARK_ORDER if n in subset]
        means = [subset[n].mean_ms for n in names]
        colors = [COLORS[BENCHMARK_ORDER.index(n)] for n in names]
        labels = [DISPLAY_NAMES.get(n, n) for n in names]

        x = np.arange(len(names))
        bars = ax.bar(x, means, color=colors, width=0.6, edgecolor="white", linewidth=0.5)

        # Annotate bar tops
        for bar, val in zip(bars, means):
            label = f"{val/1000:.2f}s" if val >= 1000 else f"{val:.0f}ms"
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() * 1.015,
                label,
                ha="center",
                va="bottom",
                fontsize=8,
                fontweight="bold",
            )

        ax.set_title(f"{rc:,} rows", fontsize=12, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=8)
        ax.set_ylabel("Mean latency (ms)", fontsize=9)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
        ax.spines[["top", "right"]].set_visible(False)
        ax.set_ylim(bottom=0, top=max(means) * 1.2 if means else 1)
        ax.grid(axis="y", alpha=0.3, linestyle="--")

    fig.tight_layout()
    out_path = output_dir / "benchmark_results.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[chart] Saved → {out_path}")
    return out_path
