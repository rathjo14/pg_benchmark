"""
results.py — pretty-print a results DataFrame and produce a chart.
"""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from tabulate import tabulate


# Colours per strategy
PALETTE = {
    "read_sql_sqlalchemy": "#e05c5c",   # red   — slowest baseline
    "psycopg2_cursor":     "#e08a5c",   # orange
    "psycopg3_binary":     "#e0c45c",   # yellow
    "asyncpg_fetch":       "#8ae05c",   # green
    "connectorx":          "#5cb8e0",   # blue
    "copy_binary":         "#a05ce0",   # purple
}


def print_table(df: pd.DataFrame) -> None:
    """Print a pivot table: rows = row_counts, cols = strategies."""
    pivot = df.pivot_table(index="n_rows", columns="strategy", values="mean_s")

    # Add a speedup column vs the baseline
    if "read_sql_sqlalchemy" in pivot.columns:
        baseline = pivot["read_sql_sqlalchemy"]
        for col in pivot.columns:
            if col != "read_sql_sqlalchemy":
                pivot[f"{col} speedup"] = (baseline / pivot[col]).map("{:.1f}x".format)

    print(tabulate(pivot, headers="keys", tablefmt="simple", floatfmt=".3f"))


def plot_results(df: pd.DataFrame, output: str = "benchmark_results.png") -> None:
    row_counts = sorted(df["n_rows"].unique())
    strategies = df["strategy"].unique().tolist()

    fig, axes = plt.subplots(
        1, len(row_counts),
        figsize=(4.5 * len(row_counts), 6),
        sharey=False,
    )
    if len(row_counts) == 1:
        axes = [axes]

    fig.suptitle("Postgres -> pandas: strategy latency comparison", fontsize=14, fontweight="bold")

    for ax, n_rows in zip(axes, row_counts):
        subset = df[df["n_rows"] == n_rows].sort_values("mean_s", ascending=False)
        colors = [PALETTE.get(s, "#aaaaaa") for s in subset["strategy"]]
        bars = ax.barh(subset["strategy"], subset["mean_s"], color=colors, edgecolor="white")

        # Annotate bars with seconds
        for bar, val in zip(bars, subset["mean_s"]):
            ax.text(
                bar.get_width() + bar.get_width() * 0.02,
                bar.get_y() + bar.get_height() / 2,
                f"{val:.3f}s",
                va="center", ha="left", fontsize=8.5,
            )

        ax.set_title(f"{n_rows:,} rows", fontsize=11)
        ax.set_xlabel("Mean time (seconds)")
        ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("%.2fs"))
        ax.invert_yaxis()
        ax.grid(axis="x", linestyle="--", alpha=0.4)
        ax.spines[["top", "right"]].set_visible(False)

    plt.tight_layout()
    plt.savefig(output, dpi=150, bbox_inches="tight")
    print(f"\nChart saved -> {output}")
    plt.show()
