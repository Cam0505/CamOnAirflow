import os
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import textwrap
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars

# --- ENV, DuckDB connection ---
paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")
con = duckdb.connect(database_string)

# --- Load ski paths (Cardrona only) ---
paths_df = con.execute("""
    SELECT *
    FROM camonairflow.public_staging.staging_all_ski_paths
    WHERE resort = 'Cardrona Alpine Resort'
""").df()
con.close()

# ========================================
# 1. Scatter plot — Gradient vs Distance
# ========================================

def plot_scatter(df):
    # Facebook (landscape 16:9)
    fig, ax = plt.subplots(figsize=(12, 7))
    sns.scatterplot(
        data=df,
        x="total_distance_m",
        y="avg_gradient",
        hue="ending_type",
        palette="Set2",
        alpha=0.6,
        s=100,
        edgecolor="k",
        ax=ax
    )
    ax.set_title("Cardrona Ski Paths — Gradient vs Distance", fontsize=18, fontweight="bold")
    ax.set_xlabel("Total Distance (m)", fontsize=14)
    ax.set_ylabel("Average Gradient (°)", fontsize=14)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(title="Ending Type", fontsize=11, title_fontsize=12)
    fig.tight_layout()
    fig.savefig("charts/cardrona_scatter_fb.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    # Instagram (square 1:1)
    fig, ax = plt.subplots(figsize=(8, 8))
    sns.scatterplot(
        data=df,
        x="total_distance_m",
        y="avg_gradient",
        hue="ending_type",
        palette="Set2",
        alpha=0.6,
        s=100,
        edgecolor="k",
        ax=ax
    )
    ax.set_title("Cardrona Ski Paths — Gradient vs Distance", fontsize=18, fontweight="bold")
    ax.set_xlabel("Total Distance (m)", fontsize=14)
    ax.set_ylabel("Average Gradient (°)", fontsize=14)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(title="Ending Type", fontsize=11, title_fontsize=12)
    fig.tight_layout()
    fig.savefig("charts/cardrona_scatter_insta.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

plot_scatter(paths_df)

# ========================================
# 2. Matrix-style Top 5 per Lift
# ========================================

def top_n_per_lift(df, column, n=5):
    """Return top-N ski paths per lift, sorted by a given column."""
    return (
        df.sort_values(column, ascending=False)
          .groupby("starting_lift")
          .head(n)[["starting_lift", "run_path", "avg_gradient", "total_distance_m", "total_vertical_m", column]]
    )

# Create ranked DataFrames
top_by_gradient = top_n_per_lift(paths_df, "avg_gradient")
top_by_length = top_n_per_lift(paths_df, "total_distance_m")
top_by_vertical = top_n_per_lift(paths_df, "total_vertical_m")

# Unique lifts
lifts = paths_df["starting_lift"].unique()
n_lifts = len(lifts)

def plot_matrix(df_g, df_l, df_v, lifts, fb=True):
    # FB = landscape, Insta = portrait
    if fb:
        fig, axes = plt.subplots(n_lifts, 3, figsize=(20, 6 * n_lifts))
        out_path = "charts/cardrona_top5_matrix_fb.png"
    else:
        fig, axes = plt.subplots(n_lifts, 3, figsize=(12, 6 * n_lifts))
        out_path = "charts/cardrona_top5_matrix_insta.png"

    if n_lifts == 1:
        axes = [axes]

    for i, lift in enumerate(lifts):
        for j, (df, metric_name, col, label_fmt) in enumerate([
            (df_g, "Top 5 by Avg Gradient (°)", "avg_gradient", lambda r: f"{r.avg_gradient:.1f}°"),
            (df_l, "Top 5 by Path Length (m)", "total_distance_m", lambda r: f"{r.total_distance_m:.0f} m"),
            (df_v, "Top 5 by Vertical Drop (m)", "total_vertical_m", lambda r: f"{r.total_vertical_m:.0f} m"),
        ]):
            ax = axes[i][j] if n_lifts > 1 else axes[j]
            data = df[df["starting_lift"] == lift]

            ax.axis("off")
            title = f"{lift}\n{metric_name}" if j == 0 else metric_name
            ax.text(0.5, 1.05, title, ha="center", va="bottom", fontsize=14, fontweight="bold", transform=ax.transAxes)

            for k, row in enumerate(data.itertuples()):
                # Wrap the run_path to avoid overflow
                wrapped_path = "\n     ".join(textwrap.wrap(row.run_path, width=40))
                text = (
                    f"{k+1}. {wrapped_path}\n"
                    f"   • {label_fmt(row)}"
                )
                ax.text(0.01, 0.9 - k * 0.18, text,
                        ha="left", va="top", fontsize=11, transform=ax.transAxes,
                        bbox=dict(facecolor="white", alpha=0.6, edgecolor="none"))

    plt.tight_layout()
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

# Export FB + Insta versions
plot_matrix(top_by_gradient, top_by_length, top_by_vertical, lifts, fb=True)   # Facebook
plot_matrix(top_by_gradient, top_by_length, top_by_vertical, lifts, fb=False)  # Instagram
