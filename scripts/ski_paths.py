import os
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
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

# --- Load ski paths from mart ---
paths_df = con.execute("""
    SELECT *
    FROM camonairflow.public_common.mart_nz_ski_paths
    WHERE resort NOT IN ('Fox Peak Ski Area', 'Mount Olympus Ski Area', 
                         'Rainbow Ski Area', 'Broken River Ski Area', 'Temple Basin Ski Area')
""").df()
con.close()

# ========================================
# 1) Scatter plot — Gradient vs Distance
# ========================================

def plot_scatter_faceted(df):
    resorts = sorted(df["resort"].unique())
    n_resorts = len(resorts)

    ncols = 3
    nrows = (n_resorts + ncols - 1) // ncols

    x_min, x_max = 0, df["total_distance_m"].max()
    y_min, y_max = 0, 35

    ending_palette = {
        "same_lift": "#1f77b4",
        "different_lift": "#2ca02c",
        "run_end": "#d62728"
    }

    fig, axes = plt.subplots(
        nrows, ncols, figsize=(6 * ncols, 5 * nrows),
        sharex=False, sharey=False
    )
    axes = axes.flatten()

    for i, resort in enumerate(resorts):
        ax = axes[i]
        subdf = df[df["resort"] == resort]
        sns.scatterplot(
            data=subdf,
            x="total_distance_m",
            y="avg_gradient_deg",
            hue="ending_type",
            palette=ending_palette,
            alpha=0.6,
            s=60,
            edgecolor="k",
            ax=ax,
            legend=False
        )
        ax.set_title(resort, fontsize=14, fontweight="bold")
        ax.set_xlabel("Distance (m)")
        ax.set_ylabel("Avg Gradient (°)")
        ax.grid(True, linestyle="--", alpha=0.5)
        ax.set_xlim(x_min, x_max)
        ax.set_ylim(y_min, y_max)

        for tick in ax.get_xticklabels():
            tick.set_rotation(45)

    for j in range(i + 1, len(axes)):
        axes[j].axis("off")

    import matplotlib.patches as mpatches
    legend_handles = [mpatches.Patch(color=c, label=k) for k, c in ending_palette.items()]
    fig.legend(
        handles=legend_handles,
        title="Ending Type",
        fontsize=11, title_fontsize=12,
        loc="center left",
        bbox_to_anchor=(1.02, 0.5)
    )

    fig.suptitle("NZ Ski Paths — Gradient vs Distance (by Resort)", fontsize=18, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 0.85, 0.96])
    fig.savefig("charts/nz_scatter_faceted.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

plot_scatter_faceted(paths_df)
