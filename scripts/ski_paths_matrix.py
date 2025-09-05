import os
import duckdb
import matplotlib.pyplot as plt
import textwrap
from math import ceil
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
    WHERE resort NOT IN (
        'Fox Peak Ski Area', 'Mount Olympus Ski Area',
        'Rainbow Ski Area', 'Broken River Ski Area', 'Temple Basin Ski Area', 'Mount Lyford Alpine Resort',
                       'Roundhill Ski Field', 'Mount Dobson Ski Field'
    )
""").df()
con.close()


# ========================================
# Helpers for Top 1 per lift
# ========================================

def top_1_per_lift_resort(df, column, ending_type=None, min_length=None):
    sub = df.copy()
    if ending_type:
        sub = sub[sub["ending_type"] == ending_type]
    if min_length is not None:
        sub = sub[sub["total_distance_m"] >= min_length]
    return (
        sub.sort_values(column, ascending=False)
        .groupby(["resort", "starting_lift"])
        .head(1)[[
            "resort", "starting_lift", "run_path",
            "avg_gradient_deg", "max_gradient_deg",
            "total_distance_m", "total_vertical_m",
            column, "ending_type"
        ]]
    )


# Ranked DataFrames
top_by_length = top_1_per_lift_resort(paths_df, "total_distance_m")
top_by_vertical = top_1_per_lift_resort(paths_df, "total_vertical_m")
top_by_same_lift = top_1_per_lift_resort(paths_df, "total_distance_m", ending_type="same_lift")
top_steep_long = top_1_per_lift_resort(paths_df, "avg_gradient_deg", min_length=500)


# ========================================
# Layout helpers
# ========================================

def wrap_path_lines(path: str, width: int = 50):
    lines = textwrap.wrap(path, width=width) if path else [""]
    if len(lines) > 1:
        lines = [lines[0]] + [f"   {ln}" for ln in lines[1:]]
    return lines


def estimate_resort_units(resort: str, blocks):
    units = 2
    for df, _, fmt in blocks:
        data = df[df["resort"] == resort]
        if data.empty:
            continue
        units += 1
        for row in data.itertuples():
            p_lines = wrap_path_lines(row.run_path, width=50)
            units += len(p_lines) + 1
        units += 1
    return units


def draw_resort_block(ax, resort: str, blocks, fonts):
    ax.axis("off")
    total_units = estimate_resort_units(resort, blocks)
    used = 0

    def y_pos():
        return 1 - (used / total_units)

    # Resort header
    ax.text(
        0.0, y_pos(), resort,
        fontsize=fonts["header"], fontweight="bold",
        transform=ax.transAxes, ha="left", va="top"
    )
    used += 2

    # Metrics
    for df, metric_name, fmt in blocks:
        data = df[df["resort"] == resort]
        if data.empty:
            continue
        ax.text(
            0.02, y_pos(), metric_name,
            fontsize=fonts["metric"], fontweight="bold",
            transform=ax.transAxes, ha="left", va="top"
        )
        used += 1

        for row in data.itertuples():
            p_lines = wrap_path_lines(row.run_path, width=50)
            first = f"- {row.starting_lift}: {p_lines[0]}" if p_lines else f"- {row.starting_lift}:"
            ax.text(
                0.04, y_pos(), first,
                fontsize=fonts["body"], transform=ax.transAxes, ha="left", va="top"
            )
            used += 1
            for ln in p_lines[1:]:
                ax.text(
                    0.07, y_pos(), ln,
                    fontsize=fonts["body"], transform=ax.transAxes, ha="left", va="top"
                )
                used += 1
            # Show avg & max gradient
            ax.text(
                0.07, y_pos(),
                f"• {fmt(row)} (avg {row.avg_gradient_deg:.2f}° / max {row.max_gradient_deg:.2f}°)",
                fontsize=fonts["body"], transform=ax.transAxes, ha="left", va="top"
            )
            used += 1
        used += 1


# ========================================
# Multi-column poster matrix (auto font scaling)
# ========================================

def plot_matrix_multicolumn(
    df_l, df_v, df_s, df_sl, resorts,
    out_path="charts/nz_top1_perlift_multicol.png",
    ncols=5, dpi=300
):
    """
    Create a wide poster layout with multiple columns of resorts.
    Fonts auto-scale depending on ncols.
    """
    # Scale fonts dynamically based on number of columns
    if ncols <= 2:
        fonts = {"header": 16, "metric": 12, "body": 10}
    elif ncols == 3:
        fonts = {"header": 14, "metric": 11, "body": 9}
    elif ncols == 4:
        fonts = {"header": 13, "metric": 10, "body": 9}
    else:  # 5+ cols → more compact
        fonts = {"header": 12, "metric": 9, "body": 8}

    blocks = [
        (df_l, "Top Length", lambda r: f"{r.total_distance_m:.0f} m"),
        (df_v, "Top Vertical Drop", lambda r: f"{r.total_vertical_m:.0f} m"),
        (df_s, "Top Same-Lift Run", lambda r: f"{r.total_distance_m:.0f} m"),
        (df_sl, "Top Steep & Long", lambda r: f"{r.total_distance_m:.0f} m"),
    ]

    n_resorts = len(resorts)
    nrows = ceil(n_resorts / ncols)

    ratios = [max(estimate_resort_units(resort, blocks), 6) for resort in resorts]
    row_ratios = []
    for i in range(nrows):
        row_ratios.append(max(ratios[i * ncols:(i + 1) * ncols] or [6]))

    page_w = 22
    fig_h = max(ceil(sum(row_ratios) * 0.22), 12)

    fig = plt.figure(figsize=(page_w, fig_h), constrained_layout=True)
    gs = fig.add_gridspec(nrows=nrows, ncols=ncols, height_ratios=row_ratios)

    for idx, resort in enumerate(resorts):
        r, c = divmod(idx, ncols)
        ax = fig.add_subplot(gs[r, c])
        draw_resort_block(ax, resort, blocks, fonts)

    fig.suptitle(
        "NZ Ski Resorts — Top 1 Path per Lift (Length, Vertical, Same-Lift, Steep & Long)\n"
        "(showing avg and max gradient)",
        fontsize=18, fontweight="bold", y=1.02
    )
    plt.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


# --- Export multi-column big poster ---
all_resorts = sorted(paths_df["resort"].unique())
plot_matrix_multicolumn(
    top_by_length, top_by_vertical, top_by_same_lift, top_steep_long,
    all_resorts, out_path="charts/nz_top1_perlift_multicol.png", ncols=4, dpi=250
)
