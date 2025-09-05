import os
import duckdb
import matplotlib.pyplot as plt
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

# --- Load ski paths from mart ---
paths_df = con.execute("""
    SELECT * 
    FROM camonairflow.public_common.mart_nz_ski_paths
    WHERE resort NOT IN (
        'Fox Peak Ski Area', 'Mount Olympus Ski Area',
        'Rainbow Ski Area', 'Broken River Ski Area', 'Temple Basin Ski Area',
        'Mount Lyford Alpine Resort', 'Roundhill Ski Field', 'Mount Dobson Ski Field'
    )
""").df()
con.close()

# ========================================
# Helpers
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

def wrap_path_lines(path: str, width: int = 70):
    lines = textwrap.wrap(path, width=width) if path else [""]
    return lines

# ========================================
# Plot one resort per figure (safe text flow)
# ========================================

def plot_resort(resort, df_l, df_v, df_s, df_sl, out_dir="charts", dpi=350):
    os.makedirs(out_dir, exist_ok=True)

    blocks = [
        (df_l, "Top Length", lambda r: f"{r.total_distance_m:.0f} m"),
        (df_v, "Top Vertical Drop", lambda r: f"{r.total_vertical_m:.0f} m"),
        (df_s, "Top Same-Lift Run", lambda r: f"{r.total_distance_m:.0f} m"),
        (df_sl, "Top Steep & Long", lambda r: f"{r.total_distance_m:.0f} m"),
    ]

    # Build lines sequentially
    all_lines = [("title", f"{resort} — Top 1 Path per Lift")]
    for df, metric_name, fmt in blocks:
        all_lines.append(("category", metric_name))
        data = df[df["resort"] == resort]
        for row in data.itertuples():
            all_lines.append(("lift", f"- {row.starting_lift}:"))
            for ln in wrap_path_lines(row.run_path):
                all_lines.append(("path", f"   {ln}"))
            all_lines.append(
                ("stat", f"   • {fmt(row)} (avg {row.avg_gradient_deg:.1f}° / max {row.max_gradient_deg:.1f}°)")
            )
            all_lines.append(("gap", ""))  # spacing between lifts
        all_lines.append(("gap", ""))  # spacing between categories

    # Figure size proportional to line count
    fig_h = max(6, len(all_lines) * 0.35)
    fig, ax = plt.subplots(figsize=(14, fig_h))
    ax.axis("off")

    y = 1.0
    line_step = 1.0 / (len(all_lines) + 5)

    for kind, text in all_lines:
        if not text.strip():
            y -= line_step * 0.5
            continue

        if kind == "title":
            ax.text(0.0, y, text, fontsize=20, fontweight="bold",
                    ha="left", va="top", transform=ax.transAxes)
        elif kind == "category":
            ax.text(0.0, y, text, fontsize=14, fontweight="bold",
                    ha="left", va="top", transform=ax.transAxes)
        elif kind == "lift":
            ax.text(0.02, y, text, fontsize=11, fontweight="bold",
                    ha="left", va="top", transform=ax.transAxes)
        elif kind == "path":
            ax.text(0.05, y, text, fontsize=11,
                    ha="left", va="top", transform=ax.transAxes)
        elif kind == "stat":
            ax.text(0.05, y, text, fontsize=10,
                    ha="left", va="top", transform=ax.transAxes)
        y -= line_step

    out_path = os.path.join(out_dir, f"ski_paths_{resort.replace(' ', '_').lower()}_top1_perlift.png")
    plt.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    print(f"✅ Saved {out_path}")

# ========================================
# Generate per-resort visuals
# ========================================

all_resorts = sorted(paths_df["resort"].unique())
for resort in all_resorts:
    plot_resort(resort, top_by_length, top_by_vertical, top_by_same_lift, top_steep_long, dpi=350)
