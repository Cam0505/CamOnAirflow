from matplotlib.lines import Line2D
import duckdb
import os
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.signal import savgol_filter

# --- ENV, DuckDB connection ---
paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")
con = duckdb.connect(database_string)

# --- Load Data ---
RESORTS = ['Remarkables', 'Mount Hutt', 'Cardrona', 'Treble Cone', 'Coronet Peak', 'Turoa', 'Whakapapa']

points = con.execute(f"""
    SELECT
        osm_id,
        resort,
        country_code,
        run_name,
        point_index,
        distance_along_run_m,
        elevation_m
    FROM camonairflow.main.ski_run_points
    WHERE run_name <> ''
    AND resort in {tuple(RESORTS)}
""").df()

runs = con.execute(f"""
    SELECT
        osm_id,
        resort,
        country_code,
        run_name,
        difficulty,
        piste_type,
        run_length_m,
        n_points
    FROM camonairflow.main.ski_runs
    WHERE run_name <> ''
    AND resort in {tuple(RESORTS)}
""").df()

# Merge run info into points
points = points.merge(runs[['osm_id', 'difficulty', 'run_length_m', 'n_points']], on='osm_id', how='left')

# Filter: Only runs with enough points and length
MIN_LENGTH = 200
MIN_POINTS = 6
runs_filtered = runs[(runs['run_length_m'] >= MIN_LENGTH) & (runs['n_points'] >= MIN_POINTS)].copy()
points = points[points['osm_id'].isin(runs_filtered['osm_id'])].copy()

# Smoothing function
def smooth_elevation(y, window=9, poly=2):
    n = len(y)
    if n < window:
        return y
    window = min(window if window % 2 == 1 else window + 1, n)
    if window < 3:
        return y
    return savgol_filter(y, window, poly)

# Color palette
difficulty_colors = {
    "novice": "#4daf4a",
    "easy": "#377eb8",
    "intermediate": "#ff7f00",
    "advanced": "#e41a1c",
    "expert": "#a65628",
    "extreme": "#800080",
    "freeride": "#00CED1",
    None: "#999999",
    "nan": "#999999"
}
all_diffs = points['difficulty'].dropna().unique()
for diff in all_diffs:
    if diff not in difficulty_colors:
        difficulty_colors[diff] = "#444444"

# --- Compute average gradient for each difficulty at each resort ---
gradient_labels = {}
for resort in RESORTS:
    subset = points[points['resort'] == resort]
    grads = {}
    for diff in subset['difficulty'].dropna().unique():
        diff_runs = subset[subset['difficulty'] == diff]['osm_id'].unique()
        grad_vals = []
        for osm_id in diff_runs:
            run_pts = subset[subset['osm_id'] == osm_id].sort_values('distance_along_run_m')
            if len(run_pts) > 1:
                elevation_drop = run_pts['elevation_m'].iloc[0] - run_pts['elevation_m'].iloc[-1]
                distance = run_pts['distance_along_run_m'].iloc[-1] - run_pts['distance_along_run_m'].iloc[0]
                if distance > 0:
                    grad = 100 * elevation_drop / distance
                    grad_vals.append(grad)
        if grad_vals:
            grads[diff] = np.mean(grad_vals)
    # sort by mean gradient, descending
    label_data = sorted(grads.items(), key=lambda x: -x[1])
    gradient_labels[resort] = label_data

# --- Subplot grid logic ---
ncols = 4
nrows = int(np.ceil(len(RESORTS) / ncols))
fig, axes = plt.subplots(nrows, ncols, figsize=(28, 2 + nrows * 8), sharey=True)
axes = axes.flatten()

for ax, resort in zip(axes, RESORTS):
    runs_this = runs_filtered[runs_filtered['resort'] == resort]
    for osm_id in runs_this['osm_id']:
        pts = points[points['osm_id'] == osm_id].sort_values('distance_along_run_m')
        # Ensure downhill:
        if len(pts) > 1 and pts.iloc[0]['elevation_m'] < pts.iloc[-1]['elevation_m']:
            pts = pts.iloc[::-1]
        y_smoothed = smooth_elevation(pts['elevation_m'].values)
        color = difficulty_colors.get(pts['difficulty'].iloc[0], "#444444")
        ax.plot(pts['distance_along_run_m'], y_smoothed,
                color=color, alpha=0.8, linewidth=2)
    # Label the longest run at its end
    if not runs_this.empty:
        longest = runs_this.loc[runs_this['run_length_m'].idxmax()]
        pts_long = points[points['osm_id'] == longest['osm_id']].sort_values('distance_along_run_m')
        ax.text(pts_long['distance_along_run_m'].iloc[-1], pts_long['elevation_m'].iloc[-1],
                longest['run_name'], fontsize=10, fontweight='bold', color='black', ha='right', va='bottom')
    ax.set_title(resort, fontsize=18, fontweight='bold')
    ax.set_xlabel("Run Distance Along Slope (meters)", fontsize=13)
    ax.grid(True, linestyle='--', alpha=0.4)

    # --- Avg Gradient Labels (top right, improved position) ---
    label_data = gradient_labels.get(resort, [])
    if label_data:
        ymin, ymax = ax.get_ylim()
        xmin, xmax = ax.get_xlim()
        y = ymax - 0.04 * (ymax - ymin)     # closer to the top
        x = xmax - 0.01 * (xmax - xmin)     # closer to right
        lineheight = 0.062 * (ymax - ymin)  # spacing
        for i, (diff, grad) in enumerate(label_data):
            color = difficulty_colors.get(diff, "#444444")
            ax.text(
                x, y - i * lineheight,
                f"{diff}: {grad:.1f}%",
                fontsize=12, color=color,
                ha='right', va='top', fontweight='bold', alpha=0.97,
                bbox=dict(facecolor='white', alpha=0.8, edgecolor='none', boxstyle='round,pad=0.24')
            )

axes[0].set_ylabel("Elevation (m)", fontsize=13)

# Hide empty subplots
for i in range(len(RESORTS), len(axes)):
    fig.delaxes(axes[i])

# --- Global legend ---
legend_elements = [
    Line2D([0], [0], color=color, lw=3, label=diff if diff else 'nan')
    for diff, color in difficulty_colors.items() if diff in all_diffs
]
fig.legend(
    handles=legend_elements,
    loc='lower center',
    ncol=5,
    fontsize=14,
    title="Difficulty",
    title_fontsize=15,
    bbox_to_anchor=(0.5, -0.025)
)

plt.suptitle(
    'Ski Run Elevation Profiles by Resort\nAll runs (filtered, smoothed, colored by difficulty)\nAvg gradient per difficulty (top right)',
    fontsize=24, fontweight='bold', y=1.06)
plt.tight_layout(rect=(0, 0, 1, 0.98))
plt.savefig("charts/ski_run_elevations_matplotlib.png", dpi=160, bbox_inches='tight')
plt.show()
print("✅ Saved chart: ski_run_elevations_matplotlib.png")
