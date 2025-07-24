from scipy.signal import savgol_filter
import duckdb
import os
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
import matplotlib.pyplot as plt
import numpy as np

# --- ENV, DuckDB connection ---
paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")
con = duckdb.connect(database_string)

RESORTS = ['Remarkables', 'Mount Hutt', 'Cardrona', 'Treble Cone', 'Coronet Peak',
           'Temple Basin', 'Mount Cheeseman', 'Mount Dobson', 'Mount Olympus']

# --- Load from correct point table ---
points = con.execute(f"""
    SELECT
        osm_id,
        resort,
        distance_along_run_m,
        elevation_m
    FROM camonairflow.main.ski_run_points
    WHERE resort in {tuple(RESORTS)}
""").df()

runs = con.execute(f"""
    SELECT
        osm_id,
        resort,
        run_name,
        case when difficulty = 'extreme' then 'intermediate' 
            when difficulty = 'expert' then 'advanced'
            else difficulty end as difficulty,
        run_length_m
    FROM camonairflow.public_base.base_filtered_ski_runs
    WHERE resort in {tuple(RESORTS)}
""").df()

gradient_stats = con.execute(f"""
    SELECT
        resort,
        case when difficulty = 'extreme' then 'intermediate' 
            when difficulty = 'expert' then 'advanced'
            else difficulty end as difficulty,
        run_count,
        mean_gradient
    FROM camonairflow.public_base.base_ski_gradient_stats
    WHERE resort in {tuple(RESORTS)}
""").df()

# Color palette
difficulty_colors = {
    "novice": "#4daf4a",
    "easy": "#377eb8",
    "intermediate": "#d90f0f",
    "advanced": "#0e0d0d",
    "freeride": "#00CED1",
    None: "#999999",
    "nan": "#999999"
}
all_diffs = runs['difficulty'].dropna().unique()
for diff in all_diffs:
    if diff not in difficulty_colors:
        difficulty_colors[diff] = "#444444"

# Smoothing function
def smooth_elevation(y, window=9, poly=2):
    n = len(y)
    if n < window:
        return y
    window = min(window if window % 2 == 1 else window + 1, n)
    if window < 3:
        return y
    return savgol_filter(y, window, poly)

max_distance = points['distance_along_run_m'].max() * 1.05

ncols = 3
nrows = 3  # Always 3x3 grid for 7 resorts + 2 blanks for legend
fig, axes = plt.subplots(nrows, ncols, figsize=(26, 20), sharey=True)
axes = axes.flatten()

# Find min/max elevation for nice axis consistency
min_elev = int(np.floor(points['elevation_m'].min() / 100.0) * 100)
max_elev = int(np.ceil(points['elevation_m'].max() / 100.0) * 100)

for ax, resort in zip(axes, RESORTS):
    runs_this = runs[runs['resort'] == resort]
    points_this = points[points['resort'] == resort]
    for _, run in runs_this.iterrows():
        pts = points_this[points_this['osm_id'] == run['osm_id']].sort_values('distance_along_run_m')
        # Ensure downhill
        if len(pts) > 1 and pts.iloc[0]['elevation_m'] < pts.iloc[-1]['elevation_m']:
            pts = pts.iloc[::-1]
        y_smoothed = smooth_elevation(pts['elevation_m'].values)
        color = difficulty_colors.get(run['difficulty'], "#444444")
        ax.plot(pts['distance_along_run_m'], y_smoothed,
                color=color, alpha=0.8, linewidth=2)
    # Label the longest run at its end
    if not runs_this.empty:
        longest = runs_this.loc[runs_this['run_length_m'].idxmax()]
        pts_long = points_this[points_this['osm_id'] == longest['osm_id']].sort_values('distance_along_run_m')
        ax.text(pts_long['distance_along_run_m'].iloc[-1], pts_long['elevation_m'].iloc[-1],
                longest['run_name'], fontsize=11, fontweight='bold', color='black', ha='right', va='bottom')
    ax.set_title(resort, fontsize=20, fontweight='bold')
    ax.set_xlabel("Run Distance Along Slope (meters)", fontsize=14)
    ax.set_ylabel("Elevation (m)", fontsize=14)
    ax.grid(True, linestyle='--', alpha=0.4)
    ax.set_xlim(0, max_distance)
    ax.set_ylim(min_elev, max_elev)
    # y-axis ticks and labels now appear on every subplot

    # --- Avg Gradient Labels (top right, max 5 per panel) ---
    labels_this = gradient_stats[(gradient_stats['resort'] == resort)]
    if not labels_this.empty:
        labels_this = labels_this.sort_values('mean_gradient', ascending=False)
        ymin, ymax = ax.get_ylim()
        xmin, xmax = ax.get_xlim()
        y = ymax - 0.036 * (ymax - ymin)
        x = xmax - 0.01 * (xmax - xmin)
        lineheight = 0.066 * (ymax - ymin)
        for i, row in enumerate(labels_this.itertuples()):
            if i >= 5:
                break  # Show max 5 for clarity
            color = difficulty_colors.get(row.difficulty, "#444444")
            ax.text(
                x, y - i * lineheight,
                f"{row.difficulty} ({row.run_count}): {row.mean_gradient:.1f}%",
                fontsize=13, color=color,
                ha='right', va='top', fontweight='bold', alpha=0.98, zorder=11,
                bbox=dict(facecolor='white', alpha=0.73, edgecolor='none', boxstyle='round,pad=0.16')
            )

# Blank the last two axes (bottom right) for legend
for i in range(len(RESORTS), nrows * ncols):
    axes[i].axis('off')

plt.suptitle(
    'Ski Run Elevation Profiles by Resort\nAll runs (filtered, smoothed, colored by difficulty)\nAvg gradient per difficulty (top right)',
    fontsize=25, fontweight='bold', y=0.98)

plt.tight_layout(rect=(0, 0.03, 1, 0.96))
plt.subplots_adjust(hspace=0.36, wspace=0.18)
plt.savefig("charts/ski_run_elevations_matplotlib.png", dpi=200, bbox_inches='tight')
plt.show()
print("✅ Saved chart: ski_run_elevations_matplotlib.png")
