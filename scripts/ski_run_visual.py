import os
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import savgol_filter
import duckdb
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

# --- Separate resorts ---
NZ_RESORTS = [
    'Temple Basin Ski Area',
    'Mount Cheeseman Ski Area',
    'Mount Dobson Ski Field',
    'Roundhill Ski Field',
    'Mount Hutt Ski Area',
    'Broken River Ski Area',
    'Porters Ski Area',
    'Rainbow Ski Area',
    'Mount Olympus Ski Area',
    'The Remarkables Ski Area',
    'Whakapapa Ski Area',
    'Cardrona Alpine Resort',
    'Manganui Ski Area',
    'Coronet Peak Ski Area',
    'Mount Lyford Alpine Resort',
    'Treble Cone Ski Area',
    'Tūroa Ski Area',
    'Craigieburn Valley Ski Area',
    'Fox Peak Ski Area'
]

AU_RESORTS = [
    'Charlotte Pass', 'Falls Creek', 'Mount Baw Baw', 'Mount Buller',
    'Mount Hotham', 'Perisher', 'Thredbo Resort'
]


REGIONS = {
    "New Zealand": NZ_RESORTS,
    "Australia": AU_RESORTS
}

# --- Difficulty color palette ---
difficulty_colors = {
    "novice": "#4daf4a",
    "easy": "#377eb8",
    "intermediate": "#d90f0f",
    "advanced": "#0e0d0d",
    "freeride": "#00CED1",
    None: "#999999",
    "nan": "#999999"
}

# --- Smoothing function ---
def smooth_elevation(y, window=9, poly=2):
    n = len(y)
    if n < window:
        return y
    window = min(window if window % 2 == 1 else window + 1, n)
    if window < 3:
        return y
    return savgol_filter(y, window, poly)

# , "percent"
# --- Plotting loop ---
for region, resorts in REGIONS.items():
    for label_mode in ["degrees"]:
        # Load Data
        points = con.execute(f"""
            SELECT osm_id, resort, distance_along_run_m, elevation_m
            FROM camonairflow.public_base.base_filtered_ski_points
            WHERE resort in {tuple(resorts)}
        """).df()

        runs = con.execute(f"""
            SELECT osm_id, resort, run_name, country_code,
                CASE WHEN difficulty = 'extreme' THEN 'intermediate'
                     WHEN difficulty = 'expert' THEN 'advanced'
                     ELSE difficulty END AS difficulty,
                run_length_m
            FROM camonairflow.public_base.base_filtered_ski_runs
            WHERE resort in {tuple(resorts)}
            and run_length_m > 200
            and n_points > 4
        """).df()

        gradient_stats = con.execute(f"""
            SELECT resort,
                CASE WHEN difficulty = 'extreme' THEN 'intermediate'
                     WHEN difficulty = 'expert' THEN 'advanced'
                     ELSE difficulty END AS difficulty,
                run_count, mean_gradient_degrees, mean_steepest_degrees,
                mean_gradient_percent, mean_steepest_percent
            FROM camonairflow.public_base.base_ski_gradient_stats
            WHERE resort in {tuple(resorts)}
        """).df()

        # Update color map if any new difficulties appear
        for diff in runs['difficulty'].dropna().unique():
            if diff not in difficulty_colors:
                difficulty_colors[diff] = "#444444"

        max_distance = points['distance_along_run_m'].max() * 1.05
        min_elev = int(np.floor(points['elevation_m'].min() / 100.0) * 100)
        max_elev = int(np.ceil(points['elevation_m'].max() / 100.0) * 100)

        # --- Plot setup ---
        ncols = 4
        nrows = int(np.ceil(len(resorts) / ncols))
        fig, axes = plt.subplots(nrows, ncols, figsize=(20, 8 * nrows), sharey=True)
        axes = axes.flatten()

        for ax, resort in zip(axes, resorts):
            runs_this = runs[runs['resort'] == resort]
            points_this = points[points['resort'] == resort]
            for _, run in runs_this.iterrows():
                pts = points_this[points_this['osm_id'] == run['osm_id']].sort_values('distance_along_run_m')
                if len(pts) > 1 and pts.iloc[0]['elevation_m'] < pts.iloc[-1]['elevation_m']:
                    pts = pts.iloc[::-1]
                y_smoothed = smooth_elevation(pts['elevation_m'].values)
                color = difficulty_colors.get(run['difficulty'], "#444444")
                ax.plot(pts['distance_along_run_m'], y_smoothed, color=color, alpha=0.8, linewidth=2)

            if not runs_this.empty:
                longest = runs_this.loc[runs_this['run_length_m'].idxmax()]
                pts_long = points_this[points_this['osm_id'] == longest['osm_id']].sort_values('distance_along_run_m')
                ax.text(
                    pts_long['distance_along_run_m'].iloc[-1], 
                    pts_long['elevation_m'].iloc[-1],
                    longest['run_name'], fontsize=11, fontweight='bold', color='black',
                    ha='right', va='bottom'
                )
            ax.set_title(resort, fontsize=20, fontweight='bold')
            ax.set_xlabel("Run Distance Along Slope (meters)", fontsize=14)
            ax.set_ylabel("Elevation (m)", fontsize=14)
            ax.grid(True, linestyle='--', alpha=0.4)
            ax.set_xlim(0, max_distance)
            ax.set_ylim(min_elev, max_elev)

            # Avg gradient labels
            labels_this = gradient_stats[gradient_stats['resort'] == resort]
            if not labels_this.empty:
                labels_this = labels_this.sort_values(
                    'mean_gradient_degrees' if label_mode == "degrees" else 'mean_gradient_percent',
                    ascending=False
                )
                ymin, ymax = ax.get_ylim()
                xmin, xmax = ax.get_xlim()
                y = ymax - 0.036 * (ymax - ymin)
                x = xmax - 0.01 * (xmax - xmin)
                lineheight = 0.066 * (ymax - ymin)
                for i, row in enumerate(labels_this.itertuples()):
                    if i >= 5:
                        break
                    color = difficulty_colors.get(row.difficulty, "#444444")
                    if label_mode == "degrees":
                        label = (
                            f"{row.difficulty} ({row.run_count}): "
                            f"{row.mean_gradient_degrees:.1f}°, {row.mean_steepest_degrees:.1f}°"
                        )
                    else:
                        label = (
                            f"{row.difficulty} ({row.run_count}): "
                            f"{row.mean_gradient_percent:.0f}%, {row.mean_steepest_percent:.0f}%"
                        )
                    ax.text(
                        x, y - i * lineheight,
                        label,
                        fontsize=13, color=color,
                        ha='right', va='top', fontweight='bold', alpha=0.98, zorder=11,
                        bbox=dict(facecolor='white', alpha=0.73, edgecolor='none', boxstyle='round,pad=0.16')
                    )

        for i in range(len(resorts), nrows * ncols):
            axes[i].axis('off')

        plt.suptitle(
            f'{region} Ski Run Elevation Profiles by Resort\n'
            f'All runs (filtered, smoothed, colored by difficulty)\n'
            f'Labels: difficulty (number of runs): mean, max gradient '
            f'({"degrees" if label_mode == "degrees" else "percent"})\n'
            f'Example: advanced (8): '
            f'{"24.1°, 32.5°" if label_mode == "degrees" else "42%, 56%"} means mean and max steepness for advanced runs',
            fontsize=22, fontweight='bold', y=0.98
        )
        plt.tight_layout(rect=(0, 0.03, 1, 0.96))
        plt.subplots_adjust(hspace=0.36, wspace=0.18)
        out_path = f"charts/ski_run_elevations_matplotlib_{region.lower().replace(' ', '_')}_{label_mode}.png"
        plt.savefig(out_path, dpi=300, bbox_inches='tight')
        plt.show()

con.close()