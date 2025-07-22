import duckdb
import os
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
import pandas as pd
from plotnine import (
    ggplot, aes, geom_line, facet_wrap, labs, theme_light, theme,
    element_text, element_rect, element_line, scale_x_continuous, scale_y_continuous,
    scale_color_manual
)
import matplotlib.colors as mcolors
import numpy as np

# --- ENV, DuckDB connection ---
paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")
con = duckdb.connect(database_string)

# --- Load Data ---
points = con.execute("""
    SELECT
        osm_id,
        resort,
        country_code,
        run_name,
        point_index,
        distance_along_run_m,
        elevation_m
    FROM camonairflow.main.ski_run_points
""").df()

runs = con.execute("""
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
""").df()

points = points.merge(runs[['osm_id', 'difficulty', 'run_length_m']], on='osm_id', how='left')
points['x_norm'] = points['distance_along_run_m'] / points['run_length_m']

# Color palette
difficulty_palette = {
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
all_diffs = sorted(points['difficulty'].dropna().unique())
for diff in all_diffs:
    if diff not in difficulty_palette:
        difficulty_palette[diff] = mcolors.to_hex(np.random.rand(3,))

points['difficulty_color'] = points['difficulty'].map(lambda x: difficulty_palette.get(x, "#444444"))

# --- Plot ---
p = (
    ggplot(points, aes(x='x_norm', y='elevation_m', group='osm_id', color='difficulty')) +
    geom_line(size=2.2, alpha=0.93) +  # Thicker lines for clarity
    facet_wrap('~resort', scales='free', ncol=2) +  # Only two columns, bigger plots
    scale_color_manual(values=difficulty_palette) +
    labs(
        title='Ski Run Elevation Profiles by Resort',
        subtitle='Line color = difficulty | Each line = one run | X = distance (normalized per run)',
        x='Normalized Run Distance (0 = start, 1 = end)',
        y='Elevation (m)'
    ) +
    scale_x_continuous(limits=(0, 1), expand=(0.01, 0)) +
    scale_y_continuous(expand=(0.03, 0.03)) +
    theme_light(base_size=22) +  # Even larger base font
    theme(
        legend_position='right',
        legend_title=element_text(size=18, weight="bold"),
        legend_text=element_text(size=15),
        axis_text_x=element_text(size=15),
        axis_text_y=element_text(size=15),
        axis_title_x=element_text(size=20, weight='bold'),
        axis_title_y=element_text(size=20, weight='bold'),
        plot_title=element_text(weight='bold', size=28),
        plot_subtitle=element_text(size=18),
        panel_spacing=0.25,
        strip_text_x=element_text(color="black", weight="bold", size=22),
        strip_background=element_rect(fill="#e0e0e0", color="#888888"),
        panel_grid_major_x=element_line(color="#343434", size=0.5, linetype='dashed')
    )
)

# --- Save ---
out_path = "/workspaces/CamOnAirFlow/charts/ski_run_elevations_larger.png"
p.save(out_path, width=24, height=18, dpi=180, limitsize=False)
print("✅ Saved chart: ski_run_elevations_larger.png")
