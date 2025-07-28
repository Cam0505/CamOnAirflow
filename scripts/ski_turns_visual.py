import duckdb
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import contextily as ctx
# from shapely.geometry import Point
from scipy.signal import savgol_filter
from dotenv import load_dotenv
import os
from project_path import get_project_paths, set_dlt_env_vars

# --- ENV setup ---
paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")
con = duckdb.connect(database_string)

# --- Define resorts and color palette ---
RESORTS = [
    'Remarkables', 'Mount Hutt', 'Cardrona', 'Treble Cone', 'Coronet Peak',
    'Temple Basin', 'Mount Cheeseman', 'Mount Dobson', 'Mount Olympus', 
    'Porters', 'RoundHill', 'Turoa'
]
difficulty_colors = {
    "beginner": "green",
    "intermediate": "blue",
    "advanced": "red"
}

# --- Load Points ---
points_df = con.execute(f"""
    SELECT osm_id, resort, run_name, lat, lon, elevation_m
    FROM camonairflow.main.ski_run_points
    WHERE lat IS NOT NULL AND lon IS NOT NULL
      AND resort IN {tuple(RESORTS)}
""").df()

# --- Load Run Metadata with normalized difficulty ---
run_meta_df = con.execute(f"""
    SELECT
        osm_id,
        resort,
        run_name,
        CASE
            WHEN difficulty = 'extreme' THEN 'intermediate'
            WHEN difficulty = 'expert' THEN 'advanced'
            ELSE difficulty
        END AS difficulty
    FROM camonairflow.public_base.base_filtered_ski_runs
    WHERE resort IN {tuple(RESORTS)}
""").df()

# --- Merge and convert to GeoDataFrame ---
df = pd.merge(points_df, run_meta_df, on=["osm_id", "resort", "run_name"], how="left")
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["lon"], df["lat"]), crs="EPSG:4326").to_crs("EPSG:2193")
gdf["x"] = gdf.geometry.x
gdf["y"] = gdf.geometry.y

# --- Plot setup ---
resorts = sorted(gdf["resort"].dropna().unique())
n_cols = 3
n_rows = -(-len(resorts) // n_cols)
fig, axs = plt.subplots(n_rows, n_cols, figsize=(6 * n_cols, 6 * n_rows), constrained_layout=True)
axs = axs.flatten()

for i, resort in enumerate(resorts):
    ax = axs[i]
    subset = gdf[gdf["resort"] == resort]

    for (osm_id, difficulty), group in subset.groupby(["osm_id", "difficulty"]):
        group = group.sort_values("elevation_m", ascending=False)

        # Smooth long runs
        if len(group) >= 9:
            x_smooth = savgol_filter(group["x"], window_length=9, polyorder=3)
            y_smooth = savgol_filter(group["y"], window_length=9, polyorder=3)
        else:
            x_smooth = group["x"]
            y_smooth = group["y"]

        color = difficulty_colors.get(difficulty, "gray")
        ax.plot(x_smooth, y_smooth, linewidth=2.2, color=color, alpha=0.9)

    ax.set_title(resort, fontsize=14)
    ax.set_aspect("equal")
    ax.set_xlabel("Easting (m)")
    ax.set_ylabel("Northing (m)")

    try:
        ctx.add_basemap(ax, crs=gdf.crs.to_string(), source=ctx.providers.OpenTopoMap, zoom=12)
    except Exception as e:
        print(f"⚠️ Basemap failed for {resort}: {e}")

# Hide unused axes
for j in range(i + 1, len(axs)):
    axs[j].axis("off")

fig.suptitle("Ski Runs by Resort (Smoothed Topo View by Difficulty)", fontsize=20)
output_path = os.path.join(paths["CHARTS_DIR"], "ski_run_colored_paths_smooth.png")
fig.savefig(output_path, dpi=300)
plt.show()
