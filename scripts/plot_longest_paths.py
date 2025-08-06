from matplotlib.patches import Patch
import os
import duckdb
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
from project_path import get_project_paths, set_dlt_env_vars

# --- ENV, DuckDB connection ---
paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")
con = duckdb.connect(database_string)

# --- NZ Resorts ---
NZ_RESORTS = [
    'Temple Basin Ski Area', 'Mount Cheeseman Ski Area', 'Mount Dobson Ski Field',
    'Roundhill Ski Field', 'Mount Hutt Ski Area', 'Broken River Ski Area',
    'Porters Ski Area', 'Mount Olympus Ski Area',
    'The Remarkables Ski Area', 'Whakapapa Ski Area', 'Cardrona Alpine Resort',
    'Manganui Ski Area', 'Coronet Peak Ski Area', 
    'Treble Cone Ski Area', 'Tūroa Ski Area', 'Craigieburn Valley Ski Area',
    'Fox Peak Ski Area'
]

# --- Query for longest path data (NZ only) - UPDATED for new schema ---
path_query = """
WITH top_paths AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY resort ORDER BY total_path_length_m DESC) as path_rank
    FROM camonairflow.public_analysis.longest_possible_paths
    WHERE total_path_length_m > 0 
        AND run_count > 0
        AND resort NOT IN ('ERROR', 'NO_DATA', 'NO_NETWORKX')
        AND resort IN {}
)
SELECT
    country_code,
    resort,
    path_id,
    starting_lift_name,
    starting_lift_osm_id,
    ending_point_type,
    ending_point_name,
    ending_point_osm_id,
    run_path_array,
    run_path_names,
    run_count,
    total_path_length_m,
    total_vertical_drop_m,
    avg_gradient,
    total_turniness_score,
    difficulty_mix,
    hardest_difficulty
FROM top_paths
WHERE path_rank <= GREATEST(1, CAST(0.1 * (SELECT COUNT(*) FROM top_paths WHERE resort = top_paths.resort) AS INTEGER))
ORDER BY total_path_length_m DESC
""".format(tuple(NZ_RESORTS))

# --- Load data with error handling ---
try:
    df_paths = con.execute(path_query).df()
except Exception as e:
    print(f"Error loading path data: {e}")
    df_paths = pd.DataFrame()

if df_paths.empty:
    print("No NZ longest path data available for plotting")
    print("Make sure to run: dbt run --select +longest_possible_paths")
    exit()

print(f"Loaded {len(df_paths)} top NZ longest paths")

# Process path data
df_paths['path_length_km'] = df_paths['total_path_length_m'] / 1000
df_paths['vertical_drop_m'] = df_paths['total_vertical_drop_m']

# Create efficiency rating based on gradient and turniness
df_paths['efficiency_rating'] = df_paths.apply(lambda row: 
    'excellent' if row['avg_gradient'] > 15 and row['total_turniness_score'] < 50 
    else 'very_good' if row['avg_gradient'] > 10 and row['total_turniness_score'] < 100
    else 'good' if row['avg_gradient'] > 5 
    else 'fair_poor', axis=1)

# Create CONSISTENT color map for resorts
all_resorts = sorted(list(df_paths['resort'].unique()))
colormap = matplotlib.colormaps['tab20']
resort_color_map = {resort: colormap(idx % 20) for idx, resort in enumerate(all_resorts)}

# Create single plot
fig, ax = plt.subplots(1, 1, figsize=(16, 12))

# --- Plot: Path Length vs Vertical Drop ---
for _, row in df_paths.iterrows():
    resort = row["resort"]
    color = resort_color_map.get(resort, 'gray')

    # Color by efficiency rating (edge colors)
    efficiency = row.get('efficiency_rating', 'fair_poor')
    if efficiency == 'excellent':
        edge_color = 'green'
        edge_width = 2
    elif efficiency == 'very_good':
        edge_color = 'blue'
        edge_width = 1.5
    elif efficiency == 'good':
        edge_color = 'orange'
        edge_width = 1
    else:
        edge_color = 'red'
        edge_width = 0.5

    # Size by turniness score (lower turniness = larger points, better skiing)
    turniness = row.get('total_turniness_score', 100)
    point_size = min(200, max(30, 200 - turniness))

    ax.scatter(
        row["path_length_km"], row["vertical_drop_m"],
        alpha=0.7, s=point_size, color=color, edgecolors=edge_color, linewidth=edge_width
    )

# Label top paths (more labels since we have fewer points)
top_paths_to_label = df_paths.nlargest(15, 'total_path_length_m')
for _, row in top_paths_to_label.iterrows():
    resort = row["resort"]
    color = resort_color_map.get(resort, 'gray')

    # Create detailed label
    ending_type = row.get('ending_point_type', 'unknown')
    ending_name = row.get('ending_point_name', 'Unknown')

    if ending_type == 'lift':
        ending_info = f"→{ending_name[:12]}"
    else:
        ending_info = f"→{ending_type}"

    path_label = f"{resort[:15]}\n{row['starting_lift_name'][:15]}\n{ending_info}\n{row['run_count']}R, {row['path_length_km']:.1f}km"

    ax.annotate(
        path_label,
        (row["path_length_km"], row["vertical_drop_m"]),
        xytext=(5, 5), textcoords='offset points',
        fontsize=8, color=color, weight='bold', alpha=0.9,
        bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7, edgecolor=color)
    )

# Add anomaly labels
# Highest vertical drop
max_vertical_idx = df_paths['vertical_drop_m'].idxmax()
max_vertical_row = df_paths.loc[max_vertical_idx]
ax.annotate(
    f"HIGHEST DROP\n{max_vertical_row['resort'][:15]}\n{max_vertical_row['vertical_drop_m']:.0f}m", 
    (float(max_vertical_row["path_length_km"]), float(max_vertical_row["vertical_drop_m"])),
    xytext=(10, -40), textcoords='offset points',
    fontsize=10, color='red', weight='bold', alpha=0.9,
    bbox=dict(boxstyle='round,pad=0.3', facecolor='pink', alpha=0.8),
    arrowprops=dict(arrowstyle='->', color='red', lw=2)
)

# Longest path
max_length_idx = df_paths['path_length_km'].idxmax()
max_length_row = df_paths.loc[max_length_idx]
ax.annotate(
    f"LONGEST PATH\n{max_length_row['resort'][:15]}\n{max_length_row['path_length_km']:.1f}km\n{max_length_row['run_count']} runs", 
    (float(max_length_row["path_length_km"]), float(max_length_row["vertical_drop_m"])),
    xytext=(-50, 20), textcoords='offset points',
    fontsize=10, color='navy', weight='bold', alpha=0.9,
    bbox=dict(boxstyle='round,pad=0.3', facecolor='lightcyan', alpha=0.8),
    arrowprops=dict(arrowstyle='->', color='navy', lw=2)
)

# Most runs in a path
max_runs_idx = df_paths['run_count'].idxmax()
max_runs_row = df_paths.loc[max_runs_idx]
ax.annotate(
    f"MOST RUNS\n{max_runs_row['resort'][:15]}\n{max_runs_row['run_count']} runs\n{max_runs_row['path_length_km']:.1f}km", 
    (float(max_runs_row["path_length_km"]), float(max_runs_row["vertical_drop_m"])),
    xytext=(20, -20), textcoords='offset points',
    fontsize=10, color='purple', weight='bold', alpha=0.9,
    bbox=dict(boxstyle='round,pad=0.3', facecolor='plum', alpha=0.8),
    arrowprops=dict(arrowstyle='->', color='purple', lw=2)
)

ax.set_xlabel("Path Length (km)", fontsize=16)
ax.set_ylabel("Vertical Drop (m)", fontsize=16)
ax.set_title("NZ Longest Possible Ski Paths (Top 10% per Resort)", fontsize=18)
ax.grid(True, alpha=0.3)

# Add legend for efficiency edge colors - MOVE TO UPPER LEFT
legend_elements = [
    Patch(facecolor='white', edgecolor='green', linewidth=2, label='Excellent Efficiency'),
    Patch(facecolor='white', edgecolor='blue', linewidth=1.5, label='Very Good Efficiency'), 
    Patch(facecolor='white', edgecolor='orange', linewidth=1, label='Good Efficiency'),
    Patch(facecolor='white', edgecolor='red', linewidth=0.5, label='Fair/Poor Efficiency')
]
ax.legend(handles=legend_elements, title='Path Efficiency Rating', loc='upper left', fontsize=12)

# Add info text box - MOVE TO LOWER LEFT
textstr = '''Point size = total experience time
Edge color = efficiency rating
Top 10% longest paths per resort
From any lift top to any endpoint'''
props = dict(boxstyle='round', facecolor='lightyellow', alpha=0.8)
ax.text(0.02, 0.02, textstr, transform=ax.transAxes, fontsize=11,
        verticalalignment='bottom', bbox=props)

plt.tight_layout()
plt.savefig("charts/nz_longest_possible_paths.png", dpi=250, bbox_inches='tight')
plt.show()

# Enhanced path rankings
print("\n=== TOP 20 LONGEST NZ SKI PATHS (Any lift top to any endpoint) ===")
df_path_longest = df_paths.nlargest(20, 'path_length_km')
for i, (_, row) in enumerate(df_path_longest.iterrows(), 1):
    ending_info = f"{row.get('ending_point_type', 'unknown')}"
    if row.get('ending_point_type') == 'lift':
        ending_info = f"lift: {row.get('ending_point_name', 'Unknown')[:15]}"

    print(f"{i:2d}. {row['resort'][:20]:20} | Start: {row['starting_lift_name'][:20]:20} | "
          f"End: {ending_info[:25]:25} | Len: {row['path_length_km']:.1f}km | "
          f"Runs: {row['run_count']:2.0f} | Drop: {row['vertical_drop_m']:.0f}m | "
          f"Grad: {row['avg_gradient']:.1f}% | Diff: {row.get('hardest_difficulty', 'unknown')}")

print("\nNZ Longest Path Stats:")
print(f"Average path length: {df_paths['path_length_km'].mean():.1f}km")
print(f"Average runs per path: {df_paths['run_count'].mean():.1f}")
print(f"Average vertical drop: {df_paths['vertical_drop_m'].mean():.0f}m")
print(f"Average gradient: {df_paths['avg_gradient'].mean():.1f}%")
print(f"Longest single path: {df_paths['path_length_km'].max():.1f}km")