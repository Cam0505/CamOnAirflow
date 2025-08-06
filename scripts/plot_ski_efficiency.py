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

# --- Query for resort data (NZ only) ---
resort_query = """
SELECT
    resort,
    total_lifts,
    total_runs,
    total_connections,
    total_lift_time_sec,
    total_run_time_intermediate_sec,
    lift_to_ski_ratio_intermediate,
    total_ski_area_time_intermediate_sec,
    run_to_run_connections,
    avg_path_length_m,
    total_network_length_m
FROM camonairflow.public_analysis.resort_ski_time_analysis
WHERE total_lift_time_sec > 0 
    AND total_run_time_intermediate_sec > 0
    AND resort NOT IN ('ERROR', 'NO_DATA', 'NO_NETWORKX')
    AND resort IN {}
ORDER BY total_runs DESC
""".format(tuple(NZ_RESORTS))

# --- Query for ski path data (NZ only) - NOW LIFT TOP TO SAME LIFT BOTTOM ---
path_query = """
SELECT
    resort,
    path_id,
    starting_lift_name,
    run_path_names,
    run_count,
    total_path_length_m,
    total_vertical_drop_m,
    path_ski_time_intermediate_sec,
    starting_lift_time_sec,
    total_experience_time_intermediate_sec,
    ski_time_percentage_intermediate,
    lift_to_path_ratio_intermediate,
    efficiency_rating_intermediate,
    difficulty_mix,
    hardest_difficulty,
    ending_point_type,
    ending_point_name,
    starting_lift_osm_id,
    ending_point_osm_id
FROM camonairflow.public_analysis.ski_path_efficiency_analysis
WHERE starting_lift_time_sec > 0 
    AND path_ski_time_intermediate_sec > 0
    AND resort NOT IN ('ERROR', 'NO_DATA', 'NO_NETWORKX')
    AND resort IN {}
    AND efficiency_rating_intermediate NOT IN ('unknown')
    AND ending_point_type = 'lift'
    AND starting_lift_osm_id = ending_point_osm_id  -- SAME LIFT!
ORDER BY total_experience_time_intermediate_sec DESC
LIMIT 200
""".format(tuple(NZ_RESORTS))

# --- Load data ---
try:
    df_resorts = con.execute(resort_query).df()
    df_paths = con.execute(path_query).df()
except Exception as e:
    print(f"Error loading data: {e}")
    exit()

if df_resorts.empty:
    print("No NZ resort data available")
    exit()

print(f"Loaded {len(df_resorts)} NZ resorts and {len(df_paths)} NZ efficiency paths")

# Process data
df_resorts['ski_to_lift_ratio'] = 1 / df_resorts['lift_to_ski_ratio_intermediate']

if not df_paths.empty:
    df_paths['total_experience_time_hours'] = df_paths['total_experience_time_intermediate_sec'] / 3600
    df_paths['ski_to_lift_ratio'] = 1 / df_paths['lift_to_path_ratio_intermediate']
    df_paths['path_length_km'] = df_paths['total_path_length_m'] / 1000

# Create consistent color map
all_resorts = set(df_resorts['resort'].unique())
if not df_paths.empty:
    all_resorts.update(df_paths['resort'].unique())
all_resorts = sorted(list(all_resorts))

colormap = matplotlib.colormaps['tab20']
resort_color_map = {resort: colormap(idx % 20) for idx, resort in enumerate(all_resorts)}

# Create plots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(24, 10))

# --- PLOT 1: Resort Size vs Efficiency ---
for _, row in df_resorts.iterrows():
    resort = row["resort"]
    color = resort_color_map[resort]
    total_connectivity = row["total_connections"] + row["run_to_run_connections"]
    point_size = min(300, max(30, total_connectivity * 3))

    ax1.scatter(
        row["total_runs"], row["ski_to_lift_ratio"],
        alpha=0.7, s=point_size, color=color, edgecolors='black', linewidth=0.5
    )

    connectivity_info = f"({row['run_to_run_connections']} R2R)"
    ax1.annotate(
        f"{resort}\n{connectivity_info}", 
        (row["total_runs"], row["ski_to_lift_ratio"]),
        xytext=(5, 5), textcoords='offset points',
        fontsize=7, color=color, weight='bold', alpha=0.8
    )

# Add anomaly labels
max_efficiency_idx = df_resorts['ski_to_lift_ratio'].idxmax()
max_efficiency_row = df_resorts.loc[max_efficiency_idx]
ax1.annotate(
    f"HIGHEST EFFICIENCY\n{max_efficiency_row['resort'][:20]}\n{max_efficiency_row['ski_to_lift_ratio']:.2f}:1", 
    (max_efficiency_row["total_runs"], max_efficiency_row["ski_to_lift_ratio"]),
    xytext=(20, 20), textcoords='offset points',
    fontsize=9, color='darkgreen', weight='bold', alpha=0.9,
    bbox=dict(boxstyle='round,pad=0.3', facecolor='lightgreen', alpha=0.7),
    arrowprops=dict(arrowstyle='->', color='darkgreen', lw=2)
)

ax1.axhline(y=2, color='blue', linestyle='--', alpha=0.6, linewidth=2, label='2:1 Ratio (Good)')
ax1.axhline(y=1, color='green', linestyle='--', alpha=0.6, linewidth=2, label='1:1 Ratio (Break-even)')
ax1.axhline(y=0.5, color='orange', linestyle='--', alpha=0.6, linewidth=2, label='1:2 Ratio (Poor)')

ax1.set_xlabel("Number of Ski Runs", fontsize=14)
ax1.set_ylabel("Ski:Lift Efficiency Ratio (higher = better)", fontsize=14)
ax1.set_title("NZ Resort Efficiency (Same Lift Cycles)", fontsize=16)
ax1.grid(True, alpha=0.3)
ax1.legend(fontsize=11, loc='upper right')

# --- PLOT 2: Ski Path Efficiency (same lift cycles) ---
if not df_paths.empty:
    for _, row in df_paths.iterrows():
        resort = row["resort"]
        color = resort_color_map.get(resort, 'gray')
        point_size = min(150, max(15, row["path_length_km"] * 20))

        ax2.scatter(
            row["total_experience_time_hours"], row["ski_to_lift_ratio"],
            alpha=0.6, s=point_size, color=color, edgecolors='black', linewidth=0.3
        )

    # Label top efficient same-lift cycles
    top_efficient_paths = df_paths.nlargest(5, 'ski_to_lift_ratio')
    for _, row in top_efficient_paths.iterrows():
        resort = row["resort"]
        color = resort_color_map.get(resort, 'gray')
        
        path_label = f"{resort[:12]}\n{row['starting_lift_name'][:15]}\n{row['run_count']}R cycle"

        ax2.annotate(
            path_label,
            (row["total_experience_time_hours"], row["ski_to_lift_ratio"]),
            xytext=(3, 3), textcoords='offset points',
            fontsize=7, color=color, weight='bold', alpha=0.9
        )

    ax2.axhline(y=2, color='blue', linestyle='--', alpha=0.6, linewidth=2, label='2:1 Ratio (Good)')
    ax2.axhline(y=1, color='green', linestyle='--', alpha=0.6, linewidth=2, label='1:1 Ratio (Break-even)')
    ax2.axhline(y=0.5, color='orange', linestyle='--', alpha=0.6, linewidth=2, label='1:2 Ratio (Poor)')

    ax2.set_xlabel("Total Experience Time (hours)", fontsize=14)
    ax2.set_ylabel("Ski:Lift Efficiency Ratio (higher = better)", fontsize=14)
    ax2.set_title("NZ Same-Lift Cycle Efficiency", fontsize=16)
    ax2.grid(True, alpha=0.3)
    ax2.legend(fontsize=11, loc='upper right')

plt.tight_layout()
plt.savefig("charts/nz_ski_efficiency_same_lift.png", dpi=250, bbox_inches='tight')
plt.show()

print("\n=== NZ SAME-LIFT EFFICIENCY ANALYSIS ===")
if not df_paths.empty:
    print(f"Found {len(df_paths)} same-lift cycle paths")
    print(f"Average same-lift cycle efficiency: {df_paths['ski_to_lift_ratio'].mean():.2f}:1")
    print(f"Best same-lift cycle efficiency: {df_paths['ski_to_lift_ratio'].max():.2f}:1")
else:
    print("No same-lift cycle paths found - check network connectivity")