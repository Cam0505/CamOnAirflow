import os
import numpy as np
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
    total_lift_time_sec,
    total_run_time_intermediate_sec,
    lift_to_ski_ratio_intermediate,
    total_ski_area_time_intermediate_sec
FROM camonairflow.public_analysis.resort_ski_time_analysis
WHERE total_lift_time_sec > 0 
    AND total_run_time_intermediate_sec > 0
    AND resort NOT IN ('ERROR', 'NO_DATA')
    AND resort IN {}
ORDER BY total_runs DESC
""".format(tuple(NZ_RESORTS))

# --- Query for ski path data (NZ only) ---
path_query = """
SELECT
    resort,
    path_id,
    starting_lift_name,
    run_path_names as run_names,
    path_ski_time_intermediate_sec,
    starting_lift_time_sec,
    total_experience_time_intermediate_sec,
    ski_time_percentage_intermediate,
    lift_to_path_ratio_intermediate,
    run_count
FROM camonairflow.public_analysis.ski_path_efficiency_analysis
WHERE starting_lift_time_sec > 0 
    AND path_ski_time_intermediate_sec > 0
    AND resort NOT IN ('ERROR', 'NO_DATA')
    AND resort IN {}
ORDER BY total_experience_time_intermediate_sec DESC
LIMIT 200
""".format(tuple(NZ_RESORTS))

df_resorts = con.execute(resort_query).df()
df_paths = con.execute(path_query).df()

if df_resorts.empty:
    print("No NZ resort data available for plotting")
    exit()

print(f"Loaded {len(df_resorts)} NZ resorts and {len(df_paths)} NZ paths")

# Process resort data
df_resorts['total_ski_area_time_hours'] = df_resorts['total_ski_area_time_intermediate_sec'] / 3600
df_resorts['ski_to_lift_ratio'] = 1 / df_resorts['lift_to_ski_ratio_intermediate']

# Process path data if available
if not df_paths.empty:
    df_paths['total_experience_time_hours'] = df_paths['total_experience_time_intermediate_sec'] / 3600
    df_paths['ski_to_lift_ratio'] = 1 / df_paths['lift_to_path_ratio_intermediate']

# Create color map for resorts
colormap = matplotlib.colormaps['tab20']
resort_color_map = {resort: colormap(idx % 20) for idx, resort in enumerate(sorted(df_resorts['resort'].unique()))}

# Create plots
if df_paths.empty:
    fig, ax1 = plt.subplots(1, 1, figsize=(16, 10))
    ax2 = None
else:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(24, 10))

# --- PLOT 1: Resort Size vs Efficiency (using number of runs) ---
for _, row in df_resorts.iterrows():
    resort = row["resort"]
    color = resort_color_map[resort]

    # Size points based on total lifts (different from x-axis which is runs)
    point_size = min(200, max(30, row["total_lifts"] * 15))

    ax1.scatter(
        row["total_runs"], row["ski_to_lift_ratio"],
        alpha=0.7, s=point_size, color=color, edgecolors='black', linewidth=0.5
    )

    # Simple text placement without adjustment (to avoid memory issues)
    ax1.annotate(
        resort, 
        (row["total_runs"], row["ski_to_lift_ratio"]),
        xytext=(5, 5), textcoords='offset points',
        fontsize=8, color=color, weight='bold', alpha=0.8
    )

# Add efficiency reference lines
ax1.axhline(y=2, color='blue', linestyle='--', alpha=0.6, linewidth=2, label='2:1 Ratio (Good)')
ax1.axhline(y=1, color='green', linestyle='--', alpha=0.6, linewidth=2, label='1:1 Ratio (Break-even)')
ax1.axhline(y=0.5, color='orange', linestyle='--', alpha=0.6, linewidth=2, label='1:2 Ratio (Poor)')

ax1.set_xlabel("Number of Ski Runs", fontsize=14)
ax1.set_ylabel("Ski:Lift Efficiency Ratio (higher = better)", fontsize=14)
ax1.set_title("New Zealand Resort Size vs Skiing Efficiency", fontsize=16)
ax1.grid(True, alpha=0.3)
ax1.legend(fontsize=11, loc='upper right')

# Add text box with explanation
textstr1 = '''Point size = number of lifts
Higher on Y-axis = more efficient
(more ski time per lift time)
All NZ resorts labeled'''
props = dict(boxstyle='round', facecolor='lightblue', alpha=0.8)
ax1.text(0.02, 0.98, textstr1, transform=ax1.transAxes, fontsize=10,
         verticalalignment='top', bbox=props)

# --- PLOT 2: Ski Path Efficiency (label paths above 1.5:1) ---
if not df_paths.empty and ax2 is not None:
    # Sort by efficiency and only label those above 1.5:1
    label_mask = df_paths['ski_to_lift_ratio'] > 1.5
    paths_to_label = df_paths[label_mask]

    for _, row in df_paths.iterrows():
        resort = row["resort"]
        color = resort_color_map.get(resort, 'gray')
        point_size = min(100, max(15, row["run_count"] * 5))
        ax2.scatter(
            row["total_experience_time_hours"], row["ski_to_lift_ratio"],
            alpha=0.5, s=point_size, color=color, edgecolors='black', linewidth=0.2
        )

    # Label paths above 1.5:1 with run names instead of lift names
    for _, row in paths_to_label.iterrows():
        resort = row["resort"]
        color = resort_color_map.get(resort, 'gray')

        # Use run names instead of lift name
        run_names = row.get('run_names', 'Unknown Runs')
        if pd.isna(run_names) or not run_names:
            run_names = 'Unknown Runs'

        # Truncate long run names lists
        if len(run_names) > 30:
            run_names = run_names[:27] + "..."

        path_label = f"{resort[:12]}\n{run_names}"

        ax2.annotate(
            path_label,
            (row["total_experience_time_hours"], row["ski_to_lift_ratio"]),
            xytext=(3, 3), textcoords='offset points',
            fontsize=7, color=color, weight='bold', alpha=0.9
        )

    # Add efficiency reference lines
    ax2.axhline(y=2, color='blue', linestyle='--', alpha=0.6, linewidth=2, label='2:1 Ratio (Good)')
    ax2.axhline(y=1, color='green', linestyle='--', alpha=0.6, linewidth=2, label='1:1 Ratio (Break-even)')
    ax2.axhline(y=0.5, color='orange', linestyle='--', alpha=0.6, linewidth=2, label='1:2 Ratio (Poor)')

    ax2.set_xlabel("Total Path Experience Time (hours)", fontsize=14)
    ax2.set_ylabel("Ski:Lift Efficiency Ratio (higher = better)", fontsize=14)
    ax2.set_title("NZ Ski Path Efficiency", fontsize=16)
    ax2.grid(True, alpha=0.3)
    ax2.legend(fontsize=11, loc='upper right')

    # Add text box with explanation
    textstr2 = '''Point size = number of runs in path
Labels: paths above 1.5:1 ratio
Shows ski run names'''
    props = dict(boxstyle='round', facecolor='lightgreen', alpha=0.8)
    ax2.text(0.02, 0.98, textstr2, transform=ax2.transAxes, fontsize=10,
             verticalalignment='top', bbox=props)

plt.tight_layout()
plt.savefig("charts/nz_resort_and_path_efficiency.png", dpi=250, bbox_inches='tight')
plt.show()

# Print efficiency rankings
print("\n=== NZ RESORTS BY EFFICIENCY (Ski:Lift Ratio) ===")
df_efficient = df_resorts.sort_values('ski_to_lift_ratio', ascending=False)
for i, (_, row) in enumerate(df_efficient.iterrows(), 1):
    print(f"{i:2d}. {row['resort'][:35]:35} | Ratio: {row['ski_to_lift_ratio']:.2f}:1 | "
          f"Runs: {row['total_runs']:3.0f} | Lifts: {row['total_lifts']:2.0f}")

if not df_paths.empty:
    print("\n=== TOP 10 MOST EFFICIENT NZ SKI PATHS ===")
    df_path_efficient = df_paths.nlargest(10, 'ski_to_lift_ratio')
    for i, (_, row) in enumerate(df_path_efficient.iterrows(), 1):
        print(f"{i:2d}. {row['resort'][:20]:20} | {row['starting_lift_name'][:25]:25} | "
              f"Ratio: {row['ski_to_lift_ratio']:.2f}:1 | Runs: {row['run_count']:2.0f}")

print(f"\nAverage NZ resort efficiency: {df_resorts['ski_to_lift_ratio'].mean():.2f}:1")
if not df_paths.empty:
    print(f"Average NZ path efficiency: {df_paths['ski_to_lift_ratio'].mean():.2f}:1")