import os
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
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

# --- Create output directory if it doesn't exist ---
charts_dir = os.path.join(paths.get("PROJECT_ROOT", "."), "charts")
os.makedirs(charts_dir, exist_ok=True)

print("Loading ski paths data from mart...")

# --- Load ski path data ---
ski_paths = con.execute("""
    SELECT
        path_id,
        resort,
        starting_lift,
        starting_lift_id,
        total_distance_m,
        total_vertical_m,
        avg_gradient_deg,
        run_path,
        distance_profile_m,
        elevation_profile_m,
        ending_type,
        ending_name
    FROM camonairflow.public_common.mart_nz_ski_paths
    WHERE distance_profile_m IS NOT NULL 
      AND elevation_profile_m IS NOT NULL
    ORDER BY resort, starting_lift
""").df()

print(f"Loaded {len(ski_paths)} ski paths")

# --- Helper function to parse array strings if needed ---
def parse_array(array_input):
    """Parse array input - handle both strings and actual arrays"""
    # If it's already a list or numpy array, return it
    if isinstance(array_input, (list, np.ndarray)):
        return array_input
    
    # If it's a string, parse it
    if isinstance(array_input, str):
        if not array_input or array_input == '[]':
            return []
        # Remove brackets and split by comma
        return [float(x.strip()) for x in array_input.strip('[]').split(',') if x.strip()]
    
    # Default case: empty list
    return []

# --- Convert array columns if needed ---
print("Processing elevation and distance profiles...")
for idx, path in ski_paths.iterrows():
    ski_paths.at[idx, 'distance_profile_m'] = parse_array(path['distance_profile_m'])
    ski_paths.at[idx, 'elevation_profile_m'] = parse_array(path['elevation_profile_m'])

# --- Filter out paths with invalid profiles ---
valid_distance = ski_paths['distance_profile_m'].apply(lambda x: isinstance(x, (list, np.ndarray)) and len(x) > 1)
valid_elevation = ski_paths['elevation_profile_m'].apply(lambda x: isinstance(x, (list, np.ndarray)) and len(x) > 1)
ski_paths = ski_paths[valid_distance & valid_elevation]

print(f"After filtering: {len(ski_paths)} valid ski paths")

# --- Find global min/max for consistent axes ---
min_elevation = float('inf')
max_elevation = float('-inf')
max_distance = 0

for _, path in ski_paths.iterrows():
    if len(path['elevation_profile_m']) > 0:
        min_elevation = min(min_elevation, min(path['elevation_profile_m']))
        max_elevation = max(max_elevation, max(path['elevation_profile_m']))
    
    if len(path['distance_profile_m']) > 0:
        max_distance = max(max_distance, max(path['distance_profile_m']))

print(f"Global elevation range: {min_elevation:.1f}m - {max_elevation:.1f}m")
print(f"Max distance: {max_distance:.1f}m")

# --- Add padding to ranges ---
elevation_range = max_elevation - min_elevation
min_elevation -= elevation_range * 0.05
max_elevation += elevation_range * 0.05
max_distance *= 1.05

# --- Get unique resorts ---
resorts = sorted(ski_paths['resort'].unique())
print(f"Found {len(resorts)} resorts: {', '.join(resorts)}")

# --- Set up figure with one facet per resort ---
ncols = min(2, len(resorts))
nrows = int(np.ceil(len(resorts) / ncols))
fig, axes = plt.subplots(nrows, ncols, figsize=(16, 8 * nrows), sharex=True, sharey=True)

# --- Handle single resort case ---
if len(resorts) == 1:
    axes = np.array([axes])

# --- Flatten axes for easy iteration ---
axes = axes.flatten()

# --- Create a consistent color map for all lifts across resorts ---
all_lifts = sorted(ski_paths['starting_lift'].unique())
cmap = plt.get_cmap("tab20") if len(all_lifts) > 10 else plt.get_cmap("tab10")
lift_colors = {lift: cmap(i % cmap.N) for i, lift in enumerate(all_lifts)}

# --- Track all lift legends for combined legend ---
legend_handles = []

# --- Plot each resort in its own facet ---
for i, resort in enumerate(resorts):
    if i >= len(axes):
        break  # Safety check
        
    ax = axes[i]
    resort_data = ski_paths[ski_paths['resort'] == resort]
    
    # Track which lifts are used in this resort
    resort_lifts = set()
    
    # Plot each ski path for this resort
    for _, path in resort_data.iterrows():
        if len(path['distance_profile_m']) <= 1 or len(path['elevation_profile_m']) <= 1:
            continue
            
        lift = path['starting_lift']
        color = lift_colors[lift]
        
        # Plot with semi-transparency to see overlapping paths
        ax.plot(path['distance_profile_m'], path['elevation_profile_m'], 
               color=color, alpha=0.4, linewidth=1.5)
        
        resort_lifts.add(lift)
    
    # Set up the plot
    ax.set_title(resort, fontsize=16, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.4)
    ax.set_xlim(0, max_distance)
    ax.set_ylim(min_elevation, max_elevation)
    
    # Add axes labels (only bottom row and left column)
    if i >= len(axes) - ncols:  # Bottom row
        ax.set_xlabel("Distance (m)", fontsize=12)
    if i % ncols == 0:  # Left column
        ax.set_ylabel("Elevation (m)", fontsize=12)
    
    # Create legend handles for this resort
    resort_handles = [Line2D([0], [0], color=lift_colors[lift], lw=2, label=lift) 
                     for lift in sorted(resort_lifts)]
    
    # Add per-resort legend
    if resort_handles:
        ax.legend(handles=resort_handles, loc='upper right', 
                 fontsize=10, framealpha=0.7)

# --- Hide empty subplots ---
for j in range(len(resorts), len(axes)):
    axes[j].axis('off')

# --- Add overall title ---
plt.suptitle("Ski Path Elevation Profiles by Resort", fontsize=20, fontweight='bold', y=0.98)

# --- Layout adjustments ---
plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.subplots_adjust(hspace=0.3, wspace=0.2)

# --- Save figure ---
out_path = os.path.join(charts_dir, "ski_path_elevation_profiles.png")
plt.savefig(out_path, dpi=300, bbox_inches='tight')
print(f"Saved visualization to: {out_path}")

# --- Also create individual resort files for better detail ---
print("Creating individual resort visualizations...")

for resort in resorts:
    resort_data = ski_paths[ski_paths['resort'] == resort]
    
    plt.figure(figsize=(12, 8))
    
    # Track lifts in this resort
    resort_lifts = set()
    
    # Plot each path
    for _, path in resort_data.iterrows():
        if len(path['distance_profile_m']) <= 1 or len(path['elevation_profile_m']) <= 1:
            continue
            
        lift = path['starting_lift']
        color = lift_colors[lift]
        
        plt.plot(path['distance_profile_m'], path['elevation_profile_m'], 
                color=color, alpha=0.4, linewidth=1.5)
        
        resort_lifts.add(lift)
    
    # Create legend handles
    resort_handles = [Line2D([0], [0], color=lift_colors[lift], lw=2, label=lift) 
                     for lift in sorted(resort_lifts)]
    
    # Set up the plot
    plt.title(f"Ski Path Elevation Profiles: {resort}", fontsize=16, fontweight='bold')
    plt.xlabel("Distance (m)", fontsize=12)
    plt.ylabel("Elevation (m)", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.xlim(0, max_distance)
    plt.ylim(min_elevation, max_elevation)
    
    # Add legend
    if resort_handles:
        plt.legend(handles=resort_handles, loc='upper right', fontsize=10, 
                  framealpha=0.7, title="Starting Lifts")
    
    # Save individual resort file
    resort_file = os.path.join(charts_dir, f"ski_path_elevation_{resort.replace(' ', '_').lower()}.png")
    plt.tight_layout()
    plt.savefig(resort_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {resort_file}")

print("All visualizations completed.")
con.close()