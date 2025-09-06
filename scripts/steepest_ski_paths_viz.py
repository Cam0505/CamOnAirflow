#!/usr/bin/env python3
import os
import duckdb
import matplotlib.pyplot as plt
import numpy as np
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

print("Loading ski paths data from mart...")
ski_paths = con.execute("""
    SELECT
        path_id,
        resort,
        starting_lift,
        starting_lift_id,
        run_count,
        total_distance_m,
        total_vertical_m,
        avg_gradient_deg,
        max_gradient_deg,
        run_path,
        node_ids,
        ending_type,
        ending_name,
        ending_id
    FROM camonairflow.public_common.mart_nz_ski_paths
    WHERE total_distance_m >= 500
    ORDER BY resort, starting_lift, avg_gradient_deg DESC
""").df()
con.close()

print(f"Loaded {len(ski_paths)} ski paths over 500m")

# --- Wrapper for path text ---
def wrap_path_lines(path: str, width: int = 60):
    """Wrap path text to multiple lines for better display"""
    lines = textwrap.wrap(path, width=width) if path else [""]
    return lines

# --- Generate steepest paths visualization per resort ---
def generate_steepest_paths_chart(resort, out_dir="charts", dpi=300):
    """Generate chart showing the top 10 steepest paths for each lift at a resort"""
    print(f"Generating chart for {resort}...")
    
    # Create output directory if it doesn't exist
    os.makedirs(out_dir, exist_ok=True)
    
    # Get all paths for this resort
    resort_paths = ski_paths[ski_paths['resort'] == resort].copy()
    
    if len(resort_paths) == 0:
        print(f"No paths found for {resort}")
        return
    
    # Get unique lifts at this resort
    lifts = resort_paths['starting_lift'].unique()
    print(f"Found {len(lifts)} lifts at {resort}")
    
    # Build chart content
    all_lines = []
    all_lines.append(("title", f"{resort} — Top 10 Steepest Ski Paths"))
    all_lines.append(("gap", ""))
    all_lines.append(("subtitle", "Paths over 500m, ranked by average gradient"))
    all_lines.append(("gap", ""))
    
    # Process each lift
    for lift in lifts:
        all_lines.append(("lift", f"{lift}"))
        
        # Get top 10 steepest paths for this lift
        lift_paths = resort_paths[resort_paths['starting_lift'] == lift].head(10)
        
        if len(lift_paths) == 0:
            all_lines.append(("note", "  No paths found for this lift"))
            continue
            
        # Add each path
        for i, path in enumerate(lift_paths.itertuples(), 1):
            # Add path header with stats
            all_lines.append(("rank", f"{i}. {path.avg_gradient_deg:.1f}° avg / {path.max_gradient_deg:.1f}° max • {path.total_distance_m:.0f}m • {path.total_vertical_m:.0f}m vertical"))
            
            # Add path description
            for line in wrap_path_lines(path.run_path):
                all_lines.append(("path", f"   {line}"))
                
            # Add ending info
            all_lines.append(("ending", f"   Ending: {path.ending_name} ({path.ending_type})"))
            all_lines.append(("gap", ""))
        
        all_lines.append(("gap", ""))
    
    # Figure size proportional to line count
    fig_h = max(10, len(all_lines) * 0.3)
    fig, ax = plt.subplots(figsize=(14, fig_h))
    ax.axis("off")
    
    # Set background color
    fig.patch.set_facecolor('#f8f9fa')
    ax.set_facecolor('#f8f9fa')
    
    # Place text with proper formatting
    y = 1.0
    line_step = 1.0 / (len(all_lines) + 5)
    
    for kind, text in all_lines:
        if kind == "gap":
            y -= line_step * 0.5
            continue
            
        if kind == "title":
            ax.text(0.5, y, text, fontsize=24, fontweight="bold",
                    ha="center", va="top", transform=ax.transAxes)
        elif kind == "subtitle":
            ax.text(0.5, y, text, fontsize=16,
                    ha="center", va="top", transform=ax.transAxes)
        elif kind == "lift":
            # Add a horizontal line before each lift
            ax.axhline(y - line_step * 0.2, xmin=0.05, xmax=0.95, color='#007bff', alpha=0.8, linewidth=2)
            ax.text(0.05, y, text, fontsize=18, fontweight="bold", color='#007bff',
                    ha="left", va="top", transform=ax.transAxes)
        elif kind == "rank":
            ax.text(0.07, y, text, fontsize=12, fontweight="bold",
                    ha="left", va="top", transform=ax.transAxes)
        elif kind == "path":
            ax.text(0.07, y, text, fontsize=11,
                    ha="left", va="top", transform=ax.transAxes)
        elif kind == "ending":
            ax.text(0.07, y, text, fontsize=10, style='italic', color='#666666',
                    ha="left", va="top", transform=ax.transAxes)
        elif kind == "note":
            ax.text(0.07, y, text, fontsize=11, style='italic', color='#666666',
                    ha="left", va="top", transform=ax.transAxes)
            
        y -= line_step
    
    # Add footer with data source
    ax.text(0.5, 0.01, "Data source: camonairflow.public_common.mart_nz_ski_paths",
            fontsize=8, color='#666666', ha="center", va="bottom", transform=ax.transAxes)
    
    # Save figure
    out_path = os.path.join(out_dir, f"steepest_paths_{resort.replace(' ', '_').lower()}.png")
    plt.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    print(f"✅ Saved {out_path}")

# Process each resort
resorts = sorted(ski_paths['resort'].unique())
print(f"Generating charts for {len(resorts)} resorts...")

for resort in resorts:
    generate_steepest_paths_chart(resort)

print("All charts generated successfully!")