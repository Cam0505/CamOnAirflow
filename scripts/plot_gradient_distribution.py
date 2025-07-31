import os
import duckdb
from dotenv import load_dotenv
import itertools
import matplotlib.pyplot as plt
import matplotlib
from project_path import get_project_paths, set_dlt_env_vars

# --- ENV, DuckDB connection ---
paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")
con = duckdb.connect(database_string)


# --- Define Regions ---
NZ_RESORTS = [
    'Temple Basin Ski Area', 'Mount Cheeseman Ski Area', 'Mount Dobson Ski Field',
    'Roundhill Ski Field', 'Mount Hutt Ski Area', 'Broken River Ski Area',
    'Porters Ski Area', 'Rainbow Ski Area', 'Mount Olympus Ski Area',
    'The Remarkables Ski Area', 'Whakapapa Ski Area', 'Cardrona Alpine Resort',
    'Manganui Ski Area', 'Coronet Peak Ski Area', 'Mount Lyford Alpine Resort',
    'Treble Cone Ski Area', 'Tūroa Ski Area', 'Craigieburn Valley Ski Area',
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

# --- Query ---
query = """
SELECT
    resort,
    gradient_bin,
    gradient_bin_center_deg,
    cumulative_pct_runs
FROM camonairflow.public_staging.staging_ski_gradient_distribution
WHERE resort in {resorts}
ORDER BY resort, gradient_bin
"""

# --- Plotting ---
markers = itertools.cycle(('o', 's', 'D', '^', 'v', '<', '>', 'p', '*', 'h', 'H', 'X', 'd'))
# Use the new Matplotlib colormap API to avoid deprecation warning
colormap = matplotlib.colormaps['tab20']
num_resorts = len(NZ_RESORTS)
colors = [colormap(i % 20) for i in range(num_resorts)]  # tab20 has 20 distinct colors

for region, resorts in REGIONS.items():
    df = con.execute(query.format(resorts=tuple(resorts))).df()

    fig, ax = plt.subplots(figsize=(12, 8))

    for idx, resort in enumerate(sorted(df['resort'].unique())):
        df_resort = df[df['resort'] == resort]
        marker = next(markers)
        color = colors[idx % len(colors)]
        ax.plot(
            df_resort['gradient_bin_center_deg'],
            df_resort['cumulative_pct_runs'] * 100,
            label=resort,
            linewidth=2,
            alpha=0.85,
            marker=marker,
            markersize=6,
            color=color
        )

    ax.set_title(f"% of Runs by Gradient (°) - {region}", fontsize=20, fontweight="bold")
    ax.set_xlabel("Gradient (°)", fontsize=14)
    ax.set_ylabel("% of Runs ≤ Gradient (°)", fontsize=14)
    ax.set_xlim(5, 50)
    ax.set_ylim(0, 105)
    ax.grid(True, linestyle='--', alpha=0.6)
    ax.legend(fontsize=9, loc='upper left', bbox_to_anchor=(1.02, 1))
    plt.tight_layout()
    out_path = f"charts/gradient_distribution_{region.lower().replace(' ', '_')}.png"
    plt.savefig(out_path, dpi=250, bbox_inches='tight')
    plt.show()
