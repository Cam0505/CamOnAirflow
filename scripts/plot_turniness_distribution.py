import os
import numpy as np
import duckdb
from dotenv import load_dotenv
import itertools
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
from project_path import get_project_paths, set_dlt_env_vars
from adjustText import adjust_text

# --- ENV, DuckDB connection ---
paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")
con = duckdb.connect(database_string)

# --- Resorts ---
NZ_RESORTS = [
    'Temple Basin Ski Area', 'Mount Cheeseman Ski Area', 'Mount Dobson Ski Field',
    'Roundhill Ski Field', 'Mount Hutt Ski Area', 'Broken River Ski Area',
    'Porters Ski Area', 'Rainbow Ski Area', 'Mount Olympus Ski Area',
    'The Remarkables Ski Area', 'Whakapapa Ski Area', 'Cardrona Alpine Resort',
    'Manganui Ski Area', 'Coronet Peak Ski Area', 'Mount Lyford Alpine Resort',
    'Treble Cone Ski Area', 'Tūroa Ski Area', 'Craigieburn Valley Ski Area',
    'Fox Peak Ski Area'
]

# --- Query ---
query = """
SELECT
    resort,
    run_name,
    piste_type,
    difficulty,
    run_length_m,
    turniness_score,
    turniness_score / NULLIF(run_length_m, 0) AS turniness_per_meter
FROM camonairflow.public_base.base_ski_run_turniness
WHERE resort in {resorts} AND turniness_score IS NOT NULL
and run_length_m >= 250
"""

df = con.execute(query.format(resorts=tuple(NZ_RESORTS))).df()

# --- Chart 1: Cumulative Distribution of Turniness per Meter (with shapes) ---
markers = itertools.cycle(('o', 's', 'D', '^', 'v', '<', '>', 'p', '*', 'h', 'H', 'X', 'd'))
colormap = matplotlib.colormaps['tab20']
num_resorts = len(NZ_RESORTS)
colors = [colormap(i % 20) for i in range(num_resorts)]

fig, ax = plt.subplots(figsize=(14, 10))  # Increased overall chart size
for idx, (resort, group) in enumerate(df.groupby("resort")):
    vals = np.sort(group["turniness_per_meter"])
    pct = np.linspace(0, 100, len(vals))
    marker = next(markers)
    color = colors[idx % len(colors)]
    ax.plot(vals, pct, label=resort, linewidth=2, alpha=0.8, marker=marker, markersize=7, color=color)
ax.set_xlabel("Turniness per meter", fontsize=14)  # Slightly decreased axis font
ax.set_ylabel("% of Runs ≤ Turniness", fontsize=14)
ax.set_title("Cumulative Distribution of Turniness per Meter by Resort", fontsize=18)
legend = ax.legend(fontsize=12, loc='upper left', bbox_to_anchor=(1.02, 1), markerscale=2)  # Larger dots in legend
plt.tight_layout()
plt.savefig("charts/turniness_cumulative_nz.png", dpi=250, bbox_inches='tight')
plt.show()


resort_color_map = {resort: colors[idx % len(colors)] for idx, resort in enumerate(sorted(df['resort'].unique()))}

plt.figure(figsize=(14, 10))
untitled_counter = 1

# Get the overall top 6 runs for turniness_per_meter and run_length_m (across all resorts, after filtering)
top6_turniness = df.nlargest(6, "turniness_per_meter")
top6_length = df.nlargest(6, "run_length_m")
outliers = pd.concat([top6_turniness, top6_length]).drop_duplicates(subset=["run_name", "resort"])

texts = []
for resort, group in df.groupby("resort"):
    color = resort_color_map[resort]
    plt.scatter(
        group["run_length_m"], group["turniness_per_meter"],
        label=resort, alpha=0.7, s=60, color=color
    )

for _, row in outliers.iterrows():
    run_name = row["run_name"]
    if not run_name or str(run_name).strip().lower() in ("", "none", "null"):
        run_name = f"Untitled{untitled_counter}"
        untitled_counter += 1
    color = resort_color_map.get(row["resort"], "black")
    texts.append(
        plt.text(
            row["run_length_m"], row["turniness_per_meter"], run_name,
            fontsize=13, color=color
        )
    )

# Adjust text to avoid overlap, with arrows if needed
adjust_text(
    texts,
    arrowprops=dict(arrowstyle="->", color='gray', lw=1),
    expand_points=(1.2, 1.4),
    expand_text=(1.2, 1.4),
    force_text=0.75,
    force_points=0.75
)

plt.xlabel("Run Length (m)", fontsize=16)
plt.ylabel("Turniness per meter", fontsize=16)
plt.title("Run Length vs Turniness per Meter (NZ Resorts)", fontsize=20)
plt.xticks(fontsize=13)
plt.yticks(fontsize=13)
plt.legend(fontsize=12, loc='upper left', bbox_to_anchor=(1.02, 1), markerscale=2)
plt.tight_layout()
plt.savefig("charts/turniness_scatter_nz.png", dpi=250, bbox_inches='tight')
plt.show()