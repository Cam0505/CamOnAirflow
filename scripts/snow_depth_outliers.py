import duckdb
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
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

# --- Load Data ---
df = con.execute("""
    SELECT
        date,
        location,
        snowfall,
        temperature_mean,
        country,
        avg_snow_depth
    FROM camonairflow.skifields.ski_field_snowfall
    WHERE avg_snow_depth IS NOT NULL
      AND country in ('NZ')
    ORDER BY date ASC
""").df()
df['date'] = pd.to_datetime(df['date'])
df['dayofyear'] = df['date'].dt.dayofyear

locations = df['location'].unique()
n_locs = len(locations)
ncols = 3
nrows = int(np.ceil(n_locs / ncols))

fig, axes = plt.subplots(nrows, ncols, figsize=(21, 13), sharex=False)
axes = axes.flatten()

for idx, loc in enumerate(locations):
    ax = axes[idx]
    data = df[df['location'] == loc].copy()

    # Calculate mean for each day-of-year for both variables
    doy_means_depth = data.groupby('dayofyear')['avg_snow_depth'].mean()
    doy_means_snowfall = data.groupby('dayofyear')['snowfall'].mean()
    data['doy_mean_depth'] = data['dayofyear'].map(doy_means_depth)
    data['doy_mean_snowfall'] = data['dayofyear'].map(doy_means_snowfall)

    # Calculate anomalies
    data['anomaly'] = data['avg_snow_depth'] - data['doy_mean_depth']
    data['snowfall_anomaly'] = data['snowfall'] - data['doy_mean_snowfall']

    # Smoothed (rolling) anomaly
    data['anomaly_smoothed'] = data['anomaly'].rolling(14, center=True, min_periods=1).mean()
    data['snowfall_anomaly_smoothed'] = data['snowfall_anomaly'].rolling(14, center=True, min_periods=1).mean()

    # --- Find symmetric y-limits centered on 0 for both axes ---
    y1max = np.nanmax(np.abs(data['anomaly']))
    y2max = np.nanmax(np.abs(data['snowfall_anomaly_smoothed']))
    y1lim = (-y1max, y1max)
    y2lim = (-y2max, y2max)

    # --- Plot snow depth anomaly bars ---
    ax.bar(
        data['date'],
        data['anomaly'],
        color=np.where(data['anomaly'] > 0, '#2976e3', '#d1504c'),
        alpha=0.7, width=3, antialiased=False, linewidth=0
    )
    # Overlay smoothed anomaly
    ax.plot(
        data['date'],
        data['anomaly_smoothed'],
        color='k', lw=1.3, alpha=0.9, label="Snow Depth Smoothed"
    )
    ax.axhline(0, color='gray', linestyle='--', lw=1)
    ax.set_title(loc, fontsize=15, fontweight='bold')
    ax.set_xlabel("Date")
    ax.set_ylabel("Snow Depth Anomaly (m)")
    ax.grid(True, linestyle='--', alpha=0.23)
    ax.set_ylim(y1lim)
    ax.legend(fontsize=8, loc='upper left')

    # --- Secondary Y-axis: Snowfall anomaly as shaded area ---
    ax2 = ax.twinx()
    # Fill between for positive (orange) and negative (blue) anomaly
    ax2.fill_between(
        data['date'],
        0, data['snowfall_anomaly_smoothed'],
        where=(data['snowfall_anomaly_smoothed'] > 0),
        color='orange', alpha=0.24, interpolate=True, label='Snowfall Anomaly +'
    )
    ax2.fill_between(
        data['date'],
        0, data['snowfall_anomaly_smoothed'],
        where=(data['snowfall_anomaly_smoothed'] < 0),
        color='#377eb8', alpha=0.15, interpolate=True, label='Snowfall Anomaly -'
    )
    ax2.set_ylabel("Snowfall Anomaly (mm/day)", color='orange')
    ax2.tick_params(axis='y', colors='orange', labelsize=9)
    ax2.set_ylim(y2lim)

    # Add a 0-line on ax2 as well, to make it clear
    ax2.axhline(0, color='orange', linestyle=':', lw=1)

# Remove unused axes
for i in range(len(locations), len(axes)):
    fig.delaxes(axes[i])

plt.suptitle(
    "Daily Natural Snow Depth Anomaly by Ski Field (NZ)\n"
    "Bars: Depth anomaly (vs long-term day-of-year mean). Shaded: Snowfall anomaly (right axis, smoothed, orange=above avg, blue=below avg). Zeros are aligned.",
    fontsize=20, fontweight='bold'
)
plt.tight_layout(rect=(0, 0.04, 1, 0.97))
plt.savefig("charts/snow_depth_anomaly_with_snowfall_shaded_aligned.png", dpi=140, bbox_inches='tight')
print("✅ Saved chart: snow_depth_anomaly_with_snowfall_shaded_aligned.png")
