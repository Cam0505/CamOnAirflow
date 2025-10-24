import duckdb
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
import os
import pandas as pd
import matplotlib.pyplot as plt
import datetime
from itertools import groupby
from operator import itemgetter
import matplotlib.dates as mdates

# Load environment variables and set DLT config
paths = get_project_paths()
set_dlt_env_vars(paths)

PROJECT_ROOT = paths["PROJECT_ROOT"]
ENV_FILE = paths["ENV_FILE"]

load_dotenv(dotenv_path=ENV_FILE)

database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")

# Connect to DuckDB/MotherDuck
con = duckdb.connect(database_string)

# Load and filter the data
df = con.execute("""
    SELECT date,
        ndsi,
        ndwi,
        ndii,
        location,
        ndsi_smooth,
        ndwi_smooth,
        ndii_smooth
    FROM camonairflow.spectral.ice_indices
    ORDER BY date ASC
""").df()

# -----------------------------
# NZ-optimized classifier
# -----------------------------
# CHANGED: thresholds tuned for NZ + fixed branch ordering (no unreachable 'Dry/Brittle'),
# CHANGED: removed the 0.50–0.60 NDII gap and tightened wetness logic,
# CHANGED: added small Aug–Oct (NZ spring) relaxation of NDSI by 0.05.
def classify_ice_point(ndsi, ndwi, ndii, month):  # CHANGED
    # seasonal tweak for NZ spring (Aug–Oct)
    ndsi_thresh_main = 0.30 - (0.05 if month in (8, 9, 10) else 0.0)  # CHANGED

    if ndsi > ndsi_thresh_main:
        # First identify very dry/brittle blue ice before "good" (ordering matters)  # CHANGED
        if ndii < 0.30 and ndwi < 0.05:
            return "Dry/Brittle Ice"

        # Good ice: strong snow/ice signal, very little liquid water               # CHANGED
        if ndii < 0.45 and ndwi < 0.10:
            return "Good Ice Conditions"

        # Wet/thawing if either water index increases                              # CHANGED
        if ndii >= 0.45 or ndwi >= 0.10:
            return "Wet/Thawing Ice"

        # Otherwise uncertain within ice-present regime
        return "Uncertain Ice"

    elif 0.15 < ndsi <= ndsi_thresh_main:  # CHANGED (was 0.20)
        # Patchy mixed surfaces: allow modest water without calling it 'wet'       # CHANGED
        if ndii < 0.60 and ndwi < 0.25:
            return "Patchy Ice/Snow"
        else:
            return "Patchy & Wet"
    else:
        return "Bare Rock or Error"

# Only plot one location (e.g. Wye Creek)
df = df[df["location"] == "Wye Creek"]
df["date"] = pd.to_datetime(df["date"])
df = df.reset_index(drop=True)

# CHANGED: pass 'month' into classifier to enable seasonal tweak
df["label"] = df.apply(
    lambda row: classify_ice_point(
        row["ndsi_smooth"], row["ndwi_smooth"], row["ndii_smooth"], row["date"].month  # CHANGED
    ),
    axis=1
)

# Color palette for all possible labels
label_colors = {
    "Good Ice Conditions": "#66c2a5",     # greenish
    "Wet/Thawing Ice": "#fc8d62",         # orange
    "Dry/Brittle Ice": "#8da0cb",         # blue/purple
    "Patchy Ice/Snow": "#ffd92f",         # yellow
    "Patchy & Wet": "#fdb863",            # tan
    "Uncertain Ice": "#bdbdbd",           # light grey
    "Bare Rock or Error": "#e78ac3",      # pink
}

fig, ax = plt.subplots(figsize=(12, 6))

# Draw shaded regions and label the middle of each (vertical bands)
for label, group in groupby(enumerate(df["label"]), key=itemgetter(1)):
    indices = [i for i, _ in group]
    start = df["date"].iloc[indices[0]]
    end = df["date"].iloc[indices[-1]] + datetime.timedelta(days=1)
    duration = (end - start).days

    ax.axvspan(start, end, color=label_colors.get(label, "#ffffff"), alpha=0.23)

    if duration >= 10:
        midpoint = start + (end - start) / 2
        ax.text(midpoint, -0.85, label, fontsize=7.5, ha="center", va="bottom", rotation=90, alpha=0.8)

# Plot indices (smoothed and raw)
df.set_index("date", inplace=True)
ax.plot(df.index, df["ndsi"], label="NDSI (raw)", linestyle="--", alpha=0.4)
ax.plot(df.index, df["ndwi"], label="NDWI (raw)", linestyle=":", alpha=0.4)
ax.plot(df.index, df["ndii"], label="NDII (raw)", linestyle="-.", alpha=0.4)
ax.plot(df.index, df["ndsi_smooth"], label="NDSI (smoothed)", marker="o")
ax.plot(df.index, df["ndwi_smooth"], label="NDWI (smoothed)", marker="x")
ax.plot(df.index, df["ndii_smooth"], label="NDII (smoothed)", marker="^")

ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=14, maxticks=40))
ax.set_title(f"Ice Quality Indices Over Time (Statistical API) for {df['location'].iloc[0]}")
ax.set_xlabel("Date")
ax.set_ylabel("Index Value")
ax.set_ylim(-1, 1)
ax.legend(loc="lower left")
ax.grid(True)
fig.autofmt_xdate()
plt.tight_layout()
plt.savefig("ice_quality_time_series.png", dpi=150)
print("📈 Saved: ice_quality_time_series.png")
