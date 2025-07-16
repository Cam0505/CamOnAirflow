import duckdb
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
import os
import pandas as pd
import matplotlib.pyplot as plt
import datetime
from itertools import groupby
from operator import itemgetter

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
        ndii_smooth,
    FROM camonairflow.main.ice_indices 
    ORDER BY date ASC
""").df()

def classify_ice_point(ndsi, ndwi, ndii):
    if ndsi > 0.40 and ndii < 0.70 and ndwi < 0.25:
        return "Good Ice Conditions"
    elif ndsi > 0.40 and (ndii >= 0.70 or ndwi >= 0.25):
        return "Wet Conditions"
    elif 0.20 < ndsi <= 0.40:
        return "Patchy Conditions"
    elif ndsi > 0.4 and ndii < 0.3:
        return "Drier Ice Conditions"
    else:
        return "Bare Rock or error"

df = df[df["location"] == "Wye Creek"]
df["date"] = pd.to_datetime(df["date"])
df = df.reset_index(drop=True)  # for groupby processing
df["label"] = df.apply(
    lambda row: classify_ice_point(row["ndsi_smooth"], row["ndwi_smooth"], row["ndii_smooth"]),
    axis=1
)

label_colors = {
    "Good Ice Conditions": "#66c2a5",
    "Wet Conditions": "#fc8d62",
    "Patchy Conditions": "#ffd92f",
    "Bare Rock or error": "#e78ac3",
}

fig, ax = plt.subplots(figsize=(12, 6))

# Draw shaded regions and vertical band labels
for label, group in groupby(enumerate(df["label"]), key=itemgetter(1)):
    indices = [i for i, _ in group]
    start = df["date"].iloc[indices[0]]
    end = df["date"].iloc[indices[-1]] + datetime.timedelta(days=1)
    duration = (end - start).days

    ax.axvspan(start, end, color=label_colors.get(label, "#ffffff"), alpha=0.2)

    if duration >= 14:
        midpoint = start + (end - start) / 2
        ax.text(midpoint, -0.85, label, fontsize=7.5, ha="center", va="bottom", rotation=90, alpha=0.8)

# Plot smoothed and raw indices
df.set_index("date", inplace=True)

ax.plot(df.index, df["ndsi"], label="NDSI (raw)", linestyle="--", alpha=0.4)
ax.plot(df.index, df["ndwi"], label="NDWI (raw)", linestyle=":", alpha=0.4)
ax.plot(df.index, df["ndii"], label="NDII (raw)", linestyle="-.", alpha=0.4)

ax.plot(df.index, df["ndsi_smooth"], label="NDSI (smoothed)", marker="o")
ax.plot(df.index, df["ndwi_smooth"], label="NDWI (smoothed)", marker="x")
ax.plot(df.index, df["ndii_smooth"], label="NDII (smoothed)", marker="^")

# Final formatting
ax.set_title("Ice Quality Indices Over Time (Statistical API)")
ax.set_xlabel("Date")
ax.set_ylabel("Index Value")
ax.set_ylim(-1, 1)
ax.legend(loc="lower left")
ax.grid(True)
fig.autofmt_xdate()
plt.tight_layout()
plt.savefig("ice_quality_time_series.png", dpi=150)
print("📈 Saved: ice_quality_time_series.png")
