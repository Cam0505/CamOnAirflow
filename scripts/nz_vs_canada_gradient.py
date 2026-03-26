import os
import textwrap
import duckdb
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
from matplotlib.patches import Patch
from project_path import get_project_paths, set_dlt_env_vars

# --- ENV, DuckDB connection ---
paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")
con = duckdb.connect(database_string)

# --- Load data ---
df_all = con.execute("""
    SELECT
        country_code,
        resort,
        gradient_bin,
        gradient_bin_center_deg,
        terrain_m
    FROM camonairflow.public_staging.staging_ski_gradient_distribution
    WHERE country_code IN ('NZ', 'CA')
    ORDER BY country_code, gradient_bin
""").df()

# Aggregate all NZ resorts into a single series.
df_nz = (
    df_all[df_all["country_code"] == "NZ"]
    .groupby(["gradient_bin", "gradient_bin_center_deg"], as_index=False, sort=True)["terrain_m"]
    .sum()
)

# Single CA resort.
df_ca = df_all[df_all["country_code"] == "CA"].copy()
ca_resorts = df_ca["resort"].unique()
ca_label = ca_resorts[0] if len(ca_resorts) == 1 else "Canada"

df_ca_agg = (
    df_ca
    .groupby(["gradient_bin", "gradient_bin_center_deg"], as_index=False, sort=True)["terrain_m"]
    .sum()
)

if df_nz.empty and df_ca_agg.empty:
    raise SystemExit("No data found for NZ or CA in staging_ski_gradient_distribution.")

# --- Shared y-axis scale ---
y_max = max(
    float(df_nz["terrain_m"].max()) if not df_nz.empty else 0,
    float(df_ca_agg["terrain_m"].max()) if not df_ca_agg.empty else 0,
)
y_top = y_max * 1.05 if y_max > 0 else 1.0

nz_resort_count = int(df_all[df_all["country_code"] == "NZ"]["resort"].nunique())
nz_resorts = sorted(df_all[df_all["country_code"] == "NZ"]["resort"].unique())
nz_resorts_listed = textwrap.fill(
    ", ".join(nz_resorts),
    width=170,
)

# --- Styling ---
colormap = matplotlib.colormaps["viridis"]
nz_color = colormap(0.25)
ca_color = colormap(0.65)
nz_edge = colormap(0.1)
ca_edge = colormap(0.5)
bar_width = 2.05

# Broad, non-resort-specific guide to how gradient commonly maps to run colour.
gradient_bands = [
    {"start": 5, "end": 15, "label": "Green", "color": "#57a773"},
    {"start": 15, "end": 25, "label": "Blue", "color": "#4c78a8"},
    {"start": 25, "end": 35, "label": "Red", "color": "#e45756"},
    {"start": 35, "end": 50, "label": "Black", "color": "#3a3a3a"},
]

fig, (ax_nz, ax_ca) = plt.subplots(1, 2, figsize=(16, 8.5), sharey=True)
fig.subplots_adjust(top=0.80, bottom=0.24, left=0.08, right=0.985, wspace=0.08)

def _band_color_for_value(value):
    for band in gradient_bands:
        if band["start"] <= value < band["end"] or (value == 50 and band["end"] == 50):
            return band["color"]
    return "#444444"

def _apply_gradient_bands(ax):
    for band in gradient_bands:
        ax.axvspan(band["start"], band["end"], color=band["color"], alpha=0.1, zorder=0)

    for tick_value, tick_label in zip(ax.get_xticks(), ax.get_xticklabels()):
        tick_label.set_color(_band_color_for_value(tick_value))
        tick_label.set_fontweight("bold")

def _draw_panel(ax, df, color, edge_color, title):
    if df.empty:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes, fontsize=13)
    else:
        ax.bar(
            df["gradient_bin_center_deg"],
            df["terrain_m"],
            width=bar_width,
            color=color,
            edgecolor=edge_color,
            linewidth=0.5,
        )
    ax.set_xlim(5, 50)
    ax.set_ylim(0, y_top)
    ax.set_xticks(np.arange(5, 51, 5))
    ax.tick_params(axis="x", labelrotation=45, labelsize=11, labelbottom=True)
    ax.tick_params(axis="y", labelsize=11)
    _apply_gradient_bands(ax)
    ax.set_xlabel("Gradient Bucket Center (°)", fontsize=12, fontweight="bold")
    ax.set_ylabel("Skiable Terrain (m)", fontsize=12, fontweight="bold")
    ax.grid(True, axis="y", linestyle="--", alpha=0.35)
    ax.set_title(title, fontsize=15, fontweight="bold", pad=10)

_draw_panel(
    ax_nz, df_nz, nz_color, nz_edge,
    "New Zealand",
)
_draw_panel(
    ax_ca, df_ca_agg, ca_color, ca_edge,
    f"Canada — {ca_label}",
)

fig.suptitle(
    "Ski Terrain Gradient Distribution: New Zealand vs Canada",
    fontsize=18, fontweight="bold",
    y=0.965,
)

fig.legend(
    handles=[
        Patch(facecolor=band["color"], edgecolor="none", alpha=0.2, label=f"{band['label']}: {band['start']}°-{band['end']}°")
        for band in gradient_bands
    ],
    loc="upper center",
    ncol=4,
    frameon=False,
    bbox_to_anchor=(0.5, 0.925),
)

fig.text(
    0.5,
    0.11,
    "Gradient guide uses broad, intuitive run-colour bands only; it is not a resort-specific rating system.",
    ha="center",
    fontsize=10,
    color="#555555",
)

fig.text(
    0.5,
    0.045,
    f"NZ resorts included ({nz_resort_count}): {nz_resorts_listed}",
    ha="center",
    fontsize=8.5,
    style="italic",
    color="#555555",
)

out_path = "charts/nz_vs_canada_gradient.png"
plt.savefig(out_path, dpi=250, bbox_inches="tight")
print(f"Saved: {out_path}")
plt.show()
