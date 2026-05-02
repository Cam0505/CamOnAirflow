"""
Ōhau Snow Fields — 1m LiDAR DEM ski map
========================================
Renders the ski run network for Ōhau Snow Fields on top of a genuine 1m
LiDAR-derived hillshade, using elevation data from the Canterbury 2020-2023
survey hosted on s3://nz-elevation (ingested to MotherDuck by
nz_elevation_lidar_dlt.py).

Output: charts/ski_network_graphs/NZ/Ohau_Snow_Fields_lidar.png

Usage:
    python scripts/ohau_lidar_map.py
"""

import os
import math
import sys
import numpy as np
from scipy.ndimage import distance_transform_edt, gaussian_filter
from scipy.interpolate import RegularGridInterpolator
from matplotlib.colors import LightSource, LinearSegmentedColormap
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import matplotlib.patches as mpatches
from pyproj import Transformer
import duckdb
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars

# ── env ───────────────────────────────────────────────────────────────────────
paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")

RESORT = "Ōhau Snow Fields"
OUTPUT_DIR = os.path.join(paths["PROJECT_ROOT"], "charts", "ski_network_graphs", "NZ")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── style ─────────────────────────────────────────────────────────────────────
BG_COLOR = "#0d0d12"
LIFT_COLOR = "#FFD700"
OUTLINE_COLOR = "#1a1a2e"   # dark navy outline on run lines

DIFFICULTY_COLORS = {
    "novice": "#4CAF50",
    "easy": "#4CAF50",
    "intermediate": "#42A5F5",
    "advanced": "#EF5350",
    "expert": "#E040FB",
    "freeride": "#00BCD4",
    None: "#90A4AE",
    "nan": "#90A4AE",
    "": "#90A4AE",
}

# NZTM2000 (EPSG:2193) → WGS84 (EPSG:4326)
_NZTM_TO_WGS84 = Transformer.from_crs("EPSG:2193", "EPSG:4326", always_xy=True)

# Alpine terrain colormap: deeper contrast to show terrain relief clearly
_TERRAIN_CMAP = LinearSegmentedColormap.from_list(
    "alpine_nz",
    [
        (0.18, 0.15, 0.11),   # very dark brown  — deepest valleys / shadow
        (0.38, 0.33, 0.25),   # warm earth       — mid-valley
        (0.55, 0.52, 0.46),   # tussock/scree    — lower slopes
        (0.70, 0.70, 0.68),   # grey rock        — upper slopes
        (0.84, 0.85, 0.87),   # blue-grey snow   — snowpack
        (0.94, 0.95, 0.96),   # pale summit      — top (not pure white)
    ],
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _safe_color(difficulty):
    if difficulty is None or str(difficulty).lower() in ("nan", "", "none"):
        return DIFFICULTY_COLORS[None]
    return DIFFICULTY_COLORS.get(str(difficulty).lower(), DIFFICULTY_COLORS[None])


def _equirect(lons, lats, lon0, lat0):
    """Equirectangular projection → metres."""
    cos_lat = math.cos(math.radians(lat0))
    xs = (np.asarray(lons) - lon0) * cos_lat * 111_320
    ys = (np.asarray(lats) - lat0) * 111_320
    return xs, ys


def _rotate(xs, ys, angle):
    c, s = math.cos(angle), math.sin(angle)
    return c * xs - s * ys, s * xs + c * ys


def _compute_rotation(runs_meta_df, lon0, lat0):
    """Angle (radians) so the mean downhill direction points to -Y (bottom of map)."""
    import pandas as pd
    vectors = []
    for _, row in runs_meta_df.iterrows():
        if any(pd.isna(row[c]) for c in ("top_lat", "top_lon", "bottom_lat", "bottom_lon")):
            continue
        tx, ty = _equirect([row["top_lon"]], [row["top_lat"]], lon0, lat0)
        bx, by = _equirect([row["bottom_lon"]], [row["bottom_lat"]], lon0, lat0)
        dx = float(bx[0] - tx[0])
        dy = float(by[0] - ty[0])
        length = math.hypot(dx, dy)
        if length > 5:
            vectors.append((dx / length, dy / length))
    if not vectors:
        return 0.0
    mean_dx = float(np.mean([v[0] for v in vectors]))
    mean_dy = float(np.mean([v[1] for v in vectors]))
    return -math.pi / 2 - math.atan2(mean_dy, mean_dx)


def _fill_nans_nearest(arr):
    nan_mask = np.isnan(arr)
    if not nan_mask.any():
        return arr.copy()
    edt_out: tuple[np.ndarray, np.ndarray] = distance_transform_edt(  # type: ignore[assignment]
        nan_mask, return_indices=True)
    _, idx = edt_out
    filled = arr.copy()
    filled[nan_mask] = arr[idx[0][nan_mask], idx[1][nan_mask]]
    return filled


def _build_dem(dem_lons, dem_lats, dem_elevs, lon0, lat0, rot):
    """
    Grid LiDAR points → smooth 2-m DEM.
    Returns grid_smooth (ny, nx), gx_c (nx,), gy_c (ny,), interpolator, bin_m.
    """
    print(f"  Projecting {len(dem_lons):,} LiDAR points …")
    xs, ys = _equirect(dem_lons, dem_lats, lon0, lat0)
    xs, ys = _rotate(xs, ys, rot)

    BIN_M = 2.0
    x_min, x_max = float(xs.min()), float(xs.max())
    y_min, y_max = float(ys.min()), float(ys.max())
    nx = max(int((x_max - x_min) / BIN_M) + 1, 2)
    ny = max(int((y_max - y_min) / BIN_M) + 1, 2)
    print(f"  Gridding to {nx}x{ny} at {BIN_M:.0f} m …")

    elev_sum, x_edges, y_edges = np.histogram2d(
        xs, ys, bins=[nx, ny],
        range=[[x_min, x_max], [y_min, y_max]], weights=dem_elevs,
    )
    count, _, _ = np.histogram2d(
        xs, ys, bins=[nx, ny],
        range=[[x_min, x_max], [y_min, y_max]],
    )
    with np.errstate(invalid="ignore"):
        grid = np.where(count > 0, elev_sum / count, np.nan)

    grid_t = grid.T                        # (ny, nx)
    grid_filled = _fill_nans_nearest(grid_t)
    # sigma=2: removes LiDAR flight-strip noise without blurring ridgelines.
    grid_smooth = gaussian_filter(grid_filled, sigma=2.0)

    gx_c = 0.5 * (x_edges[:-1] + x_edges[1:])   # (nx,)
    gy_c = 0.5 * (y_edges[:-1] + y_edges[1:])   # (ny,)

    interp = RegularGridInterpolator(
        (gy_c, gx_c), grid_smooth,
        method="linear", bounds_error=False, fill_value=np.nan,
    )
    return grid_smooth, gx_c, gy_c, interp, BIN_M


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    con = duckdb.connect(database_string)  # type: ignore
    print(f"Loading OSM run data for {RESORT!r} …")

    runs_df = con.execute("""
        SELECT
            p.osm_id, p.lat, p.lon, p.point_index,
            p.elevation_smoothed_m,
            r.difficulty, r.run_name, r.piste_type
        FROM camonairflow.ski_runs.ski_run_points p
        JOIN camonairflow.ski_runs.ski_runs r ON p.osm_id = r.osm_id
        WHERE p.resort = ?
          AND r.piste_type IN ('downhill', 'hike')
        ORDER BY p.osm_id, p.point_index
    """, [RESORT]).df()

    if runs_df.empty:
        print(f"No run points found for {RESORT!r}.")
        sys.exit(1)

    lifts_df = con.execute("""
        SELECT name, lift_type, top_lat, top_lon, bottom_lat, bottom_lon
        FROM camonairflow.ski_runs.ski_lifts
        WHERE resort = ?
    """, [RESORT]).df()

    runs_meta_df = con.execute("""
        SELECT top_lat, top_lon, bottom_lat, bottom_lon
        FROM camonairflow.ski_runs.ski_runs
        WHERE resort = ?
    """, [RESORT]).df()

    print("Loading LiDAR data (2 m grid) …")
    lidar_df = con.execute("""
        SELECT x_nztm, y_nztm, elevation_m
        FROM camonairflow.nz_lidar.ski_resort_elevation_1m
        WHERE resort_name = ?
          AND x_nztm % 2 = 0
          AND y_nztm % 2 = 0
    """, [RESORT]).df()

    con.close()
    print(f"  {len(lidar_df):,} LiDAR points loaded")

    if lidar_df.empty:
        print("No LiDAR data — run nz_elevation_lidar_dlt.py first.")
        sys.exit(1)

    # ── project LiDAR: NZTM → WGS84 ─────────────────────────────────────────
    dem_lons, dem_lats = _NZTM_TO_WGS84.transform(
        lidar_df["x_nztm"].values,
        lidar_df["y_nztm"].values,
    )

    # ── projection centre + uphill rotation ───────────────────────────────────
    lon0 = float(runs_df["lon"].mean())
    lat0 = float(runs_df["lat"].mean())
    rot = _compute_rotation(runs_meta_df, lon0, lat0)

    runs_df["x"], runs_df["y"] = _equirect(runs_df["lon"], runs_df["lat"], lon0, lat0)
    runs_df["x"], runs_df["y"] = _rotate(
        runs_df["x"].values, runs_df["y"].values, rot)

    # ── project lifts ─────────────────────────────────────────────────────────
    lifts_proj = []
    for _, lift in lifts_df.iterrows():
        bx, by = _equirect([lift["bottom_lon"]], [lift["bottom_lat"]], lon0, lat0)
        tx, ty = _equirect([lift["top_lon"]], [lift["top_lat"]], lon0, lat0)
        bxr, byr = _rotate(np.array([float(bx[0])]), np.array([float(by[0])]), rot)
        txr, tyr = _rotate(np.array([float(tx[0])]), np.array([float(ty[0])]), rot)
        lifts_proj.append({
            "name": lift["name"],
            "xs": [float(bxr[0]), float(txr[0])],
            "ys": [float(byr[0]), float(tyr[0])],
        })

    # ── build DEM ─────────────────────────────────────────────────────────────
    grid, gx_c, gy_c, interp, BIN_M = _build_dem(
        dem_lons, dem_lats, lidar_df["elevation_m"].values.astype(float),
        lon0, lat0, rot,
    )

    # ── hillshade (matplotlib LightSource) ────────────────────────────────────
    # azdeg=315 (NW sun), altdeg=25 — low sun angle gives long shadows and punchy terrain relief
    ls = LightSource(azdeg=315, altdeg=25)
    # blend_mode='overlay' gives strong contrast between lit and shadowed faces
    rgb = ls.shade(
        grid, cmap=_TERRAIN_CMAP, vert_exag=6.0,
        dx=BIN_M, dy=BIN_M, blend_mode="overlay",
    )

    # ── figure ────────────────────────────────────────────────────────────────
    W_IN, H_IN = 14, 18
    DPI = 230
    fig, ax = plt.subplots(figsize=(W_IN, H_IN), dpi=DPI)
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    extent = (float(gx_c[0]), float(gx_c[-1]), float(gy_c[0]), float(gy_c[-1]))
    ax.imshow(rgb, origin="lower", extent=extent, aspect="equal",
              interpolation="bilinear")

    # ── longest run by 2-D path length ────────────────────────────────────────
    run_lengths: dict = {}
    for osm_id, group in runs_df.groupby("osm_id", sort=False):
        g = group.sort_values("point_index")
        run_lengths[osm_id] = float(
            np.hypot(np.diff(g["x"].to_numpy(dtype=float)),
                     np.diff(g["y"].to_numpy(dtype=float))).sum()
        )
    longest_id = max(run_lengths, key=run_lengths.__getitem__)
    print(f"  Longest run osm_id={longest_id}  ({run_lengths[longest_id]:.0f} m)")

    # ── ski run lines ─────────────────────────────────────────────────────────
    for osm_id, group in runs_df.groupby("osm_id", sort=False):
        group = group.sort_values("point_index")
        rx = group["x"].to_numpy(dtype=float)
        ry = group["y"].to_numpy(dtype=float)

        color = _safe_color(group["difficulty"].iloc[0])
        is_hike = str(group["piste_type"].iloc[0]).lower() == "hike"
        lw = 1.8 if is_hike else 2.6
        ls_kw: dict = dict(linestyle="--", dashes=(5, 3)) if is_hike else {}

        outline_fx = [
            pe.Stroke(linewidth=lw + 2.5, foreground=OUTLINE_COLOR, capstyle="round"),
            pe.Normal(),
        ]

        if osm_id == longest_id:
            ax.plot(rx, ry, color="#FFD700", linewidth=lw + 6,
                    solid_capstyle="round", zorder=3, alpha=0.55)
            ax.plot(rx, ry, color="#FFF176", linewidth=lw + 2,
                    solid_capstyle="round", zorder=3, alpha=0.35)

        ax.plot(rx, ry, color=color, linewidth=lw,
                solid_capstyle="round", zorder=4,
                path_effects=outline_fx, **ls_kw)

    # ── lift lines ────────────────────────────────────────────────────────────
    lift_fx = [
        pe.Stroke(linewidth=4.5, foreground=OUTLINE_COLOR, capstyle="round"),
        pe.Normal(),
    ]
    for lp in lifts_proj:
        bx, by = lp["xs"][0], lp["ys"][0]   # bottom station
        tx, ty = lp["xs"][1], lp["ys"][1]   # top station
        ax.plot(lp["xs"], lp["ys"], color=LIFT_COLOR, linewidth=2.2,
                solid_capstyle="round", zorder=4, linestyle="--", dashes=(6, 3),
                path_effects=lift_fx)
        # Bottom station — filled circle
        ax.plot(bx, by, marker="o", markersize=7, color=LIFT_COLOR, zorder=5,
                markeredgecolor=OUTLINE_COLOR, markeredgewidth=1.2)
        # Top station — filled triangle pointing up
        ax.plot(tx, ty, marker="^", markersize=8, color=LIFT_COLOR, zorder=5,
                markeredgecolor=OUTLINE_COLOR, markeredgewidth=1.2)

    # ── run name labels ───────────────────────────────────────────────────────
    LABEL_MIN_DIST_M = 130.0
    placed = []
    candidates = []
    for osm_id, group in runs_df.groupby("osm_id", sort=False):
        group = group.sort_values("point_index")
        mid = len(group) // 2
        candidates.append((
            float(group["x"].count()),
            float(group["x"].iloc[mid]),
            float(group["y"].iloc[mid]),
            group["run_name"].iloc[0],
        ))
    candidates.sort(key=lambda t: t[0], reverse=True)

    for _, mx, my, run_name in candidates:
        if not run_name or not str(run_name).strip():
            continue
        if any(math.hypot(mx - px, my - py) < LABEL_MIN_DIST_M for px, py in placed):
            continue
        placed.append((mx, my))
        ax.text(
            mx, my, str(run_name),
            fontsize=5.5, color="white", ha="center", va="center",
            fontweight="bold", zorder=6,
            path_effects=[
                pe.Stroke(linewidth=2.0, foreground="#0d0d12"),
                pe.Normal(),
            ],
        )

    # ── axis limits ───────────────────────────────────────────────────────────
    all_x = list(runs_df["x"]) + [v for lp in lifts_proj for v in lp["xs"]]
    all_y = list(runs_df["y"]) + [v for lp in lifts_proj for v in lp["ys"]]
    x_min, x_max = min(all_x), max(all_x)
    y_min, y_max = min(all_y), max(all_y)
    pad_x = (x_max - x_min) * 0.08
    pad_y = (y_max - y_min) * 0.10
    ax.set_xlim(x_min - pad_x, x_max + pad_x)
    ax.set_ylim(y_min - pad_y, y_max + pad_y)
    ax.set_axis_off()

    # ── legend ────────────────────────────────────────────────────────────────
    seen: set = set()
    patches = []
    for diff, color in DIFFICULTY_COLORS.items():
        if diff is None or str(diff) in ("nan", ""):
            continue
        if diff in runs_df["difficulty"].values and diff not in seen:
            patches.append(mpatches.Patch(color=color, label=diff.capitalize()))
            seen.add(diff)
    import matplotlib.lines as mlines
    patches.append(mlines.Line2D([], [], color="white", linewidth=1.8,
                                 linestyle="--", dashes=(5, 3), label="Hike"))
    patches.append(mpatches.Patch(color=LIFT_COLOR, label="Lift  (▲ top  ● bottom)"))
    patches.append(mpatches.Patch(color="#FFD700", label="Longest run"))
    leg = ax.legend(
        handles=patches, loc="lower right",
        facecolor="#1a1a24", edgecolor="#444455",
        labelcolor="white", fontsize=7, framealpha=0.85,
    )
    for patch in leg.get_patches():
        patch.set_linewidth(0)

    # ── title ─────────────────────────────────────────────────────────────────
    ax.set_title(
        f"{RESORT}\n1 m LiDAR Hillshade",
        color="white", fontsize=12, pad=8, fontweight="bold",
    )

    # ── save ──────────────────────────────────────────────────────────────────
    out_path = os.path.join(OUTPUT_DIR, "Ohau_Snow_Fields_lidar.png")
    fig.savefig(out_path, dpi=DPI, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"Saved -> {out_path}")


if __name__ == "__main__":
    main()
