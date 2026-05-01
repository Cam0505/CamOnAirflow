"""
Ōhau Snow Fields — 1m LiDAR DEM ski map
========================================
Renders the ski run network for Ōhau Snow Fields on top of a genuine 1m
LiDAR-derived hillshade + contours, using elevation data from the Canterbury
2020-2023 survey hosted on s3://nz-elevation (ingested to MotherDuck by
nz_elevation_lidar_dlt.py).

The run/lift styling intentionally matches ski_network_graph.py so the output
sits alongside the regular network graphs in charts/ski_network_graphs/NZ/.

Usage:
    python scripts/ohau_lidar_map.py
"""

import os
import math
import sys
import numpy as np
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["font.family"] = ["DejaVu Sans"]
import matplotlib.pyplot as plt                          # noqa: E402
import matplotlib.patheffects as pe                      # noqa: E402
import matplotlib.lines as mlines                        # noqa: E402
from matplotlib.colors import LightSource, Normalize     # noqa: E402
from scipy.ndimage import distance_transform_edt, gaussian_filter  # noqa: E402
from pyproj import Transformer                           # noqa: E402
import duckdb                                            # noqa: E402
from dotenv import load_dotenv                           # noqa: E402
from project_path import get_project_paths, set_dlt_env_vars  # noqa: E402

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

# ── style (matches ski_network_graph.py) ─────────────────────────────────────
BG_COLOR = "#0d0d12"
LIFT_COLOR = "#FFD700"

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
GLOW_LAYERS = [(6.0, 0.06), (3.0, 0.15), (1.4, 0.85)]
LIFT_GLOW_LAYERS = [(5.0, 0.08), (2.5, 0.20), (1.2, 0.90)]
HIKE_GLOW_LAYERS = [(4.0, 0.05), (2.0, 0.12), (0.9, 0.70)]
HIKE_DASHES = (5, 4)

# NZTM2000 (EPSG:2193) → WGS84 (EPSG:4326)
_NZTM_TO_WGS84 = Transformer.from_crs("EPSG:2193", "EPSG:4326", always_xy=True)


# ── geometry helpers (mirrors ski_network_graph.py) ───────────────────────────

def _safe_text(s):
    if not s:
        return s
    return str(s).replace("$", r"\$").replace("_", r"\_")


def _safe_color(difficulty):
    if difficulty is None or str(difficulty).lower() in ("nan", "", "none"):
        return DIFFICULTY_COLORS[None]
    return DIFFICULTY_COLORS.get(str(difficulty).lower(), DIFFICULTY_COLORS[None])


def _equirect(lons, lats, lon0, lat0):
    """Simple equirectangular projection centred on (lon0, lat0) → metres."""
    cos_lat = math.cos(math.radians(lat0))
    xs = (np.asarray(lons) - lon0) * cos_lat * 111_320
    ys = (np.asarray(lats) - lat0) * 111_320
    return xs, ys


def _rotate(xs, ys, angle):
    c, s = math.cos(angle), math.sin(angle)
    return c * xs - s * ys, s * xs + c * ys


def _compute_rotation(runs_meta_df, lon0, lat0):
    """Return angle (radians) so mean uphill direction points to +Y."""
    import pandas as pd
    vectors = []
    for _, row in runs_meta_df.iterrows():
        if any(pd.isna(row[c]) for c in ("top_lat", "top_lon", "bottom_lat", "bottom_lon")):
            continue
        tx, ty = _equirect([row["top_lon"]], [row["top_lat"]], lon0, lat0)
        bx, by = _equirect([row["bottom_lon"]], [row["bottom_lat"]], lon0, lat0)
        dx = float(bx[0]) - float(tx[0])
        dy = float(by[0]) - float(ty[0])
        length = math.hypot(dx, dy)
        if length > 5:
            vectors.append((dx / length, dy / length))
    if not vectors:
        return 0.0
    mean_dx = float(np.mean([v[0] for v in vectors]))
    mean_dy = float(np.mean([v[1] for v in vectors]))
    return -math.pi / 2 - math.atan2(mean_dy, mean_dx)


def _fill_nans_nearest(arr):
    """Replace NaN cells with the value of the nearest non-NaN cell."""
    nan_mask = np.isnan(arr)
    if not nan_mask.any():
        return arr.copy()
    _, nearest_idx = distance_transform_edt(nan_mask, return_indices=True)
    filled = arr.copy()
    filled[nan_mask] = arr[nearest_idx[0][nan_mask], nearest_idx[1][nan_mask]]
    return filled


# ── LiDAR hillshade ───────────────────────────────────────────────────────────

def _draw_lidar_hillshade(ax, dem_lons, dem_lats, dem_elevs,
                          lon0, lat0, rot, interval=50):
    """
    Project LiDAR points into rotated map space, bin into a grid,
    and render hillshade + contour lines.

    Args:
        dem_lons/dem_lats: WGS84 coordinates of LiDAR pixels (1-D arrays).
        dem_elevs:         Elevation in metres (1-D array, same length).
        lon0/lat0:         Projection centre (mean of run network).
        rot:               Rotation angle in radians.
        interval:          Contour interval in metres.
    """
    print(f"  Projecting {len(dem_lons):,} LiDAR points to map space …")
    xs, ys = _equirect(dem_lons, dem_lats, lon0, lat0)
    xs, ys = _rotate(xs, ys, rot)

    # ── bin into a regular grid ───────────────────────────────────────────────
    # 5 m bins: enough resolution to show meaningful hillshading while keeping
    # the array small (~200 × 300 for the Ōhau footprint ≈ 1 × 1.5 km bbox).
    BIN_M = 5.0
    x_min, x_max = float(xs.min()), float(xs.max())
    y_min, y_max = float(ys.min()), float(ys.max())
    nx = max(int((x_max - x_min) / BIN_M) + 1, 2)
    ny = max(int((y_max - y_min) / BIN_M) + 1, 2)
    print(f"  Gridding to {nx} × {ny} at {BIN_M:.0f} m resolution …")

    elev_sum, x_edges, y_edges = np.histogram2d(
        xs, ys,
        bins=[nx, ny],
        range=[[x_min, x_max], [y_min, y_max]],
        weights=dem_elevs,
    )
    count, _, _ = np.histogram2d(
        xs, ys,
        bins=[nx, ny],
        range=[[x_min, x_max], [y_min, y_max]],
    )
    with np.errstate(invalid="ignore"):
        # (nx, ny) — NaN where no LiDAR point fell in that cell
        grid = np.where(count > 0, elev_sum / count, np.nan)

    # imshow / contour expect (ny, nx) with origin='lower'
    grid_t = grid.T  # (ny, nx)
    grid_filled = _fill_nans_nearest(grid_t)

    z_min = float(np.nanmin(grid_t))
    z_max = float(np.nanmax(grid_t))
    dx_m = (x_max - x_min) / nx
    dy_m = (y_max - y_min) / ny
    extent = [x_min, x_max, y_min, y_max]

    # Smooth the DEM slightly — removes grid-bin pixelation before shading.
    grid_smooth = gaussian_filter(grid_filled, sigma=1.2)

    # ── hillshade — primary light from upper-left, low sun for long shadows ──
    ls = LightSource(azdeg=315, altdeg=22)
    shade_rgb = ls.shade(
        grid_smooth,
        cmap=matplotlib.colormaps["terrain"],
        norm=Normalize(vmin=z_min - (z_max - z_min) * 0.6, vmax=z_max),
        blend_mode="overlay",
        vert_exag=9.0,
        dx=dx_m,
        dy=dy_m,
    )
    ax.imshow(
        shade_rgb,
        extent=extent, origin="lower", aspect="auto",
        alpha=0.55, zorder=0, interpolation="bilinear",
    )

    # ── second pass: pure intensity hillshade to deepen shadow contrast ───────
    intensity = ls.hillshade(
        grid_smooth, vert_exag=9.0, dx=dx_m, dy=dy_m,
    )  # values in [0, 1]; 0 = fully shadowed
    shadow_rgba = np.zeros((*intensity.shape, 4), dtype=np.float32)
    shadow_rgba[..., 0] = 0.04
    shadow_rgba[..., 1] = 0.04
    shadow_rgba[..., 2] = 0.10
    shadow_rgba[..., 3] = np.clip((1.0 - intensity) * 0.55, 0.0, 1.0)
    ax.imshow(
        shadow_rgba,
        extent=extent, origin="lower", aspect="auto",
        zorder=1, interpolation="bilinear",
    )

    # ── contour lines — 25 m index + 50 m bold ────────────────────────────────
    gx_c = 0.5 * (x_edges[:-1] + x_edges[1:])   # (nx,)
    gy_c = 0.5 * (y_edges[:-1] + y_edges[1:])   # (ny,)
    gx2d, gy2d = np.meshgrid(gx_c, gy_c)         # both (ny, nx)

    minor_levels = np.arange(
        math.ceil(z_min / 25) * 25, z_max + 25, 25,
    )
    major_levels = np.arange(
        math.ceil(z_min / interval) * interval, z_max + interval, interval,
    )
    if len(minor_levels) >= 2:
        ax.contour(
            gx2d, gy2d, grid_t,
            levels=minor_levels,
            colors="white", linewidths=0.35, alpha=0.25, zorder=2,
        )
    if len(major_levels) >= 2:
        cs = ax.contour(
            gx2d, gy2d, grid_t,
            levels=major_levels,
            colors="white", linewidths=0.75, alpha=0.55, zorder=2,
        )
        ax.clabel(cs, fmt="%dm", fontsize=5.0, inline=True, inline_spacing=3)


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

    # LiDAR at 5 m (every 5th integer NZTM metre in both axes)
    print("Loading LiDAR data (5 m grid) …")
    lidar_df = con.execute("""
        SELECT x_nztm, y_nztm, elevation_m
        FROM camonairflow.nz_lidar.ski_resort_elevation_1m
        WHERE resort_name = ?
          AND x_nztm % 5 = 0
          AND y_nztm % 5 = 0
    """, [RESORT]).df()

    con.close()
    print(f"  {len(lidar_df):,} LiDAR points loaded")

    if lidar_df.empty:
        print("No LiDAR data found — run nz_elevation_lidar_dlt.py first.")
        sys.exit(1)

    # ── project LiDAR: NZTM → WGS84 ─────────────────────────────────────────
    dem_lons, dem_lats = _NZTM_TO_WGS84.transform(
        lidar_df["x_nztm"].values,
        lidar_df["y_nztm"].values,
    )

    # ── projection centre and uphill rotation ─────────────────────────────────
    lon0 = float(runs_df["lon"].mean())
    lat0 = float(runs_df["lat"].mean())
    rot = _compute_rotation(runs_meta_df, lon0, lat0)

    runs_df["x"], runs_df["y"] = _equirect(runs_df["lon"], runs_df["lat"], lon0, lat0)
    runs_df["x"], runs_df["y"] = _rotate(runs_df["x"].values, runs_df["y"].values, rot)

    # ── project lifts ─────────────────────────────────────────────────────────
    lifts_projected = []
    for _, lift in lifts_df.iterrows():
        bx, by = _equirect([lift["bottom_lon"]], [lift["bottom_lat"]], lon0, lat0)
        tx, ty = _equirect([lift["top_lon"]], [lift["top_lat"]], lon0, lat0)
        bxr, byr = _rotate(np.array([float(bx[0])]), np.array([float(by[0])]), rot)
        txr, tyr = _rotate(np.array([float(tx[0])]), np.array([float(ty[0])]), rot)
        lifts_projected.append({
            "name": lift["name"],
            "xs": [float(bxr[0]), float(txr[0])],
            "ys": [float(byr[0]), float(tyr[0])],
        })

    # ── figure size (match run-network aspect ratio) ──────────────────────────
    all_x = list(runs_df["x"]) + [v for lp in lifts_projected for v in lp["xs"]]
    all_y = list(runs_df["y"]) + [v for lp in lifts_projected for v in lp["ys"]]
    x_min, x_max = min(all_x), max(all_x)
    y_min, y_max = min(all_y), max(all_y)
    data_w = float(x_max - x_min) or 1.0
    data_h = float(y_max - y_min) or 1.0
    aspect = data_h / data_w
    BASE = 12.0
    fig_w = max(BASE / aspect, 6.0) if aspect >= 1.0 else BASE
    fig_h = BASE if aspect >= 1.0 else max(BASE * aspect, 6.0)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.set_aspect("equal")
    ax.axis("off")

    # ── LiDAR hillshade + contours (below runs) ───────────────────────────────
    _draw_lidar_hillshade(
        ax,
        dem_lons, dem_lats, lidar_df["elevation_m"].values.astype(float),
        lon0, lat0, rot,
        interval=50,
    )

    # ── draw runs ─────────────────────────────────────────────────────────────
    for osm_id, group in runs_df.groupby("osm_id", sort=False):
        group = group.sort_values("point_index")
        xs = group["x"].values
        ys = group["y"].values
        color = _safe_color(group["difficulty"].iloc[0])
        is_hike = str(group["piste_type"].iloc[0]).lower() == "hike"
        layers = HIKE_GLOW_LAYERS if is_hike else GLOW_LAYERS
        kwargs = (
            dict(linestyle="--", dashes=HIKE_DASHES, solid_capstyle="butt")
            if is_hike else dict(solid_capstyle="round", solid_joinstyle="round")
        )
        for lw, alpha in layers:
            ax.plot(xs, ys, color=color, lw=lw,
                    alpha=alpha * (0.60 if is_hike else 0.55),
                    zorder=2, **kwargs)

    # ── draw lifts ────────────────────────────────────────────────────────────
    for lp in lifts_projected:
        for lw, alpha in LIFT_GLOW_LAYERS:
            ax.plot(lp["xs"], lp["ys"],
                    color=LIFT_COLOR, lw=lw, alpha=alpha,
                    linestyle="--", dashes=(6, 4), solid_capstyle="butt")
        ax.annotate(
            "", xy=(lp["xs"][1], lp["ys"][1]), xytext=(lp["xs"][0], lp["ys"][0]),
            arrowprops=dict(arrowstyle="-|>", color=LIFT_COLOR,
                            lw=0.8, mutation_scale=12, alpha=0.8),
        )
        if lp["name"]:
            mx = (lp["xs"][0] + lp["xs"][1]) / 2
            my = (lp["ys"][0] + lp["ys"][1]) / 2
            ax.text(mx, my, _safe_text(lp["name"].title()),
                    color=LIFT_COLOR, fontsize=5.5, alpha=0.75,
                    ha="center", va="center",
                    path_effects=[pe.withStroke(linewidth=2, foreground=BG_COLOR)])

    # ── run name labels ───────────────────────────────────────────────────────
    LABEL_MIN_DIST_M = 120.0
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
            group["difficulty"].iloc[0],
        ))
    candidates.sort(key=lambda t: t[0], reverse=True)
    for _, mx, my, run_name, diff in candidates:
        if not run_name or not str(run_name).strip():
            continue
        if any(math.hypot(mx - px, my - py) < LABEL_MIN_DIST_M for px, py in placed):
            continue
        placed.append((mx, my))
        ax.text(mx, my, _safe_text(run_name),
                color=_safe_color(diff), fontsize=5, alpha=0.70,
                ha="center", va="center",
                path_effects=[pe.withStroke(linewidth=2, foreground=BG_COLOR)])

    # ── legend ────────────────────────────────────────────────────────────────
    legend_items = []
    for diff, color in DIFFICULTY_COLORS.items():
        if diff is None or str(diff) in ("nan", ""):
            continue
        if diff in runs_df["difficulty"].values:
            legend_items.append(
                mlines.Line2D([0], [0], color=color, lw=2, label=diff.capitalize()))
    legend_items += [
        mlines.Line2D([0], [0], color=LIFT_COLOR, lw=1.5,
                      linestyle="--", label="Lift"),
        mlines.Line2D([0], [0], color="white", lw=0.6,
                      alpha=0.45, label="Contours (25/50 m)"),
        mlines.Line2D([0], [0], color="#aaccff", lw=0,
                      marker="s", markersize=6, alpha=0.55,
                      label="1 m LiDAR hillshade\n(Canterbury 2020–23)"),
    ]
    ax.legend(handles=legend_items, loc="lower right",
              facecolor="#1a1a24", edgecolor="#444",
              labelcolor="white", fontsize=7, framealpha=0.8)

    # ── title ─────────────────────────────────────────────────────────────────
    ax.set_title(
        _safe_text(RESORT) + "\n1 m LiDAR Terrain",
        color="white", fontsize=16, fontweight="bold", pad=12,
        path_effects=[pe.withStroke(linewidth=4, foreground=BG_COLOR)],
    )

    pad_x = data_w * 0.12
    pad_y = data_h * 0.12
    ax.set_xlim(x_min - pad_x, x_max + pad_x)
    ax.set_ylim(y_min - pad_y, y_max + pad_y)

    out_path = os.path.join(OUTPUT_DIR, "Ohau_Snow_Fields_lidar.png")
    fig.savefig(out_path, dpi=200, bbox_inches="tight",
                facecolor=BG_COLOR, edgecolor="none")
    plt.close(fig)
    print(f"Saved → {out_path}")


if __name__ == "__main__":
    main()
