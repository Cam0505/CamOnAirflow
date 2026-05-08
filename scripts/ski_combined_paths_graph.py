"""
Ski resort combined steepest + longest path visualiser.

Draws each resort's run polylines coloured by difficulty, then overlays both
pre-computed paths with distinct glow colours:

    Steepest path only  — lavender white  (#DDD0FF)
    Longest path only   — amber glow      (#FFC44D)
    Both paths overlap  — champagne       (#F5E6B0)

The core of each highlighted segment retains its difficulty colour so the
terrain character of the route remains visible underneath the glow.

Usage:
    python scripts/ski_combined_paths_graph.py                   # all resorts
    python scripts/ski_combined_paths_graph.py "Treble Cone Ski Area"
    python scripts/ski_combined_paths_graph.py "Treble Cone Ski Area" "Whakapapa Ski Area"
"""

import os
import sys
import math
import json
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["font.family"] = ["DejaVu Sans", "IPAexGothic", "AR PL UMing CN"]
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.patheffects as pe  # noqa: E402
import matplotlib.lines as mlines  # noqa: E402
from matplotlib.colors import LightSource, Normalize  # noqa: E402
import duckdb  # noqa: E402
try:
    from scipy.interpolate import griddata as _scipy_griddata
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False
from dotenv import load_dotenv  # noqa: E402
from project_path import get_project_paths, set_dlt_env_vars  # noqa: E402

# ── env ──────────────────────────────────────────────────────────────────────
paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")

OUTPUT_DIR = os.path.join(paths["PROJECT_ROOT"], "charts", "ski_combined_paths_graphs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── style ─────────────────────────────────────────────────────────────────────
BG_COLOR = "#0d0d12"
LIFT_COLOR = "#FFD700"

# Path highlight colours
STEEPEST_COLOR = "#FFFFFF"  # bright white
LONGEST_COLOR = "#FF00FF"  # vivid magenta
OVERLAP_COLOR = "#58C251"  # neon green

DIFFICULTY_COLORS = {
    "novice": "#4CAF50",
    "easy": "#42A5F5",
    "intermediate": "#D82741",
    "advanced": "#8C32DA",
    "expert": "#6318A0",
    "freeride": "#00BCD4",
    None: "#90A4AE",
    "nan": "#90A4AE",
    "": "#90A4AE",
}

GLOW_LAYERS = [
    (6.0, 0.06),
    (3.0, 0.15),
    (1.4, 0.85),
]
LIFT_GLOW_LAYERS = [
    (5.0, 0.08),
    (2.5, 0.20),
    (1.2, 0.90),
]
HIKE_GLOW_LAYERS = [
    (4.0, 0.05),
    (2.0, 0.12),
    (0.9, 0.70),
]
HIKE_DASHES = (5, 4)

# (linewidth, alpha, colour) — wide halo then soft glow
def _highlight_layers(color):
    return [
        (9.0, 0.10, color),
        (5.0, 0.24, color),
    ]


# ── helpers ───────────────────────────────────────────────────────────────────

def _safe_text(s):
    if not s:
        return s
    return str(s).replace("$", r"\$").replace("_", r"\_")


def _safe_color(difficulty):
    if difficulty is None or str(difficulty).lower() in ("nan", "", "none"):
        return DIFFICULTY_COLORS[None]
    return DIFFICULTY_COLORS.get(str(difficulty).lower(), DIFFICULTY_COLORS[None])


def _equirect(lons, lats, lon0, lat0):
    cos_lat = math.cos(math.radians(lat0))
    xs = (np.asarray(lons) - lon0) * cos_lat * 111_320
    ys = (np.asarray(lats) - lat0) * 111_320
    return xs, ys


def _compute_rotation(runs_meta_df, lon0: float, lat0: float) -> float:
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


def _rotate(xs, ys, angle: float):
    c, s = math.cos(angle), math.sin(angle)
    return c * xs - s * ys, s * xs + c * ys


def _draw_topo(ax, xs, ys, zs, interval: int = 50):
    if not _HAS_SCIPY:
        return
    valid = ~(np.isnan(xs) | np.isnan(ys) | np.isnan(zs))
    if valid.sum() < 30:
        return
    xv, yv, zv = xs[valid], ys[valid], zs[valid]
    z_min, z_max = float(zv.min()), float(zv.max())
    if z_max - z_min < interval:
        return
    pad_x = max((xv.max() - xv.min()) * 0.05, 10)
    pad_y = max((yv.max() - yv.min()) * 0.05, 10)
    gx, gy = np.mgrid[
        xv.min() - pad_x : xv.max() + pad_x : 200j,
        yv.min() - pad_y : yv.max() + pad_y : 200j,
    ]
    gz = _scipy_griddata((xv, yv), zv, (gx, gy), method="linear")
    extent = [float(gx.min()), float(gx.max()), float(gy.min()), float(gy.max())]
    gz_t = gz.T
    gz_mean = float(np.nanmean(gz_t)) if not np.all(np.isnan(gz_t)) else 0.0
    gz_filled = np.where(np.isnan(gz_t), gz_mean, gz_t)
    dx_m = (float(gx.max()) - float(gx.min())) / 199.0
    dy_m = (float(gy.max()) - float(gy.min())) / 199.0
    ls = LightSource(azdeg=315, altdeg=40)
    shade_rgb = ls.shade(
        gz_filled,
        cmap=matplotlib.colormaps["terrain"],
        norm=Normalize(vmin=z_min - (z_max - z_min) * 0.5, vmax=z_max),
        blend_mode="soft", vert_exag=3.0, dx=dx_m, dy=dy_m,
    )
    ax.imshow(shade_rgb, extent=extent, origin="lower", aspect="auto",
              alpha=0.06, zorder=0, interpolation="bilinear")
    levels = np.arange(math.ceil(z_min / interval) * interval, z_max + interval, interval)
    if len(levels) < 2:
        return
    cs = ax.contour(gx, gy, gz, levels=levels,
                    colors="white", linewidths=0.6, alpha=0.30, zorder=1)
    ax.clabel(cs, fmt="%dm", fontsize=4.5, inline=True, inline_spacing=2)


def _parse_node_ids(node_ids_json: str) -> dict:
    """Parse JSON node_ids into {run_osm_id (int): set of segment_indices (int)}."""
    try:
        node_ids = json.loads(node_ids_json) if node_ids_json else []
    except (ValueError, TypeError):
        return {}
    result: dict = {}
    for node_id in node_ids:
        parts = str(node_id).split("_")
        result.setdefault(int(parts[0]), set()).add(int(parts[1]))
    return result


def _segment_ranges(seg_indices: set):
    """Yield (start_pt, end_pt) point-index ranges for consecutive segment groups."""
    if not seg_indices:
        return
    sorted_segs = sorted(seg_indices)
    start = prev = sorted_segs[0]
    for idx in sorted_segs[1:]:
        if idx != prev + 1:
            yield (start, prev + 1)
            start = idx
        prev = idx
    yield (start, prev + 1)


# ── data loading ──────────────────────────────────────────────────────────────

def _load_steepest_paths(con) -> dict:
    rows = con.execute("""
        SELECT resort, node_ids, total_distance_m, avg_gradient_pct, run_path
        FROM camonairflow.public_common.mart_steepest_ski_paths
    """).fetchall()
    result = {}
    for resort, node_ids_json, dist_m, gradient_pct, run_path in rows:
        result[resort] = {
            "segs": _parse_node_ids(node_ids_json),
            "dist_km": float(dist_m or 0) / 1000,
            "gradient_deg": math.degrees(math.atan(float(gradient_pct or 0) / 100.0)),
            "run_path": str(run_path or ""),
        }
    return result


def _load_longest_paths(con) -> dict:
    rows = con.execute("""
        SELECT resort, node_ids, total_distance_m, run_path
        FROM camonairflow.public_common.mart_longest_ski_paths
    """).fetchall()
    result = {}
    for resort, node_ids_json, dist_m, run_path in rows:
        result[resort] = {
            "segs": _parse_node_ids(node_ids_json),
            "dist_km": float(dist_m or 0) / 1000,
            "run_path": str(run_path or ""),
        }
    return result


# ── highlight drawing helper ──────────────────────────────────────────────────

def _draw_highlight(ax, pxs, pys, glow_color, diff_color, is_hike, zorder=3, alpha_scale=1.0):
    """Draw a path-highlight: coloured glow halo behind the run, then a solid core."""
    # Halo at zorder-1 so the base-run line (zorder=2) covers the centre
    for lw, alpha, c in _highlight_layers(glow_color):
        alpha = min(alpha * alpha_scale, 1.0)
        if is_hike:
            ax.plot(pxs, pys, color=c, lw=lw, alpha=alpha,
                    linestyle="--", dashes=HIKE_DASHES,
                    solid_capstyle="butt", zorder=zorder - 1)
        else:
            ax.plot(pxs, pys, color=c, lw=lw, alpha=alpha,
                    solid_capstyle="round", solid_joinstyle="round", zorder=zorder - 1)
    # Solid opaque difficulty core on top — same width/style as the base run
    lw_core = 0.9 if is_hike else 1.4
    if is_hike:
        ax.plot(pxs, pys, color=diff_color, lw=lw_core, alpha=1.0,
                linestyle="--", dashes=HIKE_DASHES,
                solid_capstyle="butt", zorder=zorder)
    else:
        ax.plot(pxs, pys, color=diff_color, lw=lw_core, alpha=1.0,
                solid_capstyle="round", solid_joinstyle="round", zorder=zorder)


# ── resort renderer ───────────────────────────────────────────────────────────

def render_resort(
    con,
    resort_name: str,
    country_code: str = "",
    steepest_info: dict | None = None,
    longest_info: dict | None = None,
):
    steepest_info = steepest_info or {"segs": {}, "dist_km": 0.0, "gradient_deg": 0.0, "run_path": ""}
    longest_info = longest_info or {"segs": {}, "dist_km": 0.0, "run_path": ""}

    # ── 1. run points ─────────────────────────────────────────────────────────
    runs_df = con.execute("""
        SELECT
            p.osm_id,
            p.lat, p.lon,
            p.point_index,
            p.elevation_smoothed_m,
            r.difficulty,
            r.run_name,
            r.piste_type
        FROM camonairflow.ski_runs.ski_run_points p
        JOIN camonairflow.ski_runs.ski_runs r ON p.osm_id = r.osm_id
        WHERE p.resort = ?
          AND r.piste_type IN ('downhill', 'hike')
        ORDER BY p.osm_id, p.point_index
    """, [resort_name]).df()

    if runs_df.empty:
        print(f"  No run points found for {resort_name!r} — skipping.")
        return

    # ── 2. lifts ──────────────────────────────────────────────────────────────
    lifts_df = con.execute("""
        SELECT name, lift_type, top_lat, top_lon, bottom_lat, bottom_lon
        FROM camonairflow.ski_runs.ski_lifts
        WHERE resort = ?
    """, [resort_name]).df()

    # ── 3. run top/bottom coords for rotation ────────────────────────────────
    runs_meta_df = con.execute("""
        SELECT top_lat, top_lon, bottom_lat, bottom_lon
        FROM camonairflow.ski_runs.ski_runs
        WHERE resort = ?
    """, [resort_name]).df()

    # ── 4. project + rotate ───────────────────────────────────────────────────
    lon0 = runs_df["lon"].mean()
    lat0 = runs_df["lat"].mean()
    runs_df["x"], runs_df["y"] = _equirect(runs_df["lon"], runs_df["lat"], lon0, lat0)
    rot = _compute_rotation(runs_meta_df, lon0, lat0)
    runs_df["x"], runs_df["y"] = _rotate(runs_df["x"].values, runs_df["y"].values, rot)

    # ── 5. project lifts ──────────────────────────────────────────────────────
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

    # ── 6. figure sizing ──────────────────────────────────────────────────────
    all_pts_x = list(runs_df["x"].values)
    all_pts_y = list(runs_df["y"].values)
    for lp in lifts_projected:
        all_pts_x.extend(lp["xs"])
        all_pts_y.extend(lp["ys"])
    x_min, x_max = min(all_pts_x), max(all_pts_x)
    y_min, y_max = min(all_pts_y), max(all_pts_y)
    data_w = float(x_max - x_min) or 1.0
    data_h = float(y_max - y_min) or 1.0
    aspect_ratio = data_h / data_w
    BASE = 10.0
    fig_w = max(BASE / aspect_ratio, 5.0) if aspect_ratio >= 1.0 else BASE
    fig_h = BASE if aspect_ratio >= 1.0 else max(BASE * aspect_ratio, 5.0)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.set_aspect("equal")
    ax.axis("off")

    # ── 7. topo ───────────────────────────────────────────────────────────────
    elev = runs_df["elevation_smoothed_m"].values.astype(float)
    _draw_topo(ax, runs_df["x"].values, runs_df["y"].values, elev)

    steepest_segs = steepest_info["segs"]
    longest_segs = longest_info["segs"]

    # ── 8. runs ───────────────────────────────────────────────────────────────
    for osm_id, group in runs_df.groupby("osm_id", sort=False):
        group = group.sort_values("point_index")
        xs = group["x"].values
        ys = group["y"].values
        pt_idx = group["point_index"].values
        diff = group["difficulty"].iloc[0]
        color = _safe_color(diff)
        is_hike = str(group["piste_type"].iloc[0]).lower() == "hike"
        run_id = int(osm_id)

        # Base run (dimmed)
        if is_hike:
            for lw, alpha in HIKE_GLOW_LAYERS:
                ax.plot(xs, ys, color=color, lw=lw, alpha=alpha * 0.55,
                        linestyle="--", dashes=HIKE_DASHES,
                        solid_capstyle="butt", zorder=2)
            elev_arr = group["elevation_smoothed_m"].values.astype(float)
            mid = max(len(xs) // 2, 1)
            e0 = elev_arr[0] if not np.isnan(elev_arr[0]) else 0.0
            e_last = elev_arr[-1] if not np.isnan(elev_arr[-1]) else 0.0
            ax0, ay0, ax1, ay1 = (
                (xs[mid - 1], ys[mid - 1], xs[mid], ys[mid])
                if e_last > e0
                else (xs[mid], ys[mid], xs[mid - 1], ys[mid - 1])
            )
            ax.annotate("", xy=(ax1, ay1), xytext=(ax0, ay0),
                        arrowprops=dict(arrowstyle="-|>", color=color,
                                        lw=0.7, mutation_scale=7, alpha=0.80),
                        zorder=3)
        else:
            for lw, alpha in GLOW_LAYERS:
                ax.plot(xs, ys, color=color, lw=lw, alpha=alpha * 0.50,
                        solid_capstyle="round", solid_joinstyle="round", zorder=2)

        # Determine which segments of this run are on which path
        s_segs = steepest_segs.get(run_id, set())
        l_segs = longest_segs.get(run_id, set())
        overlap_only = s_segs & l_segs
        steepest_only = s_segs - overlap_only
        longest_only = l_segs - overlap_only

        # Draw in order: steepest → longest → overlap (overlap renders on top)
        for seg_set, glow_color, a_scale in (
            (steepest_only, STEEPEST_COLOR, 1.0),
            (longest_only, LONGEST_COLOR, 1.0),
            (overlap_only, OVERLAP_COLOR, 3.2),
        ):
            for seg_start, seg_end in _segment_ranges(seg_set):
                mask = (pt_idx >= seg_start) & (pt_idx <= seg_end)
                pxs, pys = xs[mask], ys[mask]
                if len(pxs) < 2:
                    continue
                _draw_highlight(ax, pxs, pys, glow_color, color, is_hike, zorder=3, alpha_scale=a_scale)
                # For hike segments on a path, add a direction arrow in the glow colour
                if is_hike:
                    hmid = max(len(pxs) // 2, 1)
                    ax.annotate("", xy=(pxs[hmid], pys[hmid]),
                                xytext=(pxs[hmid - 1], pys[hmid - 1]),
                                arrowprops=dict(arrowstyle="-|>", color=glow_color,
                                                lw=0.9, mutation_scale=9, alpha=0.95),
                                zorder=4)

    # ── 9. lifts ──────────────────────────────────────────────────────────────
    for lp in lifts_projected:
        xs, ys = lp["xs"], lp["ys"]
        for lw, alpha in LIFT_GLOW_LAYERS:
            ax.plot(xs, ys, color=LIFT_COLOR, lw=lw, alpha=alpha,
                    linestyle="--", dashes=(6, 4), solid_capstyle="butt")
        ax.annotate("", xy=(xs[1], ys[1]), xytext=(xs[0], ys[0]),
                    arrowprops=dict(arrowstyle="-|>", color=LIFT_COLOR,
                                    lw=0.8, mutation_scale=12, alpha=0.8))
        if lp["name"]:
            mx, my = (xs[0] + xs[1]) / 2, (ys[0] + ys[1]) / 2
            ax.text(mx, my, _safe_text(lp["name"].title()),
                    color=LIFT_COLOR, fontsize=5.5, alpha=0.75,
                    ha="center", va="center",
                    path_effects=[pe.withStroke(linewidth=2, foreground=BG_COLOR)])

    # ── 10. run name labels ────────────────────────────────────────────────────
    LABEL_MIN_DIST_M = 120.0
    placed: list[tuple[float, float]] = []
    candidates = []
    for osm_id, group in runs_df.groupby("osm_id", sort=False):
        group = group.sort_values("point_index")
        mid = len(group) // 2
        candidates.append((
            float(len(group)),
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

    # ── 11. legend ────────────────────────────────────────────────────────────
    legend_items = []
    for diff, color in DIFFICULTY_COLORS.items():
        if diff is None or str(diff) in ("nan", ""):
            continue
        if diff in runs_df["difficulty"].values:
            legend_items.append(
                mlines.Line2D([0], [0], color=color, lw=2, label=diff.capitalize())
            )
    legend_items.append(
        mlines.Line2D([0], [0], color=LIFT_COLOR, lw=1.5, linestyle="--", label="Lift")
    )
    if not runs_df.empty and (runs_df["piste_type"] == "hike").any():
        legend_items.append(
            mlines.Line2D([0], [0], color="#90A4AE", lw=1.2,
                          linestyle="--", dashes=HIKE_DASHES, label="Bootpack (hike)")
        )
    legend_items.append(
        mlines.Line2D([0], [0], color="white", lw=0.6, alpha=0.30, label="Contours (50m)")
    )
    if steepest_info["gradient_deg"] > 0:
        legend_items.append(
            mlines.Line2D([0], [0], color=STEEPEST_COLOR, lw=2.2, alpha=0.95,
                          label=f"Steepest ({steepest_info['gradient_deg']:.1f}° avg, "
                                f"{steepest_info['dist_km']:.1f} km)")
        )
    if longest_info["dist_km"] > 0:
        legend_items.append(
            mlines.Line2D([0], [0], color=LONGEST_COLOR, lw=2.2, alpha=0.95,
                          label=f"Longest ({longest_info['dist_km']:.1f} km)")
        )
    # Only show overlap entry if the paths actually share segments at this resort
    any_overlap = any(
        bool(steepest_segs.get(rid, set()) & longest_segs.get(rid, set()))
        for rid in set(steepest_segs) | set(longest_segs)
    )
    if any_overlap:
        legend_items.append(
            mlines.Line2D([0], [0], color=OVERLAP_COLOR, lw=2.2, alpha=0.95,
                          label="Shared (steepest + longest)")
        )
    if legend_items:
        ax.legend(handles=legend_items, loc="lower right",
                  facecolor="#1a1a24", edgecolor="#444",
                  labelcolor="white", fontsize=7, framealpha=0.8)

    # ── 12. title ─────────────────────────────────────────────────────────────
    ax.set_title(_safe_text(resort_name),
                 color="white", fontsize=18, fontweight="bold", pad=12,
                 path_effects=[pe.withStroke(linewidth=4, foreground=BG_COLOR)])

    # ── 13. view limits ───────────────────────────────────────────────────────
    pad_x = data_w * 0.12
    pad_y = data_h * 0.12
    ax.set_xlim(x_min - pad_x, x_max + pad_x)
    ax.set_ylim(y_min - pad_y, y_max + pad_y)

    # ── 14. save ──────────────────────────────────────────────────────────────
    safe_name = resort_name.replace("/", "_").replace(" ", "_")
    country_dir = os.path.join(OUTPUT_DIR, country_code) if country_code else OUTPUT_DIR
    os.makedirs(country_dir, exist_ok=True)
    out_path = os.path.join(country_dir, f"{safe_name}.png")
    fig.savefig(out_path, dpi=180, bbox_inches="tight",
                facecolor=BG_COLOR, edgecolor="none")
    plt.close(fig)
    print(f"  Saved → {out_path}")


def main():
    resort_args = sys.argv[1:]
    con = duckdb.connect(database_string)  # type: ignore

    if resort_args:
        placeholders = ", ".join(["?"] * len(resort_args))
        rows = con.execute(f"""
            SELECT DISTINCT resort, country_code
            FROM camonairflow.ski_runs.ski_run_points
            WHERE resort IN ({placeholders})
            ORDER BY resort
        """, resort_args).fetchall()
    else:
        rows = con.execute("""
            SELECT DISTINCT resort, country_code
            FROM camonairflow.ski_runs.ski_run_points
            ORDER BY country_code, resort
        """).fetchall()

    print(f"Rendering {len(rows)} resort(s)…")
    all_steepest = _load_steepest_paths(con)
    all_longest = _load_longest_paths(con)

    for resort, country_code in rows:
        print(f"  [{country_code}] {resort!r}…")
        render_resort(
            con, resort, country_code,
            steepest_info=all_steepest.get(resort),
            longest_info=all_longest.get(resort),
        )

    con.close()
    print("Done.")


if __name__ == "__main__":
    main()
