"""
Ski resort network graph visualiser.

Draws each resort's run polylines coloured by difficulty on a dark background,
with lifts shown as distinct dashed lines.  Produces one PNG per resort.

Usage:
    python scripts/ski_network_graph.py                   # all NZ resorts
    python scripts/ski_network_graph.py "Treble Cone Ski Area"
    python scripts/ski_network_graph.py "Treble Cone Ski Area" "Whakapapa Ski Area"
"""

import os
import sys
import math
import numpy as np
import matplotlib
matplotlib.use("Agg")
# Add CJK fallback fonts so Japanese/Chinese run names render without warnings
matplotlib.rcParams["font.family"] = ["DejaVu Sans", "IPAexGothic", "AR PL UMing CN"]
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.patheffects as pe  # noqa: E402
import matplotlib.lines as mlines  # noqa: E402
import duckdb  # noqa: E402
try:
    from scipy.interpolate import griddata as _scipy_griddata
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False
from dotenv import load_dotenv  # noqa: E402
from project_path import get_project_paths, set_dlt_env_vars  # noqa: E402

# ── env ─────────────────────────────────────────────────────────────────────
paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")


OUTPUT_DIR = os.path.join(paths["PROJECT_ROOT"], "charts", "ski_network_graphs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── style ────────────────────────────────────────────────────────────────────
BG_COLOR = "#0d0d12"
LIFT_COLOR = "#FFD700"      # gold

DIFFICULTY_COLORS = {
    "novice": "#4CAF50",         # green
    "easy": "#42A5F5",           # blue
    "intermediate": "#EF5350",   # red
    "advanced": "#FF9800",       # orange
    "expert": "#E040FB",         # purple
    "freeride": "#00BCD4",       # cyan
    None: "#90A4AE",             # grey
    "nan": "#90A4AE",
    "": "#90A4AE",
}

# line widths (glow = fattest+faintest, core = thinnest+brightest)
GLOW_LAYERS = [
    (6.0, 0.06),   # wide halo
    (3.0, 0.15),   # soft glow
    (1.4, 0.85),   # core line
]
LIFT_GLOW_LAYERS = [
    (5.0, 0.08),
    (2.5, 0.20),
    (1.2, 0.90),
]


def _safe_text(s):
    """Escape characters that matplotlib mathtext would misparse."""
    if not s:
        return s
    return str(s).replace("$", r"\$").replace("_", r"\_")


def _safe_color(difficulty):
    if difficulty is None or str(difficulty).lower() in ("nan", "", "none"):
        return DIFFICULTY_COLORS[None]
    return DIFFICULTY_COLORS.get(str(difficulty).lower(), DIFFICULTY_COLORS[None])


def _equirect(lons, lats, lon0, lat0):
    """Simple equirectangular projection centred on (lon0, lat0)."""
    cos_lat = math.cos(math.radians(lat0))
    xs = (np.asarray(lons) - lon0) * cos_lat * 111_320
    ys = (np.asarray(lats) - lat0) * 111_320
    return xs, ys


def _compute_rotation(runs_meta_df, lon0: float, lat0: float) -> float:
    """Return rotation angle (radians) so the mean uphill direction points to +Y (top of image)."""
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
    # Rotate so downhill → -Y (bottom of image), uphill → +Y (top of image)
    return -math.pi / 2 - math.atan2(mean_dy, mean_dx)


def _rotate(xs, ys, angle: float):
    """Rotate coordinate arrays by angle radians around the origin."""
    c, s = math.cos(angle), math.sin(angle)
    return c * xs - s * ys, s * xs + c * ys


def _draw_topo(ax, xs, ys, zs, interval: int = 100):
    """Interpolate a grid from scattered run-point elevations and draw faint contour lines."""
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
    levels = np.arange(
        math.ceil(z_min / interval) * interval,
        z_max + interval,
        interval,
    )
    if len(levels) < 2:
        return
    cs = ax.contour(gx, gy, gz, levels=levels,
                    colors="white", linewidths=0.6, alpha=0.30, zorder=1)
    ax.clabel(cs, fmt="%dm", fontsize=4.5, inline=True, inline_spacing=2)


def _parse_node_ids(node_ids_json: str) -> dict:
    """Parse JSON node_ids string into {run_osm_id (int): set_of_segment_indices (int)}."""
    import json
    try:
        node_ids = json.loads(node_ids_json) if node_ids_json else []
    except (ValueError, TypeError):
        return {}
    path_segs: dict = {}
    for node_id in node_ids:
        parts = str(node_id).split("_")
        run_id = int(parts[0])
        seg_idx = int(parts[1])
        path_segs.setdefault(run_id, set()).add(seg_idx)
    return path_segs


def _consecutive_ranges(seg_indices: set):
    """Yield (start, end) inclusive ranges from a set of integer segment indices."""
    for idx in sorted(seg_indices):
        if '_start' not in _consecutive_ranges.__dict__:
            _consecutive_ranges._start = idx
            _consecutive_ranges._prev = idx
        elif idx != _consecutive_ranges._prev + 1:
            yield (_consecutive_ranges._start, _consecutive_ranges._prev)
            _consecutive_ranges._start = idx
            _consecutive_ranges._prev = idx
        else:
            _consecutive_ranges._prev = idx
    if '_start' in _consecutive_ranges.__dict__:
        yield (_consecutive_ranges._start, _consecutive_ranges._prev)
        del _consecutive_ranges._start, _consecutive_ranges._prev


def _segment_ranges(seg_indices: set):
    """Yield (start_pt, end_pt) point-index ranges for a set of segment indices.
    Segment N connects point N to point N+1, so a consecutive run of segments
    [N, N+1, N+2] maps to points [N, N+1, N+2, N+3].
    """
    if not seg_indices:
        return
    sorted_segs = sorted(seg_indices)
    start = sorted_segs[0]
    prev = sorted_segs[0]
    for idx in sorted_segs[1:]:
        if idx != prev + 1:
            yield (start, prev + 1)  # point indices: seg_start to seg_end+1 inclusive
            start = idx
        prev = idx
    yield (start, prev + 1)


def _load_all_longest_paths(con) -> dict:
    """Load pre-computed longest paths for all resorts from the mart.

    Returns:
        dict mapping resort name -> (path_segments_dict, distance_m, description)
        where path_segments_dict is {run_osm_id (int): set_of_segment_indices (int)}
    """
    rows = con.execute("""
        SELECT resort, node_ids, total_distance_m, run_path
        FROM camonairflow.public_common.mart_longest_ski_paths
    """).fetchall()
    result = {}
    for resort, node_ids_json, dist_m, run_path in rows:
        path_segs = _parse_node_ids(node_ids_json)
        result[resort] = (path_segs, float(dist_m or 0), str(run_path or ""))
    return result


def render_resort(con, resort_name: str, country_code: str = "",
                  path_info: tuple = (set(), 0.0, "")):
    # ── 1. run polylines ──────────────────────────────────────────────────────
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
        and r.piste_type = 'downhill'
        ORDER BY p.osm_id, p.point_index
    """, [resort_name]).df()

    if runs_df.empty:
        print(f"  No run points found for {resort_name!r} — skipping.")
        return

    # ── 2. lifts ─────────────────────────────────────────────────────────────
    lifts_df = con.execute("""
        SELECT name, lift_type,
               top_lat, top_lon, bottom_lat, bottom_lon
        FROM camonairflow.ski_runs.ski_lifts
        WHERE resort = ?
    """, [resort_name]).df()

    # ── 3. run top/bottom coords (for uphill rotation) ────────────────────────
    runs_meta_df = con.execute("""
        SELECT top_lat, top_lon, bottom_lat, bottom_lon
        FROM camonairflow.ski_runs.ski_runs
        WHERE resort = ?
    """, [resort_name]).df()

    # ── 4. project to metres ─────────────────────────────────────────────────
    lon0 = runs_df["lon"].mean()
    lat0 = runs_df["lat"].mean()
    runs_df["x"], runs_df["y"] = _equirect(runs_df["lon"], runs_df["lat"], lon0, lat0)

    # ── 5. compute & apply uphill rotation ───────────────────────────────────
    rot = _compute_rotation(runs_meta_df, lon0, lat0)
    runs_df["x"], runs_df["y"] = _rotate(runs_df["x"].values, runs_df["y"].values, rot)

    # ── 5b. pre-project lift coordinates (needed for bounding box & drawing) ─
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

    # ── 5c. size figure to match actual data aspect ratio ────────────────────
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
    if aspect_ratio >= 1.0:  # portrait data
        fig_w = max(BASE / aspect_ratio, 5.0)
        fig_h = BASE
    else:  # landscape data
        fig_w = BASE
        fig_h = max(BASE * aspect_ratio, 5.0)

    # ── 6. longest path (pre-loaded from mart_longest_ski_paths) ─────────────
    # path_segments: {run_osm_id (int): set of segment_indices (int)}
    path_segments, path_dist_m, path_desc = path_info
    path_dist_km = path_dist_m / 1000
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.set_aspect("equal")
    ax.axis("off")

    # ── 8. topo contour lines (below runs) ───────────────────────────────────
    elev = runs_df["elevation_smoothed_m"].values.astype(float)
    _draw_topo(ax, runs_df["x"].values, runs_df["y"].values, elev)

    # ── 9. draw runs — path segments highlighted, rest dimmed ────────────────
    HIGHLIGHT_LAYERS = [
        (9.0, 0.08, "white"),   # wide white halo
        (5.0, 0.18, "white"),   # soft white glow
    ]

    for osm_id, group in runs_df.groupby("osm_id", sort=False):
        group = group.sort_values("point_index")
        xs = group["x"].values
        ys = group["y"].values
        pt_idx = group["point_index"].values
        diff = group["difficulty"].iloc[0]
        color = _safe_color(diff)

        touched_segs = path_segments.get(int(osm_id))

        # Every run drawn dimmed as background
        for lw, alpha in GLOW_LAYERS:
            ax.plot(xs, ys, color=color, lw=lw, alpha=alpha * 0.55,
                    solid_capstyle="round", solid_joinstyle="round", zorder=2)

        if touched_segs:
            # Overlay only the specific segment ranges that are on the path
            # _segment_ranges yields (start_pt, end_pt) as point indices already
            for seg_start, seg_end in _segment_ranges(touched_segs):
                mask = (pt_idx >= seg_start) & (pt_idx <= seg_end)
                pxs, pys = xs[mask], ys[mask]
                if len(pxs) < 2:
                    continue
                for lw, alpha, c in HIGHLIGHT_LAYERS:
                    ax.plot(pxs, pys, color=c, lw=lw, alpha=alpha,
                            solid_capstyle="round", solid_joinstyle="round", zorder=3)
                for lw, alpha in GLOW_LAYERS:
                    ax.plot(pxs, pys, color=color, lw=lw, alpha=alpha,
                            solid_capstyle="round", solid_joinstyle="round", zorder=3)

    # ── 10. draw lifts ─────────────────────────────────────────────────────────
    for lp in lifts_projected:
        xs = lp["xs"]
        ys = lp["ys"]

        for lw, alpha in LIFT_GLOW_LAYERS:
            ax.plot(xs, ys,
                    color=LIFT_COLOR, lw=lw, alpha=alpha,
                    linestyle="--", dashes=(6, 4),
                    solid_capstyle="butt")

        # arrowhead at top
        ax.annotate(
            "", xy=(xs[1], ys[1]), xytext=(xs[0], ys[0]),
            arrowprops=dict(
                arrowstyle="-|>",
                color=LIFT_COLOR,
                lw=0.8,
                mutation_scale=12,
                alpha=0.8,
            ),
        )

        # lift name label
        if lp["name"]:
            mx = (xs[0] + xs[1]) / 2
            my = (ys[0] + ys[1]) / 2
            ax.text(
                mx, my, _safe_text(lp["name"].title()),
                color=LIFT_COLOR, fontsize=5.5, alpha=0.75,
                ha="center", va="center", rotation=0,
                path_effects=[pe.withStroke(linewidth=2, foreground=BG_COLOR)],
            )

    # ── 11. run name labels (midpoint of each run) ────────────────────────────
    for osm_id, group in runs_df.groupby("osm_id", sort=False):
        group = group.sort_values("point_index")
        mid = len(group) // 2
        mx = group["x"].iloc[mid]
        my = group["y"].iloc[mid]
        run_name = group["run_name"].iloc[0]
        diff = group["difficulty"].iloc[0]
        color = _safe_color(diff)

        if run_name and str(run_name).strip():
            ax.text(
                mx, my, _safe_text(run_name),
                color=color, fontsize=5, alpha=0.70,
                ha="center", va="center",
                path_effects=[pe.withStroke(linewidth=2, foreground=BG_COLOR)],
            )

    # ── 12. legend ────────────────────────────────────────────────────────────
    legend_items = []
    for diff, color in DIFFICULTY_COLORS.items():
        if diff is None or str(diff) in ("nan", ""):
            continue
        if diff in runs_df["difficulty"].values or (
            diff == "easy" and runs_df["difficulty"].isin(["easy"]).any()
        ):
            legend_items.append(
                mlines.Line2D([0], [0], color=color, lw=2, label=diff.capitalize())
            )
    legend_items.append(
        mlines.Line2D([0], [0], color=LIFT_COLOR, lw=1.5,
                      linestyle="--", label="Lift")
    )
    legend_items.append(
        mlines.Line2D([0], [0], color="white", lw=0.6,
                      alpha=0.30, label="Contours (100m)")
    )
    if path_dist_m > 0:
        legend_items.append(
            mlines.Line2D([0], [0], color="white", lw=2.0,
                          alpha=0.9, label=f"Longest path ({path_dist_km:.1f} km)")
        )
    if legend_items:
        ax.legend(
            handles=legend_items, loc="lower right",
            facecolor="#1a1a24", edgecolor="#444",
            labelcolor="white", fontsize=7,
            framealpha=0.8,
        )

    # ── 13. title ─────────────────────────────────────────────────────────────
    ax.set_title(
        _safe_text(resort_name),
        color="white", fontsize=18, fontweight="bold", pad=12,
        path_effects=[pe.withStroke(linewidth=4, foreground=BG_COLOR)],
    )

    # ── 14. fix view limits to include all data (runs + lifts) ───────────────
    pad_x = data_w * 0.12
    pad_y = data_h * 0.12
    ax.set_xlim(x_min - pad_x, x_max + pad_x)
    ax.set_ylim(y_min - pad_y, y_max + pad_y)

    # ── 15. save ─────────────────────────────────────────────────────────────
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
        # Look up country codes for explicitly named resorts
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
    all_paths = _load_all_longest_paths(con)
    for resort, country_code in rows:
        path_info = all_paths.get(resort, (set(), 0.0, ""))
        print(f"  [{country_code}] {resort!r}…")
        render_resort(con, resort, country_code, path_info=path_info)

    con.close()
    print("Done.")


if __name__ == "__main__":
    main()
