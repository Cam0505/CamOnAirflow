"""
Ski resort gradient heat-map visualiser.

Draws each run's polyline as per-segment heat lines coloured by absolute
gradient — blue = gentle, yellow = moderate, red = steep.
No difficulty colouring; pure steepness signal.

The colour scale is normalised per resort to the 95th-percentile segment
gradient so extreme cliff sections don't compress the rest into blue.
A colorbar (bottom-left inset) shows the gradient in degrees.

Usage:
    python scripts/ski_gradient_heatmap.py                   # all resorts
    python scripts/ski_gradient_heatmap.py "Treble Cone Ski Area"
    python scripts/ski_gradient_heatmap.py "Treble Cone Ski Area" "Whakapapa Ski Area"
"""

import os
import sys
import math
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["font.family"] = ["DejaVu Sans", "IPAexGothic", "AR PL UMing CN"]
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.patheffects as pe  # noqa: E402
import matplotlib.lines as mlines  # noqa: E402
from matplotlib.collections import LineCollection  # noqa: E402
from matplotlib.colors import Normalize, LightSource  # noqa: E402
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

OUTPUT_DIR = os.path.join(paths["PROJECT_ROOT"], "charts", "ski_gradient_heatmaps")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── style ─────────────────────────────────────────────────────────────────────
BG_COLOR = "#0d0d12"
LIFT_COLOR = "#FFD700"  # gold

# Blue → yellow → red: cool/gentle to hot/steep
GRAD_CMAP = matplotlib.colormaps["RdYlBu_r"]

# Clip the colormap scale at this percentile so cliff outliers don't wash out
# the rest of the resort into a uniform blue.
GRAD_VMAX_PERCENTILE = 95

LIFT_GLOW_LAYERS = [
    (5.0, 0.08),
    (2.5, 0.20),
    (1.2, 0.90),
]
HIKE_DASHES = (5, 4)


# ── helpers ───────────────────────────────────────────────────────────────────

def _safe_text(s):
    if not s:
        return s
    return str(s).replace("$", r"\$").replace("_", r"\_")


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
        blend_mode="soft",
        vert_exag=3.0,
        dx=dx_m,
        dy=dy_m,
    )
    ax.imshow(shade_rgb, extent=extent, origin="lower", aspect="auto",
              alpha=0.06, zorder=0, interpolation="bilinear")

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


def _build_gradient_segments(runs_df):
    """
    For every consecutive pair of projected points per run, compute the
    absolute gradient in degrees from smoothed elevation and 2-D distance.

    Returns four lists:
        dl_segs, dl_grads  — downhill run segments and their gradients (°)
        hk_segs, hk_grads  — hike/bootpack segments and their gradients (°)
    """
    dl_segs, dl_grads = [], []
    hk_segs, hk_grads = [], []

    for osm_id, group in runs_df.groupby("osm_id", sort=False):
        group = group.sort_values("point_index")
        xs = group["x"].values
        ys = group["y"].values
        elevs = group["elevation_smoothed_m"].values.astype(float)
        is_hike = str(group["piste_type"].iloc[0]).lower() == "hike"

        for i in range(len(xs) - 1):
            dx = float(xs[i + 1]) - float(xs[i])
            dy = float(ys[i + 1]) - float(ys[i])
            dist = math.hypot(dx, dy)
            e0, e1 = elevs[i], elevs[i + 1]

            if dist > 0 and not (np.isnan(e0) or np.isnan(e1)):
                grad_deg = math.degrees(math.atan(abs(float(e0) - float(e1)) / dist))
            else:
                grad_deg = 0.0

            seg = [[xs[i], ys[i]], [xs[i + 1], ys[i + 1]]]
            if is_hike:
                hk_segs.append(seg)
                hk_grads.append(grad_deg)
            else:
                dl_segs.append(seg)
                dl_grads.append(grad_deg)

    return dl_segs, dl_grads, hk_segs, hk_grads


# ── main render ───────────────────────────────────────────────────────────────

def render_resort(con, resort_name: str, country_code: str = ""):
    # ── 1. run polylines ──────────────────────────────────────────────────────
    runs_df = con.execute("""
        SELECT
            p.osm_id,
            p.lat, p.lon,
            p.point_index,
            p.elevation_smoothed_m,
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
        SELECT name, lift_type,
               top_lat, top_lon, bottom_lat, bottom_lon
        FROM camonairflow.ski_runs.ski_lifts
        WHERE resort = ?
    """, [resort_name]).df()

    # ── 3. run top/bottom coords (for rotation) ───────────────────────────────
    runs_meta_df = con.execute("""
        SELECT top_lat, top_lon, bottom_lat, bottom_lon
        FROM camonairflow.ski_runs.ski_runs
        WHERE resort = ?
    """, [resort_name]).df()

    # ── 4. project to metres ──────────────────────────────────────────────────
    lon0 = runs_df["lon"].mean()
    lat0 = runs_df["lat"].mean()
    runs_df["x"], runs_df["y"] = _equirect(runs_df["lon"], runs_df["lat"], lon0, lat0)

    # ── 5. uphill rotation ────────────────────────────────────────────────────
    rot = _compute_rotation(runs_meta_df, lon0, lat0)
    runs_df["x"], runs_df["y"] = _rotate(runs_df["x"].values, runs_df["y"].values, rot)

    # ── 5b. project lift coordinates ─────────────────────────────────────────
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

    # ── 5c. figure size from data aspect ratio ────────────────────────────────
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
    if aspect_ratio >= 1.0:
        fig_w = max(BASE / aspect_ratio, 5.0)
        fig_h = BASE
    else:
        fig_w = BASE
        fig_h = max(BASE * aspect_ratio, 5.0)

    # ── 6. compute per-segment gradients ─────────────────────────────────────
    dl_segs, dl_grads, hk_segs, hk_grads = _build_gradient_segments(runs_df)

    all_grads = dl_grads + hk_grads
    if not all_grads:
        print(f"  No gradient data for {resort_name!r} — skipping.")
        return

    vmax = float(np.percentile(all_grads, GRAD_VMAX_PERCENTILE))
    vmax = max(vmax, 5.0)  # floor so flat resorts still show a meaningful scale
    norm = Normalize(vmin=0.0, vmax=vmax)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.set_aspect("equal")
    ax.axis("off")

    # ── 7. topo contours ─────────────────────────────────────────────────────
    elev = runs_df["elevation_smoothed_m"].values.astype(float)
    _draw_topo(ax, runs_df["x"].values, runs_df["y"].values, elev)

    # ── 8. downhill runs as gradient-coloured LineCollections ─────────────────
    if dl_segs:
        dl_arr = np.array(dl_grads)
        # Glow layer — wide, semi-transparent
        lc_glow = LineCollection(dl_segs, cmap=GRAD_CMAP, norm=norm,
                                 linewidths=5.5, alpha=0.18, zorder=2)
        lc_glow.set_array(dl_arr)
        ax.add_collection(lc_glow)
        # Core layer — thin, opaque
        lc_core = LineCollection(dl_segs, cmap=GRAD_CMAP, norm=norm,
                                 linewidths=1.6, alpha=0.92, zorder=3,
                                 capstyle="round", joinstyle="round")
        lc_core.set_array(dl_arr)
        ax.add_collection(lc_core)

    # ── 9. hike segments — dashed, gradient-coloured ──────────────────────────
    for seg, grad in zip(hk_segs, hk_grads):
        color = GRAD_CMAP(norm(grad))
        xs_h = [seg[0][0], seg[1][0]]
        ys_h = [seg[0][1], seg[1][1]]
        ax.plot(xs_h, ys_h, color=color, lw=1.2, alpha=0.75,
                linestyle="--", dashes=HIKE_DASHES,
                solid_capstyle="butt", zorder=3)

    # ── 10. lifts ─────────────────────────────────────────────────────────────
    for lp in lifts_projected:
        xs = lp["xs"]
        ys = lp["ys"]
        for lw, alpha in LIFT_GLOW_LAYERS:
            ax.plot(xs, ys, color=LIFT_COLOR, lw=lw, alpha=alpha,
                    linestyle="--", dashes=(6, 4), solid_capstyle="butt")
        ax.annotate(
            "", xy=(xs[1], ys[1]), xytext=(xs[0], ys[0]),
            arrowprops=dict(
                arrowstyle="-|>", color=LIFT_COLOR, lw=0.8,
                mutation_scale=12, alpha=0.8,
            ),
        )
        if lp["name"]:
            mx = (xs[0] + xs[1]) / 2
            my = (ys[0] + ys[1]) / 2
            ax.text(
                mx, my, _safe_text(lp["name"].title()),
                color=LIFT_COLOR, fontsize=5.5, alpha=0.75,
                ha="center", va="center",
                path_effects=[pe.withStroke(linewidth=2, foreground=BG_COLOR)],
            )

    # ── 11. run name labels ───────────────────────────────────────────────────
    LABEL_MIN_DIST_M = 120.0
    placed: list[tuple[float, float]] = []
    label_candidates = []
    for osm_id, group in runs_df.groupby("osm_id", sort=False):
        group = group.sort_values("point_index")
        mid = len(group) // 2
        mx = float(group["x"].iloc[mid])
        my = float(group["y"].iloc[mid])
        run_name = group["run_name"].iloc[0]
        label_candidates.append((float(len(group)), mx, my, run_name))
    label_candidates.sort(key=lambda t: t[0], reverse=True)

    for _, mx, my, run_name in label_candidates:
        if not run_name or not str(run_name).strip():
            continue
        if any(math.hypot(mx - px, my - py) < LABEL_MIN_DIST_M for px, py in placed):
            continue
        placed.append((mx, my))
        ax.text(
            mx, my, _safe_text(run_name),
            color="white", fontsize=5, alpha=0.65,
            ha="center", va="center",
            path_effects=[pe.withStroke(linewidth=2, foreground=BG_COLOR)],
        )

    # ── 12. gradient colorbar (inset, bottom-left) ────────────────────────────
    if dl_segs:
        sm = plt.cm.ScalarMappable(cmap=GRAD_CMAP, norm=norm)
        sm.set_array([])
        cax = ax.inset_axes([0.02, 0.04, 0.025, 0.30])
        cbar = fig.colorbar(sm, cax=cax, orientation="vertical")
        cbar.set_label("Gradient (°)", color="white", fontsize=6, labelpad=4)
        cbar.ax.yaxis.set_tick_params(color="white", labelcolor="white", labelsize=5.5)
        cbar.outline.set_edgecolor("#555")

    # ── 13. legend (lifts / hike / contours) ─────────────────────────────────
    legend_items = [
        mlines.Line2D([0], [0], color=LIFT_COLOR, lw=1.5,
                      linestyle="--", label="Lift"),
    ]
    if hk_segs:
        legend_items.append(
            mlines.Line2D([0], [0], color="#90A4AE", lw=1.2,
                          linestyle="--", dashes=HIKE_DASHES,
                          label="Bootpack (hike)")
        )
    legend_items.append(
        mlines.Line2D([0], [0], color="white", lw=0.6,
                      alpha=0.30, label="Contours (50m)")
    )
    ax.legend(
        handles=legend_items, loc="lower right",
        facecolor="#1a1a24", edgecolor="#444",
        labelcolor="white", fontsize=7,
        framealpha=0.8,
    )

    # ── 14. title ─────────────────────────────────────────────────────────────
    ax.set_title(
        _safe_text(resort_name),
        color="white", fontsize=18, fontweight="bold", pad=12,
        path_effects=[pe.withStroke(linewidth=4, foreground=BG_COLOR)],
    )

    # ── 15. view limits ───────────────────────────────────────────────────────
    pad_x = data_w * 0.12
    pad_y = data_h * 0.12
    ax.set_xlim(x_min - pad_x, x_max + pad_x)
    ax.set_ylim(y_min - pad_y, y_max + pad_y)

    # ── 16. save ──────────────────────────────────────────────────────────────
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
    for resort, country_code in rows:
        print(f"  [{country_code}] {resort!r}…")
        render_resort(con, resort, country_code)

    con.close()
    print("Done.")


if __name__ == "__main__":
    main()
