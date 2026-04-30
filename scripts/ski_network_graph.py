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
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import duckdb
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars

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
BG_COLOR   = "#0d0d12"
LIFT_COLOR = "#FFD700"      # gold

DIFFICULTY_COLORS = {
    "novice":       "#4CAF50",   # green
    "easy":         "#42A5F5",   # blue
    "intermediate": "#EF5350",   # red
    "advanced":     "#FF9800",   # orange
    "expert":       "#E040FB",   # purple
    "freeride":     "#00BCD4",   # cyan
    None:           "#90A4AE",   # grey
    "nan":          "#90A4AE",
    "":             "#90A4AE",
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


def render_resort(con, resort_name: str, country_code: str = ""):
    # ── 1. run polylines ─────────────────────────────────────────────────────
    runs_df = con.execute("""
        SELECT
            p.osm_id,
            p.lat, p.lon,
            p.point_index,
            r.difficulty,
            r.run_name,
            r.piste_type
        FROM camonairflow.ski_runs.ski_run_points p
        JOIN camonairflow.ski_runs.ski_runs r ON p.osm_id = r.osm_id
        WHERE p.resort = ?
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

    # ── 3. project to metres ─────────────────────────────────────────────────
    lon0 = runs_df["lon"].mean()
    lat0 = runs_df["lat"].mean()

    runs_df["x"], runs_df["y"] = _equirect(runs_df["lon"], runs_df["lat"], lon0, lat0)

    # ── 4. figure ────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(14, 10), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.set_aspect("equal")
    ax.axis("off")

    # ── 5. draw runs ─────────────────────────────────────────────────────────
    for osm_id, group in runs_df.groupby("osm_id", sort=False):
        group = group.sort_values("point_index")
        xs = group["x"].values
        ys = group["y"].values
        diff = group["difficulty"].iloc[0]
        color = _safe_color(diff)

        for lw, alpha in GLOW_LAYERS:
            ax.plot(xs, ys, color=color, lw=lw, alpha=alpha,
                    solid_capstyle="round", solid_joinstyle="round")

    # ── 6. draw lifts ─────────────────────────────────────────────────────────
    for _, lift in lifts_df.iterrows():
        bx, by = _equirect([lift["bottom_lon"]], [lift["bottom_lat"]], lon0, lat0)
        tx, ty = _equirect([lift["top_lon"]],    [lift["top_lat"]],    lon0, lat0)
        xs = [float(bx[0]), float(tx[0])]
        ys = [float(by[0]), float(ty[0])]

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
        if lift["name"]:
            mx = (xs[0] + xs[1]) / 2
            my = (ys[0] + ys[1]) / 2
            ax.text(
                mx, my, _safe_text(lift["name"].title()),
                color=LIFT_COLOR, fontsize=5.5, alpha=0.75,
                ha="center", va="center", rotation=0,
                path_effects=[pe.withStroke(linewidth=2, foreground=BG_COLOR)],
            )

    # ── 7. run name labels (midpoint of each run) ────────────────────────────
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

    # ── 8. legend ────────────────────────────────────────────────────────────
    legend_items = []
    for diff, color in DIFFICULTY_COLORS.items():
        if diff is None or str(diff) in ("nan", ""):
            continue
        # only show difficulties that appear in this resort
        if diff in runs_df["difficulty"].values or (
            diff == "easy" and runs_df["difficulty"].isin(["easy"]).any()
        ):
            legend_items.append(
                matplotlib.lines.Line2D([0], [0], color=color, lw=2, label=diff.capitalize())
            )
    # lifts
    legend_items.append(
        matplotlib.lines.Line2D([0], [0], color=LIFT_COLOR, lw=1.5,
                                linestyle="--", label="Lift")
    )
    if legend_items:
        leg = ax.legend(
            handles=legend_items, loc="lower right",
            facecolor="#1a1a24", edgecolor="#444",
            labelcolor="white", fontsize=7,
            framealpha=0.8,
        )

    # ── 9. title ─────────────────────────────────────────────────────────────
    ax.set_title(
        _safe_text(resort_name),
        color="white", fontsize=18, fontweight="bold", pad=12,
        path_effects=[pe.withStroke(linewidth=4, foreground=BG_COLOR)],
    )

    # ── 10. save ─────────────────────────────────────────────────────────────
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

    con = duckdb.connect(database_string)

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
    for resort, country_code in rows:
        print(f"  [{country_code}] {resort!r}…")
        render_resort(con, resort, country_code)

    con.close()
    print("Done.")


if __name__ == "__main__":
    main()
