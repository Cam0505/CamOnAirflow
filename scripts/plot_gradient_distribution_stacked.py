"""
Gradient distribution chart with bars stacked by difficulty – all countries.
Stack order (bottom → top): None, novice, easy, intermediate, advanced, freeride
"""

import argparse
import math
import os
import unicodedata
import duckdb
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib as mpl
import numpy as np
from matplotlib.patches import Patch
from project_path import get_project_paths, set_dlt_env_vars


# ---------------------------------------------------------------------------
# Region / country config
# ---------------------------------------------------------------------------
COUNTRIES = {
    "New Zealand": "NZ",
    "Australia": "AU",
    "Canada": "CA",
    "Japan": "JP",
    "China": "CN",
    "Chile": "CL",
    "Argentina": "AR",
}

DISPLAY_NAME_OVERRIDES = {
    "Tūroa Ski Area": "Turoa Ski Area",
    "太舞滑雪场": "Thaiwoo Ski Resort",
    "雪如意滑雪场": "Snow Ruyi Ski Resort",
    "富龙滑雪场": "Fulong Ski Resort",
    "密苑云顶乐园": "Yunding Resort Secret Garden",
    "国家高山滑雪中心": "National Alpine Skiing Centre",
    "禾木（吉克普林）国际滑雪度假区": "Jikepulin Hemu Ski Resort",
    "将军山滑雪场": "Jiangjunshan Ski Resort",
}

CJK_FONT_CANDIDATES = [
    "Noto Sans CJK SC",
    "Noto Sans CJK JP",
    "Source Han Sans SC",
    "Source Han Sans CN",
    "WenQuanYi Zen Hei",
    "SimHei",
    "Microsoft YaHei",
    "Arial Unicode MS",
    "Sarasa Gothic SC",
    "Sarasa UI SC",
]


def configure_matplotlib_fonts():
    plt.rcParams["axes.unicode_minus"] = False
    available_fonts = {font.name for font in fm.fontManager.ttflist}
    for font_name in CJK_FONT_CANDIDATES:
        if font_name in available_fonts:
            plt.rcParams["font.family"] = font_name
            return True
    plt.rcParams["font.family"] = "DejaVu Sans"
    return False


HAS_CJK_FONT = configure_matplotlib_fonts()


# ---------------------------------------------------------------------------
# Difficulty config
# ---------------------------------------------------------------------------
DIFFICULTY_ORDER = ["none", "novice", "easy", "intermediate", "advanced", "freeride"]

DIFFICULTY_COLORS = {
    "none": "#9e9e9e",
    "novice": "#57a773",
    "easy": "#4c78a8",
    "intermediate": "#e45756",
    "advanced": "#3a3a3a",
    "freeride": "#00d8fe",
}

DIFFICULTY_LABELS = {
    "none": "None",
    "novice": "Novice",
    "easy": "Easy",
    "intermediate": "Intermediate",
    "advanced": "Advanced",
    "freeride": "Freeride",
}

# ---------------------------------------------------------------------------
# Gradient band background shading
# ---------------------------------------------------------------------------
GRADIENT_BANDS = [
    {"start": 5, "end": 15, "label": "Green", "color": "#57a773"},
    {"start": 15, "end": 25, "label": "Blue", "color": "#4c78a8"},
    {"start": 25, "end": 35, "label": "Red", "color": "#e45756"},
    {"start": 35, "end": 55, "label": "Black", "color": "#3a3a3a"},
]

BAR_WIDTH = 2.05


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate stacked ski terrain gradient distribution charts by country."
    )
    parser.add_argument(
        "--all-countries",
        action="store_true",
        help="Generate charts for every country instead of only countries in the latest DLT ski_runs load.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Country targeting (mirrors non-stacked version)
# ---------------------------------------------------------------------------
def build_country_filter(column_name, countries):
    if not countries:
        return "", []
    placeholders = ", ".join(["?"] * len(countries))
    return f" WHERE {column_name} IN ({placeholders})", countries


def get_target_countries(con, run_all_countries=False):
    if run_all_countries:
        print("Running stacked gradient distribution visuals for all countries.")
        return None

    latest_load = con.execute(
        """
        SELECT load_id, inserted_at
        FROM camonairflow.ski_runs._dlt_loads
        WHERE status = 0
        ORDER BY inserted_at DESC, load_id DESC
        LIMIT 1
        """
    ).fetchone()

    if latest_load is None:
        raise ValueError("No successful DLT loads found in camonairflow.ski_runs._dlt_loads.")

    latest_load_id, inserted_at = latest_load
    countries = [
        row[0]
        for row in con.execute(
            """
            SELECT DISTINCT r.country_code
            FROM camonairflow.ski_runs.ski_runs AS r
            INNER JOIN camonairflow.ski_runs._dlt_loads AS l
                ON r._dlt_load_id = l.load_id
            WHERE l.load_id = ?
              AND coalesce(r.country_code, '') <> ''
            ORDER BY r.country_code
            """,
            [latest_load_id],
        ).fetchall()
    ]

    if not countries:
        raise ValueError(
            f"Latest successful DLT load {latest_load_id} at {inserted_at} has no country_code values "
            "in camonairflow.ski_runs.ski_runs."
        )

    print(
        f"Running stacked gradient distribution visuals for latest load {latest_load_id} at {inserted_at} "
        f"covering countries: {', '.join(countries)}"
    )
    return countries


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_plot_data(con, target_countries):
    country_filter, params = build_country_filter("country_code", target_countries)
    return con.execute(
        f"""
        SELECT
            country_code,
            resort,
            gradient_bin,
            gradient_bin_center_deg,
            COALESCE(difficulty, '') AS difficulty,
            terrain_m
        FROM camonairflow.public_staging.staging_ski_gradient_distribution
        {country_filter}
        ORDER BY country_code, resort, gradient_bin
        """,
        params,
    ).df()


def load_resolution_data(con, target_countries):
    """Load length-weighted average DEM resolution per resort from ski_runs."""
    if target_countries:
        placeholders = ", ".join(["?"] * len(target_countries))
        extra = f"AND country_code IN ({placeholders})"
        params = list(target_countries)
    else:
        extra = ""
        params = []
    try:
        rows = con.execute(
            f"""
            SELECT
                country_code,
                resort,
                SUM(avg_point_resolution_m * run_length_m) / NULLIF(SUM(run_length_m), 0) AS avg_resolution_m
            FROM camonairflow.ski_runs.ski_runs
            WHERE avg_point_resolution_m IS NOT NULL
            {extra}
            GROUP BY country_code, resort
            """,
            params,
        ).fetchall()
        return {(row[0], row[1]): row[2] for row in rows}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def has_non_ascii_text(value):
    if value is None:
        return False
    return any(ord(char) > 127 for char in str(value))


def get_display_text(value, fallback=None):
    if value is None:
        return fallback or ""
    text = str(value).strip()
    if not text:
        return fallback or ""
    text = DISPLAY_NAME_OVERRIDES.get(text, text)
    if HAS_CJK_FONT or not has_non_ascii_text(text):
        return text
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", errors="ignore").decode("ascii").strip()
    return ascii_text or fallback or ""


def short_resort_name(name: str) -> str:
    suffixes = [" Ski Area", " Ski Field", " Alpine Resort", " Valley Ski Area", " Resort"]
    short = get_display_text(name, fallback="Unnamed Resort")
    for suffix in suffixes:
        short = short.replace(suffix, "")
    return short.strip()


def _normalise_difficulty(val):
    if val is None or str(val).strip() in ("", "nan"):
        return "none"
    v = str(val).strip().lower()
    try:
        float(v)
        return "none"
    except ValueError:
        pass
    return v if v in DIFFICULTY_ORDER else "none"


# ---------------------------------------------------------------------------
# Plotting helpers
# ---------------------------------------------------------------------------
def _band_color_for_value(value):
    for band in GRADIENT_BANDS:
        if band["start"] <= value < band["end"] or (value >= 50 and band["end"] == 55):
            return band["color"]
    return "#444444"


def _apply_gradient_bands(ax):
    for band in GRADIENT_BANDS:
        ax.axvspan(band["start"], band["end"], color=band["color"], alpha=0.1, zorder=0)
    for tick_value, tick_label in zip(ax.get_xticks(), ax.get_xticklabels()):
        tick_label.set_color(_band_color_for_value(tick_value))
        tick_label.set_fontweight("bold")


def _resolution_color(resolution_m):
    """Map DEM resolution in metres to an RGB color on a green→yellow→red log scale."""
    if resolution_m is None or resolution_m <= 0:
        return None
    lo, hi = math.log(1.0), math.log(200.0)
    t = max(0.0, min(1.0, (math.log(max(resolution_m, 1.0)) - lo) / (hi - lo)))
    # RdYlGn(1-t): t=0 (fine resolution) → green, t=1 (coarse) → red
    cmap = mpl.colormaps["RdYlGn"]
    return cmap(1.0 - t)[:3]


def _draw_resolution_stamp(ax, resolution_m):
    """Draw a small color-coded DEM resolution badge in the top-right corner of ax."""
    if resolution_m is None:
        return
    color = _resolution_color(resolution_m)
    if color is None:
        return
    r, g, b = color
    text_color = "black" if (0.299 * r + 0.587 * g + 0.114 * b) > 0.55 else "white"
    ax.text(
        0.975, 0.965,
        f"~{resolution_m:.0f}m",
        transform=ax.transAxes,
        fontsize=8,
        fontweight="bold",
        ha="right",
        va="top",
        color=text_color,
        zorder=5,
        bbox=dict(
            boxstyle="round,pad=0.35",
            facecolor=color,
            edgecolor=text_color,
            linewidth=1.0,
            alpha=0.88,
        ),
    )


# ---------------------------------------------------------------------------
# Per-country chart generation
# ---------------------------------------------------------------------------
def generate_country_plots(df_all, resolution_by_resort=None):
    df_all = df_all.copy()
    df_all["diff_key"] = df_all["difficulty"].apply(_normalise_difficulty)

    for region, country_code in COUNTRIES.items():
        df = df_all[df_all["country_code"] == country_code]
        if df.empty:
            print(f"No data for {region} ({country_code}), skipping.")
            continue

        # Pivot: (resort, gradient_bin, gradient_bin_center_deg) × difficulty
        pivot = (
            df
            .groupby(["resort", "gradient_bin", "gradient_bin_center_deg", "diff_key"], as_index=False)["terrain_m"]
            .sum()
            .pivot_table(
                index=["resort", "gradient_bin", "gradient_bin_center_deg"],
                columns="diff_key",
                values="terrain_m",
                fill_value=0,
            )
            .reset_index()
        )

        for d in DIFFICULTY_ORDER:
            if d not in pivot.columns:
                pivot[d] = 0.0

        resort_order = sorted(df["resort"].unique())
        n_panels = len(resort_order)
        ncols = 3
        nrows = int(np.ceil(n_panels / ncols))

        y_max = pivot[DIFFICULTY_ORDER].sum(axis=1).max()
        y_top = y_max * 1.05 if y_max > 0 else 1.0

        fig, axes = plt.subplots(
            nrows, ncols,
            figsize=(18, 4.2 * nrows),
            sharex=False,
            sharey=False,
        )
        axes = np.array(axes).reshape(nrows, ncols)

        for idx, resort in enumerate(resort_order):
            r, c = divmod(idx, ncols)
            ax = axes[r, c]

            df_r = pivot[pivot["resort"] == resort].sort_values("gradient_bin_center_deg")
            x = df_r["gradient_bin_center_deg"].values
            bottoms = np.zeros(len(x))

            for diff_key in DIFFICULTY_ORDER:
                heights = df_r[diff_key].values
                ax.bar(
                    x,
                    heights,
                    bottom=bottoms,
                    width=BAR_WIDTH,
                    color=DIFFICULTY_COLORS[diff_key],
                    edgecolor="white",
                    linewidth=0.3,
                    label=DIFFICULTY_LABELS[diff_key],
                )
                bottoms = bottoms + heights

            ax.set_title(
                f"{country_code} - {short_resort_name(resort)}",
                fontsize=14,
                fontweight="bold",
                pad=8,
            )
            ax.grid(True, axis="y", linestyle="--", alpha=0.35)
            ax.set_xlim(5, 55)
            ax.set_ylim(0, y_top)
            ax.set_xticks(np.arange(5, 56, 5))
            ax.tick_params(axis="x", labelrotation=45, labelsize=11, labelbottom=True)
            ax.tick_params(axis="y", labelsize=11)
            _apply_gradient_bands(ax)
            if resolution_by_resort is not None:
                _draw_resolution_stamp(ax, resolution_by_resort.get((country_code, resort)))
            ax.set_xlabel("Gradient Bucket Center (°)", fontsize=12, fontweight="bold")
            ax.set_ylabel("API Skiable Terrain (m)", fontsize=12, fontweight="bold")

        for idx in range(n_panels, nrows * ncols):
            r, c = divmod(idx, ncols)
            axes[r, c].set_visible(False)

        fig.suptitle(
            f"Ski Terrain Gradient Distribution by Difficulty ({region})",
            fontsize=22,
            fontweight="bold",
            y=0.995,
        )

        difficulty_handles = [
            Patch(facecolor=DIFFICULTY_COLORS[d], edgecolor="none", label=DIFFICULTY_LABELS[d])
            for d in DIFFICULTY_ORDER
        ]
        fig.legend(
            handles=difficulty_handles,
            loc="upper center",
            ncol=len(DIFFICULTY_ORDER),
            frameon=False,
            bbox_to_anchor=(0.5, 0.965),
            fontsize=11,
        )

        plt.tight_layout(rect=(0.03, 0.05, 1, 0.94))
        out_path = f"charts/gradient_distribution_{country_code.lower()}_stacked.png"
        plt.savefig(out_path, dpi=250, bbox_inches="tight")
        print(f"Saved → {out_path}")
        plt.show()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    args = parse_args()

    paths = get_project_paths()
    set_dlt_env_vars(paths)
    load_dotenv(dotenv_path=paths["ENV_FILE"])
    database_string = os.getenv("MD")
    if not database_string:
        raise ValueError("Missing MD in environment.")

    con = duckdb.connect(database_string)
    try:
        target_countries = get_target_countries(con, run_all_countries=args.all_countries)
        df_all = load_plot_data(con, target_countries)
        if df_all.empty:
            raise ValueError("No gradient distribution data found for the selected countries.")
        resolution_by_resort = load_resolution_data(con, target_countries)
        generate_country_plots(df_all, resolution_by_resort=resolution_by_resort)
    finally:
        con.close()


if __name__ == "__main__":
    main()
