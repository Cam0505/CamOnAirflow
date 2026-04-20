import argparse
import os
import unicodedata
import duckdb
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib
import matplotlib.font_manager as fm
import numpy as np
from matplotlib.patches import Patch
from project_path import get_project_paths, set_dlt_env_vars


# --- Define Regions ---
COUNTRIES = {
    "New Zealand": "NZ",
    "Australia": "AU",
    "Canada": "CA",
    "Japan": "JP",
    "China": "CN",
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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate ski terrain gradient distribution charts by country."
    )
    parser.add_argument(
        "--all-countries",
        action="store_true",
        help="Generate charts for every country instead of only countries in the latest DLT ski_runs load.",
    )
    return parser.parse_args()


def build_country_filter(column_name, countries):
    if not countries:
        return "", []
    placeholders = ", ".join(["?"] * len(countries))
    return f" WHERE {column_name} IN ({placeholders})", countries


def get_target_countries(con, run_all_countries=False):
    if run_all_countries:
        print("Running gradient distribution visuals for all countries.")
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
            f"Latest successful DLT load {latest_load_id} at {inserted_at} has no country_code values in camonairflow.ski_runs.ski_runs."
        )

    print(
        f"Running gradient distribution visuals for latest load {latest_load_id} at {inserted_at} "
        f"covering countries: {', '.join(countries)}"
    )
    return countries


def load_plot_data(con, target_countries):
    country_filter, params = build_country_filter("country_code", target_countries)
    return con.execute(
        f"""
        SELECT
            country_code,
            resort,
            gradient_bin,
            gradient_bin_center_deg,
            terrain_m
        FROM camonairflow.public_staging.staging_ski_gradient_distribution
        {country_filter}
        ORDER BY country_code, resort, gradient_bin
        """,
        params,
    ).df()

# --- Plotting ---
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
    suffixes = [
        " Ski Area", " Ski Field", " Alpine Resort", " Valley Ski Area", " Resort"
    ]
    short_name = get_display_text(name, fallback="Unnamed Resort")
    for suffix in suffixes:
        short_name = short_name.replace(suffix, "")
    return short_name.strip()


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


# Broad, non-resort-specific guide to how gradient commonly maps to run colour.
gradient_bands = [
    {"start": 5, "end": 15, "label": "Green", "color": "#57a773"},
    {"start": 15, "end": 25, "label": "Blue", "color": "#4c78a8"},
    {"start": 25, "end": 35, "label": "Red", "color": "#e45756"},
    {"start": 35, "end": 55, "label": "Black", "color": "#3a3a3a"},
]

colormap = matplotlib.colormaps["viridis"]
bar_color = colormap(0.25)
edge_color = colormap(0.1)
bar_width = 2.05

def generate_country_plots(df_all):
    for region, country_code in COUNTRIES.items():
        df = df_all[df_all["country_code"] == country_code]
        if df.empty:
            print(f"No data for {region} ({country_code}), skipping.")
            continue

        resort_order = sorted(df["resort"].unique())
        n_panels = len(resort_order)
        ncols = 3
        nrows = int(np.ceil(n_panels / ncols))
        y_max = float(df["terrain_m"].max())
        y_top = y_max * 1.05 if y_max > 0 else 1.0

        fig, axes = plt.subplots(
            nrows,
            ncols,
            figsize=(18, 4.2 * nrows),
            sharex=False,
            sharey=False
        )
        axes = np.array(axes).reshape(nrows, ncols)

        for idx, resort in enumerate(resort_order):
            r, c = divmod(idx, ncols)
            ax = axes[r, c]
            df_resort = df[df["resort"] == resort]

            ax.bar(
                df_resort["gradient_bin_center_deg"],
                df_resort["terrain_m"],
                width=bar_width,
                color=bar_color,
                edgecolor=edge_color,
                linewidth=0.5
            )
            ax.set_title(
                f"{country_code} - {short_resort_name(resort)}",
                fontsize=14,
                fontweight="bold",
                pad=8
            )
            ax.grid(True, axis="y", linestyle="--", alpha=0.35)
            ax.set_xlim(5, 55)
            ax.set_ylim(0, y_top)
            ax.set_xticks(np.arange(5, 56, 5))
            ax.tick_params(axis="x", labelrotation=45, labelsize=11, labelbottom=True)
            ax.tick_params(axis="y", labelsize=11)
            _apply_gradient_bands(ax)
            ax.set_xlabel("Gradient Bucket Center (°)", fontsize=12, fontweight="bold")
            ax.set_ylabel("API Skiable Terrain (m)", fontsize=12, fontweight="bold")

        for idx in range(n_panels, nrows * ncols):
            r, c = divmod(idx, ncols)
            axes[r, c].set_visible(False)

        fig.suptitle(f"Ski Terrain Gradient Distribution ({region})", fontsize=22, fontweight="bold", y=0.995)
        fig.legend(
            handles=[
                Patch(facecolor=band["color"], edgecolor="none", alpha=0.2, label=f"{band['label']}: {band['start']}\u00b0-{band['end']}\u00b0")
                for band in gradient_bands
            ],
            loc="upper center",
            ncol=4,
            frameon=False,
            bbox_to_anchor=(0.5, 0.965),
        )
        plt.tight_layout(rect=(0.03, 0.05, 1, 0.94))
        out_path = f"charts/gradient_distribution_{country_code.lower()}.png"
        plt.savefig(out_path, dpi=250, bbox_inches="tight")
        plt.show()


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
        generate_country_plots(df_all)
    finally:
        con.close()


if __name__ == "__main__":
    main()
