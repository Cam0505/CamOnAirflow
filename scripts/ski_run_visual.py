import argparse
import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from scipy.signal import savgol_filter
import duckdb
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars

try:
    import japanize_matplotlib
    japanize_matplotlib.japanize()
except ImportError:
    japanize_matplotlib = None


DISPLAY_NAME_OVERRIDES = {
    "太舞滑雪场": "Thaiwoo Ski Resort",
    "雪如意滑雪场": "Snow Ruyi Ski Resort",
    "富龙滑雪场": "Fulong Ski Resort",
    "密苑云顶乐园": "Yunding Resort Secret Garden",
    "国家高山滑雪中心": "National Alpine Skiing Centre",
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

# --- Difficulty color palette ---
difficulty_colors = {
    "novice": "#4daf4a",
    "easy": "#377eb8",
    "intermediate": "#d90f0f",
    "advanced": "#0e0d0d",
    "freeride": "#00CED1",
    None: "#999999",
    "nan": "#999999"
}


def configure_matplotlib_fonts():
    available_fonts = {font.name for font in fm.fontManager.ttflist}
    for font_name in CJK_FONT_CANDIDATES:
        if font_name in available_fonts:
            plt.rcParams["font.family"] = font_name
            plt.rcParams["axes.unicode_minus"] = False
            return True
    plt.rcParams["font.family"] = "DejaVu Sans"
    plt.rcParams["axes.unicode_minus"] = False
    return False


HAS_CJK_FONT = configure_matplotlib_fonts()

# --- Smoothing function ---
def smooth_elevation(y, window=9, poly=2):
    n = len(y)
    if n < window:
        return y
    window = min(window if window % 2 == 1 else window + 1, n)
    if window < 3:
        return y
    return savgol_filter(y, window, poly)


def normalize_region(value):
    if value is None:
        return "Unknown Region"
    text = str(value).strip()
    return text if text else "Unknown Region"


def slugify_region(region):
    return normalize_region(region).lower().replace(' ', '_').replace('/', '_')


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
    return fallback or text.encode("ascii", errors="ignore").decode("ascii").strip()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate ski run elevation charts by region."
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
    return f" AND {column_name} IN ({placeholders})", countries


def get_target_countries(con, run_all_countries=False):
    if run_all_countries:
        print("Running ski run visuals for all countries.")
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
        f"Running ski run visuals for latest load {latest_load_id} at {inserted_at} "
        f"covering countries: {', '.join(countries)}"
    )
    return countries


def load_plot_data(con, target_countries):
    runs_country_filter, runs_params = build_country_filter("country_code", target_countries)
    points_country_filter, points_params = build_country_filter("r.country_code", target_countries)
    gradient_country_filter, gradient_params = build_country_filter("country_code", target_countries)

    runs = con.execute(
        f"""
        SELECT osm_id, resort, region, run_name, country_code,
            CASE WHEN difficulty = 'extreme' THEN 'intermediate'
                 WHEN difficulty = 'expert' THEN 'advanced'
                 ELSE difficulty END AS difficulty,
            piste_type,
            run_length_m,
            n_points
        FROM camonairflow.public_base.base_filtered_ski_runs
        WHERE run_length_m > 200
          AND n_points > 4
          AND coalesce(piste_type, '') NOT IN ('skitour', 'nordic', 'sled', 'snow_park')
          {runs_country_filter}
        """,
        runs_params,
    ).df()

    points = con.execute(
        f"""
        SELECT p.osm_id, p.resort, r.region, p.distance_along_run_m, p.elevation_m
        FROM camonairflow.public_base.base_filtered_ski_points AS p
        INNER JOIN camonairflow.public_base.base_filtered_ski_runs AS r
            ON p.osm_id = r.osm_id
           AND p.resort = r.resort
        WHERE r.run_length_m > 200
          AND r.n_points > 4
          AND coalesce(r.piste_type, '') NOT IN ('skitour', 'nordic', 'sled', 'snow_park')
          {points_country_filter}
        """,
        points_params,
    ).df()

    gradient_stats = con.execute(
        f"""
        WITH filtered_runs AS (
            SELECT DISTINCT resort, region
            FROM camonairflow.public_base.base_filtered_ski_runs
            WHERE run_length_m > 200
              AND n_points > 4
              AND coalesce(piste_type, '') NOT IN ('skitour', 'nordic', 'sled', 'snow_park')
              {gradient_country_filter}
        )
        SELECT fr.region,
            gs.resort,
            CASE WHEN gs.difficulty = 'extreme' THEN 'intermediate'
                 WHEN gs.difficulty = 'expert' THEN 'advanced'
                 ELSE gs.difficulty END AS difficulty,
            gs.run_count,
            gs.mean_gradient_degrees,
            gs.mean_steepest_degrees,
            gs.mean_gradient_percent,
            gs.mean_steepest_percent
        FROM camonairflow.public_base.base_ski_gradient_stats AS gs
        INNER JOIN filtered_runs AS fr
            ON gs.resort = fr.resort
        """,
        gradient_params,
    ).df()

    runs['region'] = runs['region'].map(normalize_region)
    points['region'] = points['region'].map(normalize_region)
    gradient_stats['region'] = gradient_stats['region'].map(normalize_region)
    return runs, points, gradient_stats


def generate_region_plots(runs, points, gradient_stats):
    for region in sorted(runs['region'].dropna().unique()):
        runs_region = runs[runs['region'] == region].copy()
        points_region = points[points['region'] == region].copy()
        gradient_stats_region = gradient_stats[gradient_stats['region'] == region].copy()
        resorts = sorted(runs_region['resort'].dropna().unique())

        if not resorts or points_region.empty:
            continue

        for label_mode in ["degrees"]:
            for diff in runs_region['difficulty'].dropna().unique():
                if diff not in difficulty_colors:
                    difficulty_colors[diff] = "#444444"

            max_distance = points_region['distance_along_run_m'].max() * 1.05
            min_elev = int(np.floor(points_region['elevation_m'].min() / 100.0) * 100)
            max_elev = int(np.ceil(points_region['elevation_m'].max() / 100.0) * 100)

            ncols = 4
            nrows = int(np.ceil(len(resorts) / ncols))
            fig, axes = plt.subplots(nrows, ncols, figsize=(20, 8 * nrows), sharey=True)
            axes = axes.flatten()

            for ax, resort in zip(axes, resorts):
                runs_this = runs_region[runs_region['resort'] == resort]
                points_this = points_region[points_region['resort'] == resort]
                for _, run in runs_this.iterrows():
                    pts = points_this[points_this['osm_id'] == run['osm_id']].sort_values('distance_along_run_m')
                    if len(pts) > 1 and pts.iloc[0]['elevation_m'] < pts.iloc[-1]['elevation_m']:
                        pts = pts.iloc[::-1]
                    y_smoothed = smooth_elevation(pts['elevation_m'].values)
                    color = difficulty_colors.get(run['difficulty'], "#444444")
                    ax.plot(pts['distance_along_run_m'], y_smoothed, color=color, alpha=0.8, linewidth=2)

                if not runs_this.empty:
                    longest = runs_this.loc[runs_this['run_length_m'].idxmax()]
                    pts_long = points_this[points_this['osm_id'] == longest['osm_id']].sort_values('distance_along_run_m')
                    ax.text(
                        pts_long['distance_along_run_m'].iloc[-1],
                        pts_long['elevation_m'].iloc[-1],
                        get_display_text(longest['run_name'], fallback=f"Run {longest['osm_id']}"),
                        fontsize=11, fontweight='bold', color='black',
                        ha='right', va='bottom'
                    )
                ax.set_title(get_display_text(resort, fallback="Unnamed Resort"), fontsize=20, fontweight='bold')
                ax.set_xlabel("Run Distance Along Slope (meters)", fontsize=14)
                ax.set_ylabel("Elevation (m)", fontsize=14)
                ax.grid(True, linestyle='--', alpha=0.4)
                ax.set_xlim(0, max_distance)
                ax.set_ylim(min_elev, max_elev)

                labels_this = gradient_stats_region[gradient_stats_region['resort'] == resort]
                if not labels_this.empty:
                    labels_this = labels_this.sort_values(
                        'mean_gradient_degrees' if label_mode == "degrees" else 'mean_gradient_percent',
                        ascending=False
                    )
                    ymin, ymax = ax.get_ylim()
                    xmin, xmax = ax.get_xlim()
                    y = ymax - 0.036 * (ymax - ymin)
                    x = xmax - 0.01 * (xmax - xmin)
                    lineheight = 0.066 * (ymax - ymin)
                    for i, row in enumerate(labels_this.itertuples()):
                        if i >= 5:
                            break
                        color = difficulty_colors.get(row.difficulty, "#444444")
                        if label_mode == "degrees":
                            label = (
                                f"{row.difficulty} ({row.run_count}): "
                                f"{row.mean_gradient_degrees:.1f}°, {row.mean_steepest_degrees:.1f}°"
                            )
                        else:
                            label = (
                                f"{row.difficulty} ({row.run_count}): "
                                f"{row.mean_gradient_percent:.0f}%, {row.mean_steepest_percent:.0f}%"
                            )
                        ax.text(
                            x, y - i * lineheight,
                            label,
                            fontsize=13, color=color,
                            ha='right', va='top', fontweight='bold', alpha=0.98, zorder=11,
                            bbox=dict(facecolor='white', alpha=0.73, edgecolor='none', boxstyle='round,pad=0.16')
                        )

            for i in range(len(resorts), nrows * ncols):
                axes[i].axis('off')

            plt.suptitle(
                f'{region} Ski Run Elevation Profiles by Resort\n'
                f'All runs (filtered, smoothed, colored by difficulty)\n'
                f'Labels: difficulty (number of runs): mean, max gradient '
                f'({"degrees" if label_mode == "degrees" else "percent"})\n'
                f'Example: advanced (8): '
                f'{"24.1°, 32.5°" if label_mode == "degrees" else "42%, 56%"} means mean and max steepness for advanced runs',
                fontsize=22, fontweight='bold', y=0.98
            )
            plt.tight_layout(rect=(0, 0.03, 1, 0.96))
            plt.subplots_adjust(hspace=0.36, wspace=0.18)
            out_path = f"charts/ski_run_elevations_matplotlib_{slugify_region(region)}_{label_mode}.png"
            plt.savefig(out_path, dpi=300, bbox_inches='tight')
            plt.close(fig)


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
        runs, points, gradient_stats = load_plot_data(con, target_countries)
        generate_region_plots(runs, points, gradient_stats)
    finally:
        con.close()


if __name__ == "__main__":
    main()