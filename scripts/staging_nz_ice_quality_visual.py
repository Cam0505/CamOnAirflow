import argparse
import datetime
import os
import re
from itertools import groupby
from operator import itemgetter

import duckdb
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv

from project_path import get_project_paths, set_dlt_env_vars


DEFAULT_TABLE_CANDIDATES = [
    "camonairflow.public_staging.staging_nz_ice_quality_estimate",
    "public_staging.staging_nz_ice_quality_estimate",
    "staging_nz_ice_quality_estimate",
]

CATEGORY_COLORS = {
    "Excellent Ice": "#1a9850",
    "Good Ice": "#66bd63",
    "Marginal / Variable Ice": "#fee08b",
    "Poor / Thawing Ice": "#fdae61",
    "No Usable Ice": "#d73027",
    "Unknown / Missing Spectral Signal": "#bdbdbd",
}

SENTINEL_COLORS = {
    "Good Ice Conditions": "#66c2a5",
    "Wet/Thawing Ice": "#fc8d62",
    "Dry/Brittle Ice": "#8da0cb",
    "Patchy Ice/Snow": "#ffd92f",
    "Patchy & Wet": "#fdb863",
    "Uncertain Ice": "#bdbdbd",
    "Bare Rock or Error": "#e78ac3",
}


def sanitize_filename(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "_", value.strip())
    return cleaned.strip("_") or "unknown_location"


def resolve_table_name(con: duckdb.DuckDBPyConnection, override: str | None) -> str:
    if override:
        con.execute(f"select 1 from {override} limit 1")
        return override

    for candidate in DEFAULT_TABLE_CANDIDATES:
        try:
            con.execute(f"select 1 from {candidate} limit 1")
            return candidate
        except Exception:
            continue

    raise ValueError(
        "Could not resolve staging table. Provide --table explicitly."
    )


def load_data(con: duckdb.DuckDBPyConnection, table_name: str, location: str | None) -> pd.DataFrame:
    location_filter = ""
    if location:
        escaped = location.replace("'", "''")
        location_filter = f"where location = '{escaped}'"

    query = f"""
        select
            cast(obs_date as date) as obs_date,
            location,
            ice_condition_category_nz,
            ice_quality_bucket_nz,
            ice_quality_estimate_score_0_1,
            spectral_ice_score_0_1,
            freeze_support_score_0_1,
            melt_pressure_score_0_1,
            temperature_mean_c,
            precipitation_total_mm,
            freezing_hours,
            thaw_hours
        from {table_name}
        {location_filter}
        order by location, obs_date
    """
    return con.execute(query).df()


def add_category_bands(ax, dates: pd.Series, labels: pd.Series, color_map: dict, y_text: float, alpha: float) -> None:
    for label, group in groupby(enumerate(labels), key=itemgetter(1)):
        indices = [i for i, _ in group]
        start = dates.iloc[indices[0]]
        end = dates.iloc[indices[-1]] + datetime.timedelta(days=1)
        duration = (end - start).days

        ax.axvspan(start, end, color=color_map.get(label, "#ffffff"), alpha=alpha)

        if duration >= 12:
            midpoint = start + (end - start) / 2
            ax.text(
                midpoint,
                y_text,
                label,
                fontsize=7,
                ha="center",
                va="bottom",
                rotation=90,
                alpha=0.75,
            )


def plot_location(df_loc: pd.DataFrame, location: str, output_dir: str) -> str:
    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)

    # Panel 1: main quality scores
    axes[0].plot(df_loc["obs_date"], df_loc["ice_quality_estimate_score_0_1"], label="Overall quality", linewidth=2.2)
    axes[0].plot(df_loc["obs_date"], df_loc["spectral_ice_score_0_1"], label="Spectral score", linewidth=1.4, alpha=0.85)
    axes[0].plot(df_loc["obs_date"], df_loc["freeze_support_score_0_1"], label="Freeze support", linewidth=1.4, alpha=0.85)
    axes[0].plot(df_loc["obs_date"], 1.0 - df_loc["melt_pressure_score_0_1"], label="Inverse melt pressure", linewidth=1.4, alpha=0.85)
    axes[0].set_ylim(0, 1)
    axes[0].set_ylabel("Score (0-1)")
    axes[0].grid(True, alpha=0.25)
    axes[0].legend(loc="lower left", ncols=2)

    # Panel 2: key weather signals
    ax2_right = axes[1].twinx()
    axes[1].bar(df_loc["obs_date"], df_loc["precipitation_total_mm"], width=0.9, alpha=0.35, label="Precip mm", color="#2c7fb8")
    axes[1].plot(df_loc["obs_date"], df_loc["freezing_hours"], label="Freezing hrs", color="#2166ac", linewidth=1.4)
    axes[1].plot(df_loc["obs_date"], df_loc["thaw_hours"], label="Thaw hrs", color="#f46d43", linewidth=1.4)
    ax2_right.plot(df_loc["obs_date"], df_loc["temperature_mean_c"], label="Temp C", color="#d7301f", linewidth=1.4, linestyle="--")
    axes[1].set_ylabel("Hours / Precip")
    ax2_right.set_ylabel("Temp (C)")
    axes[1].grid(True, alpha=0.25)

    left_handles, left_labels = axes[1].get_legend_handles_labels()
    right_handles, right_labels = ax2_right.get_legend_handles_labels()
    axes[1].legend(left_handles + right_handles, left_labels + right_labels, loc="upper left", ncols=2)

    # Panel 3: categorical bands
    axes[2].set_ylim(0, 1)
    add_category_bands(
        axes[2],
        df_loc["obs_date"],
        df_loc["ice_quality_bucket_nz"],
        CATEGORY_COLORS,
        y_text=0.03,
        alpha=0.35,
    )
    add_category_bands(
        axes[2],
        df_loc["obs_date"],
        df_loc["ice_condition_category_nz"],
        SENTINEL_COLORS,
        y_text=0.52,
        alpha=0.18,
    )
    axes[2].set_yticks([])
    axes[2].set_ylabel("Category")

    axes[2].xaxis.set_major_locator(mdates.AutoDateLocator(minticks=10, maxticks=24))
    axes[2].xaxis.set_major_formatter(mdates.ConciseDateFormatter(axes[2].xaxis.get_major_locator()))

    fig.suptitle(f"NZ Ice Quality Staging Overview: {location}", fontsize=14)
    fig.tight_layout()

    filename = f"{sanitize_filename(location)}_staging_nz_ice_quality.png"
    output_path = os.path.join(output_dir, filename)
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Visualize NZ staging ice quality, one image per location.")
    parser.add_argument("--table", default=None, help="Fully qualified table name to query.")
    parser.add_argument(
        "--output-dir",
        default="charts/nz_ice_quality",
        help="Directory where location charts are written.",
    )
    parser.add_argument(
        "--location",
        default=None,
        help="Optional single location filter. If omitted, renders all locations.",
    )
    args = parser.parse_args()

    paths = get_project_paths()
    set_dlt_env_vars(paths)
    load_dotenv(dotenv_path=paths["ENV_FILE"])

    database_string = os.getenv("MD")
    if not database_string:
        raise ValueError("Missing MD in environment.")

    os.makedirs(args.output_dir, exist_ok=True)

    con = duckdb.connect(database_string)
    table_name = resolve_table_name(con, args.table)
    df = load_data(con, table_name, args.location)

    if df.empty:
        raise ValueError("No rows returned from staging table for the selected filter.")

    df["obs_date"] = pd.to_datetime(df["obs_date"])
    rendered = 0

    for location, df_loc in df.groupby("location", sort=True):
        location_name = str(location)
        df_loc = df_loc.sort_values("obs_date").reset_index(drop=True)
        output_path = plot_location(df_loc, location_name, args.output_dir)
        rendered += 1
        print(f"Saved: {output_path}")

    print(f"Rendered {rendered} location chart(s) from {table_name}")


if __name__ == "__main__":
    main()
