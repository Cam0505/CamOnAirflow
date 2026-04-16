#!/usr/bin/env python3
"""Graph elevation-adjusted JMA snowfall for Japanese ski resorts from the ski runs pipeline."""

from __future__ import annotations

import math
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

from project_path import get_project_paths, set_dlt_env_vars

paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])

CHARTS_DIR = str(paths["CHARTS_DIR"])
os.makedirs(CHARTS_DIR, exist_ok=True)

START_SEASON_YEAR = 2017
END_SEASON_YEAR = 2026
MAX_STATION_DISTANCE_KM = 25.0
MIN_DISTINCT_ELEVATION_GAP_M = 150.0
SNOW_LAPSE_RATE_C_PER_KM = 6.5
SNOW_FULL_BELOW_C = -1.5
RAIN_ALL_ABOVE_C = 3.0

STATION_REGEX = re.compile(
    r"javascript:viewPoint\('([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)'\)"
)


def get_connection() -> duckdb.DuckDBPyConnection:
    database_string = os.getenv("MD")
    if not database_string:
        raise ValueError("Missing MD in environment.")
    return duckdb.connect(database_string)


def format_season_label(season_year: str) -> str:
    year = int(season_year)
    return f"{year - 1}-{str(year)[-2:]}"


def infer_season_year(date_value: pd.Timestamp) -> str:
    return str(date_value.year + 1 if date_value.month in (11, 12) else date_value.year)


def parse_jma_number(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (float, np.floating)) and pd.isna(value):
        return 0.0
    text = str(value).replace(",", "").strip()
    if text in {"///", "--", "×", "", "nan"}:
        return 0.0
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    return float(match.group()) if match else 0.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return 2 * radius_km * math.asin(math.sqrt(a))


def load_jp_resorts() -> pd.DataFrame:
    query = """
        SELECT
            resort,
            AVG((top_lat + bottom_lat) / 2.0) AS lat,
            AVG((top_lon + bottom_lon) / 2.0) AS lon,
            MAX(top_elevation_m) AS resort_elevation_m
        FROM camonairflow.ski_runs.ski_runs
        WHERE country_code = 'JP'
        GROUP BY resort
        ORDER BY resort
    """
    con = get_connection()
    try:
        df = con.execute(query).df()
    finally:
        con.close()

    if df.empty:
        raise ValueError("No Japanese ski resorts found in the ski runs pipeline output.")
    return df


def fetch_prefecture_station_page(prec_no: int, session: requests.Session) -> list[dict[str, object]]:
    url = f"https://www.data.jma.go.jp/stats/etrn/select/prefecture.php?prec_no={prec_no:02d}"
    response = session.get(url, timeout=30)
    response.raise_for_status()
    text = response.text
    if "javascript:viewPoint" not in text:
        return []

    stations: list[dict[str, object]] = []
    for match in STATION_REGEX.finditer(text):
        values = match.groups()
        station_type, block_no, name, _, lat_d, lat_m, lon_d, lon_m, height, f_pre, _, f_tem, _, f_snc, _, *_ = values
        stations.append(
            {
                "prec_no": f"{prec_no:02d}",
                "block_no": block_no,
                "station_type": station_type,
                "station_name": name,
                "station_lat": float(lat_d) + float(lat_m) / 60.0,
                "station_lon": float(lon_d) + float(lon_m) / 60.0,
                "station_elevation_m": float(height),
                "has_precip": int(f_pre),
                "has_temp": int(f_tem),
                "has_snow": int(f_snc),
            }
        )
    return stations


def load_jma_station_directory() -> pd.DataFrame:
    session = requests.Session()
    rows: list[dict[str, object]] = []
    for prec_no in range(1, 100):
        rows.extend(fetch_prefecture_station_page(prec_no, session))

    stations = pd.DataFrame(rows)
    if stations.empty:
        raise ValueError("No JMA stations parsed from the official directory pages.")

    stations = stations.drop_duplicates(subset=["prec_no", "block_no"])
    stations = stations[
        (stations["has_snow"] == 1)
        & (stations["has_temp"] == 1)
        & (stations["has_precip"] == 1)
    ].copy()
    return stations


def match_resorts_to_stations(resorts: pd.DataFrame, stations: pd.DataFrame) -> pd.DataFrame:
    matched_rows: list[dict[str, object]] = []
    for _, resort in resorts.iterrows():
        candidates = stations.copy()
        candidates["distance_km"] = candidates.apply(
            lambda row: haversine_km(
                float(resort["lat"]),
                float(resort["lon"]),
                float(row["station_lat"]),
                float(row["station_lon"]),
            ),
            axis=1,
        )
        best = candidates.nsmallest(1, "distance_km").iloc[0]
        elev_gap_m = float(resort["resort_elevation_m"]) - float(best["station_elevation_m"])
        if float(best["distance_km"]) <= MAX_STATION_DISTANCE_KM and elev_gap_m >= MIN_DISTINCT_ELEVATION_GAP_M:
            matched_rows.append(
                {
                    "resort": resort["resort"],
                    "resort_lat": float(resort["lat"]),
                    "resort_lon": float(resort["lon"]),
                    "resort_elevation_m": float(resort["resort_elevation_m"]),
                    "station_name": str(best["station_name"]),
                    "station_type": str(best["station_type"]),
                    "prec_no": str(best["prec_no"]),
                    "block_no": str(best["block_no"]),
                    "station_elevation_m": float(best["station_elevation_m"]),
                    "distance_km": float(best["distance_km"]),
                    "elev_gap_m": elev_gap_m,
                }
            )

    matched = pd.DataFrame(matched_rows).sort_values("resort").reset_index(drop=True)
    if matched.empty:
        raise ValueError("No Japanese resorts met the nearby-station and elevation-gap filters.")
    return matched


def fetch_jma_month(station_type: str, prec_no: str, block_no: str, year: int, month: int, session: requests.Session) -> pd.DataFrame:
    endpoint_candidates = [f"daily_{station_type}1.php"]
    if station_type == "a":
        endpoint_candidates.append("daily_s1.php")
    elif station_type == "s":
        endpoint_candidates.append("daily_a1.php")

    html = None
    for endpoint in endpoint_candidates:
        url = (
            f"https://www.data.jma.go.jp/stats/etrn/view/{endpoint}"
            f"?prec_no={prec_no}&block_no={block_no}&year={year}&month={month}&day=&view="
        )
        response = session.get(url, timeout=30)
        response.raise_for_status()
        candidate_html = response.text
        if "ページを表示することが出来ませんでした" not in candidate_html:
            html = candidate_html
            break

    if html is None:
        raise RuntimeError(f"JMA daily page failed for station {prec_no}-{block_no} {year}-{month:02d}.")

    tables = pd.read_html(StringIO(html), flavor="lxml")
    if not tables:
        raise RuntimeError(f"No JMA table found for station {prec_no}-{block_no} {year}-{month:02d}.")

    table = tables[0]
    table.columns = [
        " ".join(str(level) for level in col if str(level) != "nan").strip()
        for col in table.columns
    ]

    day_col = table.columns[0]
    precip_col = next((c for c in table.columns if "降水量" in c and "合計" in c), None)
    temp_col = next((c for c in table.columns if "気温" in c and "平均" in c), None)
    if precip_col is None or temp_col is None:
        raise RuntimeError(f"Expected JMA precip/temp columns not found for station {prec_no}-{block_no} {year}-{month:02d}.")

    out = table[[day_col, precip_col, temp_col]].copy()
    out.columns = ["day", "jma_precip_mm", "jma_temp_mean_c"]
    out["day"] = pd.to_numeric(out["day"], errors="coerce")
    out = out[out["day"].notna()].copy()
    out["date"] = pd.to_datetime({"year": year, "month": month, "day": out["day"].astype(int)})
    out["jma_precip_mm"] = out["jma_precip_mm"].map(parse_jma_number).astype(float)
    out["jma_temp_mean_c"] = out["jma_temp_mean_c"].map(parse_jma_number).astype(float)
    out["prec_no"] = prec_no
    out["block_no"] = block_no
    return out[["prec_no", "block_no", "date", "jma_precip_mm", "jma_temp_mean_c"]]


def fetch_all_station_months(matches: pd.DataFrame) -> pd.DataFrame:
    unique_stations = matches[["station_type", "prec_no", "block_no"]].drop_duplicates().itertuples(index=False)
    tasks: list[tuple[str, str, str, int, int]] = []
    for station in unique_stations:
        for season_year in range(START_SEASON_YEAR, END_SEASON_YEAR + 1):
            tasks.extend(
                [
                    (str(station.station_type), str(station.prec_no), str(station.block_no), season_year - 1, 11),
                    (str(station.station_type), str(station.prec_no), str(station.block_no), season_year - 1, 12),
                    (str(station.station_type), str(station.prec_no), str(station.block_no), season_year, 1),
                    (str(station.station_type), str(station.prec_no), str(station.block_no), season_year, 2),
                    (str(station.station_type), str(station.prec_no), str(station.block_no), season_year, 3),
                    (str(station.station_type), str(station.prec_no), str(station.block_no), season_year, 4),
                ]
            )

    frames: list[pd.DataFrame] = []
    session = requests.Session()
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_map = {
            executor.submit(fetch_jma_month, station_type, prec_no, block_no, year, month, session): (station_type, prec_no, block_no, year, month)
            for station_type, prec_no, block_no, year, month in tasks
        }
        for future in as_completed(future_map):
            frames.append(future.result())

    station_data = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["prec_no", "block_no", "date"])
    return station_data.sort_values(["prec_no", "block_no", "date"]).reset_index(drop=True)


def apply_elevation_adjustment(df: pd.DataFrame) -> pd.DataFrame:
    elevation_delta_km = np.maximum(df["resort_elevation_m"] - df["station_elevation_m"], 0.0) / 1000.0
    adjusted_temp = df["jma_temp_mean_c"] - SNOW_LAPSE_RATE_C_PER_KM * elevation_delta_km

    snow_fraction = np.where(
        adjusted_temp <= SNOW_FULL_BELOW_C,
        1.0,
        np.where(
            adjusted_temp >= RAIN_ALL_ABOVE_C,
            0.0,
            np.clip((RAIN_ALL_ABOVE_C - adjusted_temp) / (RAIN_ALL_ABOVE_C - SNOW_FULL_BELOW_C), 0.0, 1.0),
        ),
    )

    slr = np.where(
        adjusted_temp <= -8.0,
        22.0,
        np.where(
            adjusted_temp <= -5.0,
            19.0,
            np.where(
                adjusted_temp <= -2.0,
                14.0,
                np.where(adjusted_temp <= 0.0, 12.0, np.where(adjusted_temp <= 1.5, 10.0, 8.0)),
            ),
        ),
    )

    out = df.copy()
    out["season_year"] = out["date"].map(infer_season_year)
    out = out[out["season_year"].astype(int).between(START_SEASON_YEAR, END_SEASON_YEAR)].copy()
    out["adjusted_temp_mean_c"] = adjusted_temp.astype(float)
    out["elev_adjusted_snowfall_cm"] = (out["jma_precip_mm"] * snow_fraction * slr / 10.0).astype(float)
    out["day_of_season"] = out.groupby(["resort", "season_year"]).cumcount() + 1
    out["cumulative_snowfall_cm"] = out.groupby(["resort", "season_year"])["elev_adjusted_snowfall_cm"].cumsum()
    return out


def month_ticks_for_resort(df: pd.DataFrame) -> tuple[list[int], list[str]]:
    month_map = {11: "Nov", 12: "Dec", 1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr"}
    tick_positions: list[int] = []
    tick_labels: list[str] = []
    for month in df["date"].dt.month.drop_duplicates():
        month_rows = df[df["date"].dt.month == month]
        if month_rows.empty:
            continue
        tick_positions.append(int(month_rows["day_of_season"].min()))
        tick_labels.append(month_map.get(int(month), str(month)))
    return tick_positions, tick_labels


def plot_resorts(df: pd.DataFrame, matches: pd.DataFrame, output_path: str) -> None:
    resorts = matches["resort"].tolist()
    seasons = sorted(df["season_year"].dropna().unique(), key=int)
    cmap = plt.get_cmap("tab10")
    color_lookup = {season: cmap(idx % 10) for idx, season in enumerate(seasons)}

    n_panels = len(resorts)
    ncols = 4 if n_panels > 4 else max(1, n_panels)
    nrows = int(np.ceil(n_panels / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(22, 4.8 * nrows), constrained_layout=True)
    axes_flat = np.atleast_1d(axes).flatten()

    for idx, (ax, resort) in enumerate(zip(axes_flat, resorts)):
        panel_df = df[df["resort"] == resort].copy()
        match_row = matches[matches["resort"] == resort].iloc[0]
        tick_positions, tick_labels = month_ticks_for_resort(panel_df)

        for season_year in seasons:
            season_df = panel_df[panel_df["season_year"] == season_year]
            ax.plot(
                season_df["day_of_season"],
                season_df["cumulative_snowfall_cm"],
                color=color_lookup[season_year],
                linewidth=1.8,
                alpha=0.92,
                label=format_season_label(season_year) if idx == 0 else None,
            )

        ax.text(
            0.02,
            0.98,
            f"{match_row['station_name']} | gap {match_row['elev_gap_m']:.0f} m",
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=7.5,
            color="#444444",
        )
        ax.set_title(resort, fontsize=10.5, fontweight="bold")
        ax.set_xlabel("Month")
        ax.set_ylabel("Elev-adjusted snowfall (cm)")
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels)
        ax.grid(True, axis="y", linestyle="--", alpha=0.3)

        if idx == 0:
            ax.legend(frameon=False, fontsize=7.5, ncol=2, loc="upper left")

    for ax in axes_flat[len(resorts):]:
        ax.set_visible(False)

    fig.suptitle(
        "Japanese Ski Resorts: JMA Elevation-Adjusted Seasonal Snowfall\n"
        "One resort per facet, one line per season year, using nearest snow-capable JMA station",
        fontsize=16,
        fontweight="bold",
    )
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    resorts = load_jp_resorts()
    stations = load_jma_station_directory()
    matches = match_resorts_to_stations(resorts, stations)
    station_data = fetch_all_station_months(matches)

    merged = matches.merge(station_data, on=["prec_no", "block_no"], how="left")
    adjusted = apply_elevation_adjustment(merged)

    chart_path = os.path.join(CHARTS_DIR, "japan_jma_elevation_adjusted_resorts_2017_2026.png")
    plot_resorts(adjusted, matches, chart_path)

    summary = (
        adjusted.groupby(["resort", "season_year"], as_index=False)["elev_adjusted_snowfall_cm"]
        .sum()
        .sort_values(by=["resort", "season_year"])
    )

    print(f"Matched Japanese resorts with distinct elevation-adjusted JMA signal: {len(matches)}")
    print("\nResort-to-station matches:")
    print(
        matches[["resort", "station_name", "distance_km", "resort_elevation_m", "station_elevation_m", "elev_gap_m"]]
        .to_string(
            index=False,
            formatters={
                "distance_km": "{:.1f}".format,
                "resort_elevation_m": "{:.0f}".format,
                "station_elevation_m": "{:.0f}".format,
                "elev_gap_m": "{:.0f}".format,
            },
        )
    )
    print(f"\nSaved chart: {chart_path}")
    print("\nSeasonal elevation-adjusted totals preview (cm):")
    print(summary.head(40).to_string(index=False, formatters={"elev_adjusted_snowfall_cm": "{:.1f}".format}))


if __name__ == "__main__":
    main()
