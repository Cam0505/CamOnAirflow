#!/usr/bin/env python3
"""Load raw Japanese ski-field daily weather from JMA using nearby station observations."""

from __future__ import annotations

import argparse
import logging
import math
import os
import re
import time as tyme
from datetime import date, datetime, timedelta, timezone
from io import StringIO

import dlt
import numpy as np
import pandas as pd
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.sources.helpers import requests
from dotenv import load_dotenv

from project_path import get_project_paths, set_dlt_env_vars

paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]

load_dotenv(dotenv_path=ENV_FILE)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

START_DATE = date(2023, 11, 1)
WINTER_MONTHS = {11, 12, 1, 2, 3, 4}
BATCH_SIZE = 500
MAX_STATION_DISTANCE_KM = 25.0
MIN_DISTINCT_ELEVATION_GAP_M = 150.0

STATION_REGEX = re.compile(
    r"javascript:viewPoint\('([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)','([^']*)'\)"
)

JP_RESORTS: list[dict[str, object]] = [
    {"name": "Kiroro Resort", "country": "JP", "region": "Hokkaido", "lat": 43.0795, "lon": 140.9866, "timezone": "Asia/Tokyo", "resort_elevation": 1180},
    {"name": "Rusutsu Resort Ski Area", "country": "JP", "region": "Hokkaido", "lat": 42.7380, "lon": 140.8048, "timezone": "Asia/Tokyo", "resort_elevation": 994},
    {"name": "Mount Racey", "country": "JP", "region": "Hokkaido", "lat": 43.0567, "lon": 142.0017, "timezone": "Asia/Tokyo", "resort_elevation": 1135},
    {"name": "Niseko United", "country": "JP", "region": "Hokkaido", "lat": 42.8625, "lon": 140.7042, "timezone": "Asia/Tokyo", "resort_elevation": 1100},
    {"name": "Furano Ski Resort", "country": "JP", "region": "Hokkaido", "lat": 43.3420, "lon": 142.3830, "timezone": "Asia/Tokyo", "resort_elevation": 950},
    {"name": "Tomamu Ski Resort", "country": "JP", "region": "Hokkaido", "lat": 43.0811, "lon": 142.6203, "timezone": "Asia/Tokyo", "resort_elevation": 1239},
    {"name": "Hakodate Nanae Snowpark", "country": "JP", "region": "Hokkaido", "lat": 41.9536, "lon": 140.7450, "timezone": "Asia/Tokyo", "resort_elevation": 950},
    {"name": "Sahoro", "country": "JP", "region": "Hokkaido", "lat": 43.1430, "lon": 142.9770, "timezone": "Asia/Tokyo", "resort_elevation": 1000},
    {"name": "Appi Kogen Ski Resort", "country": "JP", "region": "Honshu", "lat": 40.0054, "lon": 140.9602, "timezone": "Asia/Tokyo", "resort_elevation": 1180},
    {"name": "Shizukuishi Ski Resort", "country": "JP", "region": "Honshu", "lat": 39.6927, "lon": 140.9757, "timezone": "Asia/Tokyo", "resort_elevation": 1132},
    {"name": "Takasu Snow Park", "country": "JP", "region": "Honshu", "lat": 35.9358, "lon": 136.8849, "timezone": "Asia/Tokyo", "resort_elevation": 1550},
    {"name": "Zao Onsen Ski Resort", "country": "JP", "region": "Honshu", "lat": 38.1666, "lon": 140.4175, "timezone": "Asia/Tokyo", "resort_elevation": 1550},
    {"name": "Miyagi Zao Eboshi Resort", "country": "JP", "region": "Honshu", "lat": 38.1368, "lon": 140.5317, "timezone": "Asia/Tokyo", "resort_elevation": 1350},
]


def parse_jma_number(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (float, np.floating)) and pd.isna(value):
        return None
    text = str(value).replace(",", "").strip()
    if text in {"///", "--", "×", "", "nan"}:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    return float(match.group()) if match else None


def coerce_float(value: object, default: float = 0.0) -> float:
    parsed = parse_jma_number(value)
    return parsed if parsed is not None else default


def get_request_year(day: date) -> int:
    """Return the ski season year so Nov-Dec and Jan-Apr are grouped together."""
    return day.year + 1 if day.month in {11, 12} else day.year


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


def get_jp_resorts_with_timestamp() -> list[dict[str, object]]:
    now = datetime.now(timezone.utc).isoformat()
    return [{**resort, "last_updated": now} for resort in JP_RESORTS]


def fetch_prefecture_station_page(prec_no: int) -> list[dict[str, object]]:
    url = f"https://www.data.jma.go.jp/stats/etrn/select/prefecture.php?prec_no={prec_no:02d}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    text = response.text
    if "javascript:viewPoint" not in text:
        return []

    rows: list[dict[str, object]] = []
    for match in STATION_REGEX.finditer(text):
        values = match.groups()
        _, block_no, station_name, _, lat_d, lat_m, lon_d, lon_m, height, f_pre, _, f_tem, _, f_snc, _, *_ = values
        rows.append(
            {
                "prec_no": f"{prec_no:02d}",
                "block_no": block_no,
                "station_name": station_name,
                "station_lat": float(lat_d) + float(lat_m) / 60.0,
                "station_lon": float(lon_d) + float(lon_m) / 60.0,
                "station_elevation": float(height),
                "has_precip": int(f_pre),
                "has_temp": int(f_tem),
                "has_snow": int(f_snc),
            }
        )
    return rows


def load_jma_station_directory() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for prec_no in range(1, 100):
        rows.extend(fetch_prefecture_station_page(prec_no))

    stations = pd.DataFrame(rows)
    if stations.empty:
        raise ValueError("No JMA stations parsed from the official directory.")

    stations = stations.drop_duplicates(subset=["prec_no", "block_no"])
    stations = stations[
        (stations["has_precip"] == 1)
        & (stations["has_temp"] == 1)
        & (stations["has_snow"] == 1)
    ].copy()
    return stations


def get_japanese_ski_fields_with_timestamp() -> list[dict[str, object]]:
    resorts = get_jp_resorts_with_timestamp()
    stations = load_jma_station_directory()

    matched_rows: list[dict[str, object]] = []
    for resort in resorts:
        resort_lat = coerce_float(resort.get("lat"))
        resort_lon = coerce_float(resort.get("lon"))
        resort_elevation = coerce_float(resort.get("resort_elevation"))

        candidates = stations.copy()
        candidates["distance_km"] = candidates.apply(
            lambda row: haversine_km(
                resort_lat,
                resort_lon,
                float(row["station_lat"]),
                float(row["station_lon"]),
            ),
            axis=1,
        )
        best = candidates.nsmallest(1, "distance_km").iloc[0]
        elev_gap_m = resort_elevation - float(best["station_elevation"])

        if float(best["distance_km"]) <= MAX_STATION_DISTANCE_KM and elev_gap_m >= MIN_DISTINCT_ELEVATION_GAP_M:
            matched_rows.append(
                {
                    "name": str(resort["name"]),
                    "country": str(resort["country"]),
                    "region": str(resort["region"]),
                    "lat": resort_lat,
                    "lon": resort_lon,
                    "timezone": str(resort["timezone"]),
                    "resort_elevation": resort_elevation,
                    "station_name": str(best["station_name"]),
                    "station_prec_no": str(best["prec_no"]),
                    "station_block_no": str(best["block_no"]),
                    "station_elevation": float(best["station_elevation"]),
                    "station_distance_km": float(best["distance_km"]),
                    "last_updated": str(resort["last_updated"]),
                }
            )

    if not matched_rows:
        raise ValueError("No Japanese resorts found with a nearby snow-capable JMA station.")

    logger.info("Matched %s Japanese ski resorts to nearby JMA stations", len(matched_rows))
    return matched_rows


def get_all_missing_date_ranges_by_year(
    logger: logging.Logger,
    locations: list[dict[str, object]],
    start_date: date,
    end_date: date,
    dataset: pd.DataFrame | None,
    daily_default: bool = False,
):
    """Return missing date ranges by resort and request year."""
    try:
        table_truncated = dataset is None or dataset.empty
        missing_ranges: dict[str, dict[object, tuple[date, date]]] = {}
        default_applied = False
        safe_dataset = dataset if dataset is not None else pd.DataFrame()

        last_14_start = end_date - timedelta(days=13)
        last_14_end = end_date
        last_14_set = set(pd.date_range(last_14_start, last_14_end).date)

        dataset_locations = set(safe_dataset["location"].unique()) if not safe_dataset.empty else set()

        for loc in locations:
            name = str(loc["name"])
            all_dates = pd.date_range(start_date, end_date)
            winter_dates = [d.date() for d in all_dates if d.month in WINTER_MONTHS]

            if table_truncated or name not in dataset_locations:
                missing = set(winter_dates)
            else:
                loc_df = safe_dataset[safe_dataset["location"] == name]
                existing_dates = set(pd.to_datetime(loc_df["date"]).dt.date)
                missing = set(winter_dates) - existing_dates

            season_buckets: dict[object, list[date]] = {}
            for missing_day in sorted(missing):
                season_year = get_request_year(missing_day)
                season_buckets.setdefault(season_year, []).append(missing_day)

            season_ranges: dict[object, tuple[date, date]] = {
                season_year: (min(days), max(days)) for season_year, days in season_buckets.items()
            }
            applied = False
            expanded = False

            if daily_default:
                for season, (rng_start, rng_end) in list(season_ranges.items()):
                    rng_set = set(pd.date_range(rng_start, rng_end).date)
                    if last_14_set.issubset(rng_set):
                        applied = True
                        break
                    if last_14_set & rng_set:
                        new_start = min(rng_start, last_14_start)
                        new_end = max(rng_end, last_14_end)
                        del season_ranges[season]
                        season_ranges["default_14d"] = (new_start, new_end)
                        applied = True
                        expanded = True
                        break

                if not applied and not expanded:
                    season_ranges["default_14d"] = (last_14_start, last_14_end)
                    applied = True

            missing_ranges[name] = season_ranges
            if applied:
                default_applied = True

        return missing_ranges, table_truncated, default_applied

    except Exception as exc:
        logger.error("Failed to retrieve missing date ranges: %s", exc)
        missing_ranges = {}
        for loc in locations:
            all_days = [d.date() for d in pd.date_range(start_date, end_date) if d.month in WINTER_MONTHS]
            fallback_buckets: dict[object, list[date]] = {}
            for current_day in all_days:
                season_year = get_request_year(current_day)
                fallback_buckets.setdefault(season_year, []).append(current_day)
            missing_ranges[str(loc["name"])] = {
                season_year: (min(days), max(days)) for season_year, days in fallback_buckets.items()
            }
        return missing_ranges, False, False


def find_column(columns: list[str], required_terms: list[str], excluded_terms: list[str] | None = None) -> str | None:
    excluded_terms = excluded_terms or []
    for column in columns:
        if all(term in column for term in required_terms) and not any(term in column for term in excluded_terms):
            return column
    return None


def fetch_jma_month(location: dict[str, object], year: int, month: int) -> pd.DataFrame:
    endpoints = ["daily_a1.php", "daily_s1.php"]
    html = None

    for endpoint in endpoints:
        url = (
            f"https://www.data.jma.go.jp/stats/etrn/view/{endpoint}"
            f"?prec_no={location['station_prec_no']}&block_no={location['station_block_no']}&year={year}&month={month}&day=&view="
        )
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        candidate_html = response.text
        if "ページを表示することが出来ませんでした" not in candidate_html:
            html = candidate_html
            break

    if html is None:
        raise RuntimeError(
            f"JMA daily page failed for {location['name']} ({location['station_prec_no']}-{location['station_block_no']}) {year}-{month:02d}."
        )

    tables = pd.read_html(StringIO(html), flavor="lxml")
    if not tables:
        raise RuntimeError(
            f"No JMA table found for {location['name']} ({location['station_prec_no']}-{location['station_block_no']}) {year}-{month:02d}."
        )

    table = tables[0]
    table.columns = [
        " ".join(str(level) for level in col if str(level) != "nan").strip()
        for col in table.columns
    ]
    columns = list(table.columns)

    day_col = columns[0]
    precip_col = find_column(columns, ["降水量", "合計"])
    temp_mean_col = find_column(columns, ["気温", "平均"])
    temp_max_col = find_column(columns, ["気温", "最高"])
    temp_min_col = find_column(columns, ["気温", "最低"])
    humidity_mean_col = find_column(columns, ["湿度", "平均"])
    snowfall_col = (
        find_column(columns, ["降雪の深さ", "合計"])
        or find_column(columns, ["雪", "降雪", "合計"])
    )
    snow_depth_col = find_column(columns, ["最深積雪"])

    if precip_col is None or temp_mean_col is None:
        raise RuntimeError(
            f"Expected JMA precip/temp columns not found for {location['name']} {year}-{month:02d}."
        )

    data = pd.DataFrame({"day": pd.to_numeric(table[day_col], errors="coerce")})
    data = data[data["day"].notna()].copy()
    data["date"] = pd.to_datetime({"year": year, "month": month, "day": data["day"].astype(int)})

    def map_column(column_name: str | None) -> pd.Series:
        if column_name is None:
            return pd.Series(np.nan, index=data.index, dtype="float64")
        parsed = table.loc[data.index, column_name].map(parse_jma_number)
        return pd.to_numeric(parsed, errors="coerce").astype("float64")

    data["precipitation_sum"] = map_column(precip_col)
    data["temperature_mean"] = map_column(temp_mean_col)
    data["temperature_max"] = map_column(temp_max_col)
    data["temperature_min"] = map_column(temp_min_col)
    data["relative_humidity_mean"] = map_column(humidity_mean_col)
    data["snowfall"] = map_column(snowfall_col).fillna(0.0)
    data["snow_depth"] = map_column(snow_depth_col).fillna(0.0)

    return data[[
        "date",
        "precipitation_sum",
        "temperature_mean",
        "temperature_max",
        "temperature_min",
        "relative_humidity_mean",
        "snowfall",
        "snow_depth",
    ]]


def fetch_jma_data(location: dict[str, object], start_date: date, end_date: date) -> pd.DataFrame:
    logger.debug(
        "Fetching JMA daily data for %s from %s to %s using station %s",
        location["name"],
        start_date,
        end_date,
        location["station_name"],
    )

    month_starts = pd.period_range(start=start_date, end=end_date, freq="M")
    frames: list[pd.DataFrame] = []

    for period in month_starts:
        if period.month not in WINTER_MONTHS:
            continue
        try:
            monthly = fetch_jma_month(location, period.year, period.month)
            frames.append(monthly)
            tyme.sleep(0.2)
        except Exception as exc:
            logger.warning(
                "Failed JMA fetch for %s %s-%02d: %s",
                location["name"],
                period.year,
                period.month,
                exc,
            )

    if not frames:
        return pd.DataFrame()

    data = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["date"])
    data = data[(data["date"].dt.date >= start_date) & (data["date"].dt.date <= end_date)].copy()

    resort_elevation = coerce_float(location.get("resort_elevation", 0.0))
    station_elevation = coerce_float(location.get("station_elevation", 0.0))

    data["location"] = str(location["name"])
    data["country"] = "JP"
    data["region"] = str(location.get("region", ""))
    data["timezone"] = str(location["timezone"])
    data["source_name"] = "JMA"
    data["station_name"] = str(location["station_name"])
    data["station_prec_no"] = str(location["station_prec_no"])
    data["station_block_no"] = str(location["station_block_no"])
    data["station_elevation"] = station_elevation
    data["station_distance_km"] = coerce_float(location.get("station_distance_km", 0.0))
    data["resort_elevation"] = resort_elevation
    data["last_updated"] = str(location["last_updated"])
    return data


@dlt.source
def japan_jma_source(logger: logging.Logger, dataset: pd.DataFrame | None, run_from_date: date | None = None):
    locations = get_japanese_ski_fields_with_timestamp()

    @dlt.resource(write_disposition="merge", name="jp_ski_field_weather", primary_key=["location", "date"])
    def jp_ski_field_weather():
        state = dlt.current.source_state().setdefault(
            "japan_jma_weather",
            {
                "Daily_Requests": {},
                "Processed_Ranges": {},
                "Daily_default": {},
            },
        )

        today = date.today()
        today_str = str(today)
        end_date = today - timedelta(days=2)
        state["Daily_Requests"] = {today_str: state.get("Daily_Requests", {}).get(today_str, 0)}
        state["Daily_default"] = {today_str: state.get("Daily_default", {}).get(today_str, True)}

        effective_start_date = START_DATE
        if run_from_date is not None:
            effective_start_date = max(run_from_date, START_DATE)
            logger.info(
                "Start date provided: collecting raw Japan JMA data from %s to %s",
                effective_start_date,
                end_date,
            )
            missing_ranges_by_location, table_truncated, default_applied = get_all_missing_date_ranges_by_year(
                logger,
                locations,
                effective_start_date,
                end_date,
                None,
                daily_default=False,
            )
        else:
            missing_ranges_by_location, table_truncated, default_applied = get_all_missing_date_ranges_by_year(
                logger,
                locations,
                effective_start_date,
                end_date,
                dataset,
                daily_default=bool(state["Daily_default"][today_str]),
            )

        if table_truncated:
            state["Processed_Ranges"] = {}

        logger.info("Starting Japan JMA weather collection for %s resorts", len(locations))

        for location in locations:
            location_name = str(location["name"])

            request_ranges = missing_ranges_by_location.get(location_name, {})
            if not request_ranges:
                logger.info("No missing ranges for %s, skipping", location_name)
                continue

            for request_year, (range_start, range_end) in request_ranges.items():
                logger.info(
                    "Requesting raw JMA data for %s year bucket %s: %s to %s",
                    location_name,
                    request_year,
                    range_start,
                    range_end,
                )

                try:
                    data = fetch_jma_data(location, range_start, range_end)
                    state["Daily_Requests"][today_str] = state["Daily_Requests"].get(today_str, 0) + 1

                    if data.empty:
                        logger.warning("No data returned for %s (%s to %s)", location_name, range_start, range_end)
                        continue

                    state.setdefault("Processed_Ranges", {}).setdefault(location_name, []).append(
                        {
                            "request_year": str(request_year),
                            "start_date": str(range_start),
                            "end_date": str(range_end),
                            "loaded_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )

                    for i in range(0, len(data), BATCH_SIZE):
                        yield data.iloc[i : i + BATCH_SIZE].to_dict(orient="records")

                except Exception as exc:
                    logger.error("Error processing %s %s to %s: %s", location_name, range_start, range_end, exc)

                tyme.sleep(0.3)

        if default_applied:
            state["Daily_default"][today_str] = False

        logger.info("Completed Japan JMA weather collection")

    return jp_ski_field_weather


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Japan JMA raw ski-field daily weather pipeline")
    parser.add_argument(
        "--start-date",
        type=str,
        help="Optional start date for fetching Japan JMA data (format: YYYY-MM-DD)",
    )
    args = parser.parse_args()

    run_from_date = None
    if args.start_date:
        try:
            parsed_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
            if parsed_date < START_DATE:
                logger.warning("Start date %s is before %s; clamping to %s", parsed_date, START_DATE, START_DATE)
                parsed_date = START_DATE
            run_from_date = parsed_date
            logger.info("Will run Japan JMA pipeline from specified start date: %s", run_from_date)
        except ValueError as exc:
            logger.error("Invalid start date format: %s. Please use YYYY-MM-DD.", exc)
            raise

    pipeline = dlt.pipeline(
        pipeline_name="japan_jma_weather_pipeline",
        destination=os.getenv("DLT_DESTINATION", "motherduck"),
        dataset_name="weather_jp",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False,
    )

    dataset = None

    if run_from_date is None:
        try:
            ds = pipeline.dataset()
            try:
                dataset = ds["jp_ski_field_weather"].df()
            except Exception as exc:
                logger.warning("Could not load full 'jp_ski_field_weather' table (continuing with empty dataset): %s", exc)
                dataset = None
        except PipelineNeverRan:
            logger.warning("No previous runs found for the Japan JMA pipeline. Assuming first run.")
        except DatabaseUndefinedRelation:
            logger.warning("Table does not exist yet for the Japan JMA pipeline. Assuming first run.")
        except Exception as exc:
            logger.warning("Unexpected error while probing Japan JMA dataset; continuing with empty dataset: %s", exc)
            dataset = None
    else:
        logger.info("Start date provided; skipping remote dataset fetch and running with dataset=None.")

    try:
        logger.info("Running Japan JMA pipeline...")
        source = japan_jma_source(logger, dataset, run_from_date)
        load_info = pipeline.run(source)

        state = source.state.get("japan_jma_weather", {})
        logger.info("Daily Requests: %s", state.get("Daily_Requests", {}))
        logger.info("Processed Ranges: %s", sum(len(v) for v in state.get("Processed_Ranges", {}).values()))
        logger.info("Pipeline run completed. Load Info: %s", load_info)


    except Exception as exc:
        logger.error("Japan JMA pipeline run failed: %s", exc)
        raise
