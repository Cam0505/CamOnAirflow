import logging
from dotenv import load_dotenv
from datetime import datetime, date, timedelta, timezone
import pandas as pd
import dlt
from dlt.sources.helpers import requests
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
import os
import time as tyme
from project_path import get_project_paths, set_dlt_env_vars
import argparse

# Load environment variables and set DLT config
paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
DBT_DIR = paths["DBT_DIR"]

load_dotenv(dotenv_path=ENV_FILE)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Ski field locations with dynamic last_updated
# resort_elevation values are approximate representative on-mountain elevations for weather modeling,
# not exact summit elevations.
def get_ski_fields_with_timestamp():
    now = datetime.now(timezone.utc).isoformat()
    return [
        {**field, "last_updated": now}
        for field in [
            # New Zealand 
            {"name": "Remarkables", "country": "NZ", "lat": -45.0661, "lon": 168.8196, "timezone": "Pacific/Auckland", "resort_elevation": 1650},
            {"name": "Cardrona", "country": "NZ", "lat": -44.8746, "lon": 168.9481, "timezone": "Pacific/Auckland", "resort_elevation": 1650},
            {"name": "Treble Cone", "country": "NZ", "lat": -44.6301, "lon": 168.8806, "timezone": "Pacific/Auckland", "resort_elevation": 1300},
            {"name": "Mount Hutt", "country": "NZ", "lat": -43.4707, "lon": 171.5306, "timezone": "Pacific/Auckland", "resort_elevation": 1600},
            {"name": "Ohau", "country": "NZ", "lat": -44.2157, "lon": 169.7711, "timezone": "Pacific/Auckland", "resort_elevation": 1500}, 
            {"name": "Coronet Peak", "country": "NZ", "lat": -44.9206, "lon": 168.7349, "timezone": "Pacific/Auckland", "resort_elevation": 1350}, 
            {"name": "Whakapapa", "country": "NZ", "lat": -39.2659, "lon": 175.5600, "timezone": "Pacific/Auckland", "resort_elevation": 2300},
            {"name": "Turoa", "country": "NZ", "lat": -39.3002, "lon": 175.5525, "timezone": "Pacific/Auckland", "resort_elevation": 2150},
            # Japan
            {"name": "Kiroro Resort", "country": "JP", "lat": 43.0795, "lon": 140.9866, "timezone": "Asia/Tokyo", "resort_elevation": 1180},
            {"name": "Rusutsu Resort Ski Area", "country": "JP", "lat": 42.7380, "lon": 140.8048, "timezone": "Asia/Tokyo", "resort_elevation": 994},
            {"name": "Mount Racey", "country": "JP", "lat": 43.0567, "lon": 142.0017, "timezone": "Asia/Tokyo", "resort_elevation": 1135},
            {"name": "Niseko United", "country": "JP", "lat": 42.8625, "lon": 140.7042, "timezone": "Asia/Tokyo", "resort_elevation": 1100},
            {"name": "Furano Ski Resort", "country": "JP", "lat": 43.3420, "lon": 142.3830, "timezone": "Asia/Tokyo", "resort_elevation": 950},
            {"name": "Tomamu Ski Resort", "country": "JP", "lat": 43.0811, "lon": 142.6203, "timezone": "Asia/Tokyo", "resort_elevation": 1239},
            {"name": "Hakodate Nanae Snowpark", "country": "JP", "lat": 41.9536, "lon": 140.7450, "timezone": "Asia/Tokyo", "resort_elevation": 950},
            {"name": "Sahoro", "country": "JP", "lat": 43.1430, "lon": 142.9770, "timezone": "Asia/Tokyo", "resort_elevation": 1000},
            {"name": "Appi Kogen Ski Resort", "country": "JP", "lat": 40.0054, "lon": 140.9602, "timezone": "Asia/Tokyo", "resort_elevation": 1180},
            {"name": "Shizukuishi Ski Resort", "country": "JP", "lat": 39.6927, "lon": 140.9757, "timezone": "Asia/Tokyo", "resort_elevation": 1132},
            {"name": "Takasu Snow Park", "country": "JP", "lat": 35.9358, "lon": 136.8849, "timezone": "Asia/Tokyo", "resort_elevation": 1550},
            {"name": "Zao Onsen Ski Resort", "country": "JP", "lat": 38.1666, "lon": 140.4175, "timezone": "Asia/Tokyo", "resort_elevation": 1550},
            {"name": "Miyagi Zao Eboshi Resort", "country": "JP", "lat": 38.1368, "lon": 140.5317, "timezone": "Asia/Tokyo", "resort_elevation": 1350},
            # Australia
            {"name": "Thredbo", "country": "AU", "lat": -36.5040, "lon": 148.2987, "timezone": "Australia/Sydney", "resort_elevation": 1550},
            {"name": "Perisher", "country": "AU", "lat": -36.4058, "lon": 148.4134, "timezone": "Australia/Sydney", "resort_elevation": 1700},
            {"name": "Mt Buller", "country": "AU", "lat": -37.1467, "lon": 146.4473, "timezone": "Australia/Melbourne", "resort_elevation": 1650},
            {"name": "Falls Creek", "country": "AU", "lat": -36.8655, "lon": 147.2861, "timezone": "Australia/Melbourne", "resort_elevation": 1700},
            {"name": "Mt Hotham", "country": "AU", "lat": -36.9762, "lon": 147.1359, "timezone": "Australia/Melbourne", "resort_elevation": 1750},
            
            # Unsupported until regional snowpack profiles are calibrated:
            # {"name": "Valle Nevado", "country": "CL", "lat": -33.3556, "lon": -70.2489, "timezone": "America/Santiago", "resort_elevation": 3200},
            # {"name": "Portillo", "country": "CL", "lat": -32.8352, "lon": -70.1309, "timezone": "America/Santiago", "resort_elevation": 3050},
            # {"name": "La Parva", "country": "CL", "lat": -33.3319, "lon": -70.2917, "timezone": "America/Santiago", "resort_elevation": 3150},
            # {"name": "El Colorado", "country": "CL", "lat": -33.3500, "lon": -70.2833, "timezone": "America/Santiago", "resort_elevation": 2900},
            # {"name": "Nevados de Chillan", "country": "CL", "lat": -36.9086, "lon": -71.4064, "timezone": "America/Santiago", "resort_elevation": 2300},
            # {"name": "Cerro Catedral", "country": "AR", "lat": -41.1739, "lon": -71.5489, "timezone": "America/Argentina/Buenos_Aires", "resort_elevation": 2050},
            # {"name": "Las Lenas", "country": "AR", "lat": -35.1500, "lon": -70.0833, "timezone": "America/Argentina/Buenos_Aires", "resort_elevation": 3050},
            # {"name": "Cerro Castor", "country": "AR", "lat": -54.7203, "lon": -68.0000, "timezone": "America/Argentina/Buenos_Aires", "resort_elevation": 950},
            # {"name": "Chapelco", "country": "AR", "lat": -40.1622, "lon": -71.2106, "timezone": "America/Argentina/Buenos_Aires", "resort_elevation": 1750},
            # {"name": "Cerro Bayo", "country": "AR", "lat": -40.7500, "lon": -71.6000, "timezone": "America/Argentina/Buenos_Aires", "resort_elevation": 1600},
        ]
    ]

SKI_FIELDS = get_ski_fields_with_timestamp()
START_DATE = date(2022, 11, 1)
BATCH_SIZE = 500  # Number of rows to yield at once

GLOBAL_COMPARISON_MODELS = [
    "era5",  # Reanalysis gold standard for long-term consistency
    "ecmwf_ifs",  # Native ECMWF HRES model (~9 km)
    "ukmo_seamless",  # UK Met Office Unified Model (~10 km)
    "icon_seamless",  # German Model (~11 km)
    "gem_seamless",  # Canadian Model (~15 km)
    "cma_grapes_global",  # Chinese Model (~15 km)
    "gfs_seamless",  # US Model (~25 km)
    "meteofrance_seamless",  # French ARPEGE Model (~25 km)
]

COUNTRY_MODEL_PRIORITY = {
    "JP": ["jma_seamless"],
    "AU": ["bom_access_global"],
    "NZ": ["bom_access_global"],
}

DAILY_VARIABLE_CONFIG = {
    "snowfall_sum": {
        "model_column_template": "snowfall_{model}_cm",
    },
    "precipitation_sum": {
        "model_column_template": "precipitation_sum_{model}_mm",
    },
    "temperature_2m_mean": {
        "model_column_template": "temperature_mean_{model}_c",
    },
    "snow_depth_max": {
        "model_column_template": "snow_depth_{model}_m",
    },
}

REQUESTED_DAILY_VARIABLES = list(DAILY_VARIABLE_CONFIG.keys())
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_MAX_MODELS_PER_REQUEST = 5
OPEN_METEO_RETRY_ATTEMPTS = 1
OPEN_METEO_BACKOFF_SECONDS = 20
OPEN_METEO_REQUEST_TIMEOUT = 15
OPEN_METEO_MIN_REQUEST_SPACING_SECONDS = 1.0
_LAST_OPEN_METEO_REQUEST_TS = 0.0

# Global Retry limits
GLOBAL_MAX_RETRIES = 5  # The absolute maximum number of retries across the entire script run
_GLOBAL_RETRY_COUNT = 0

class GlobalRateLimitReached(Exception):
    """Raised when the global retry limit for API requests is exhausted."""
    pass


def _to_numeric_series(values, record_count: int) -> pd.Series:
    return pd.to_numeric(
        pd.Series(values if values is not None else [0.0] * record_count),
        errors="coerce",
    ).fillna(0.0)


def build_daily_weather_frame(daily_data: dict, models: list[str]) -> pd.DataFrame:
    """Build a daily dataframe with only model-specific Open-Meteo fields."""
    record_count = len(daily_data["time"])
    df_dict = {
        "date": pd.to_datetime(daily_data["time"]).date,
    }

    for api_var, config in DAILY_VARIABLE_CONFIG.items():
        for model in models:
            model_key = f"{api_var}_{model}"
            if model_key not in daily_data:
                continue

            model_column = config["model_column_template"].format(model=model)
            df_dict[model_column] = _to_numeric_series(daily_data.get(model_key), record_count)

    return pd.DataFrame(df_dict)


def get_winter_spring_months_for_lat(latitude: float) -> set[int]:
    """Return winter and spring months for a latitude's hemisphere."""
    return {11, 12, 1, 2, 3, 4} if latitude >= 0 else {6, 7, 8, 9, 10, 11}


def get_winter_spring_season_year(day: date, latitude: float) -> int:
    """Return a season year that keeps cross-year northern Nov-Apr seasons together."""
    if latitude >= 0 and day.month in (11, 12):
        return day.year + 1
    return day.year


def get_all_missing_date_ranges_by_season(logger, locations, start_date, end_date, dataset, daily_default=False):
    """
    Returns:
      missing_ranges_by_location: dict of location_name -> dict of season_year -> (min_date, max_date)
      table_truncated: bool, True if the table is empty (truncated)
      default_applied: dict of location_name -> bool, True if the 14-day default was applied or covered by a missing range
    Only includes hemisphere winter and spring dates.
    """
    try:
        table_truncated = dataset is None or dataset.empty

        missing_ranges = {}
        default_applied = False
        
        last_14_start = end_date - timedelta(days=13)
        last_14_end = end_date
        last_14_set = set(pd.date_range(last_14_start, last_14_end).date)

        for loc in locations:
            name = loc["name"]
            latitude = loc["lat"]
            winter_months = get_winter_spring_months_for_lat(latitude)
            all_dates = pd.date_range(start_date, end_date)
            winter_dates = [d.date() for d in all_dates if d.month in winter_months]
            last_14_winter_set = {d for d in last_14_set if d.month in winter_months}
            winter_14_start = min(last_14_winter_set) if last_14_winter_set else None
            winter_14_end = max(last_14_winter_set) if last_14_winter_set else None

            if table_truncated or name not in dataset["location"].unique():
                missing = set(winter_dates)
            else:
                loc_df = dataset[dataset["location"] == name]
                existing_dates = set(pd.to_datetime(loc_df["date"]).dt.date)
                missing = set(winter_dates) - existing_dates

            # Group missing dates by year and get min/max per year
            seasons = {}
            for d in sorted(missing):
                season_year = get_winter_spring_season_year(d, latitude)
                seasons.setdefault(season_year, []).append(d)
            
            # For each season, get min/max
            season_ranges = {
                year: (min(ds), max(ds)) for year, ds in seasons.items()
            }

            # --- 14-day default logic, per location ---
            applied = False
            expanded = False

            # logger.info(f"Daily default for {name}: {daily_default}, last 14 days: {last_14_start} to {last_14_end}")

            if daily_default and last_14_winter_set:
                if winter_14_start is None or winter_14_end is None:
                    continue
                for season, (rng_start, rng_end) in list(season_ranges.items()):
                    rng_set = set(pd.date_range(rng_start, rng_end).date)
                    if last_14_winter_set.issubset(rng_set):
                        applied = True
                        break
                    elif last_14_winter_set & rng_set:
                        new_start = min(rng_start, winter_14_start)
                        new_end = max(rng_end, winter_14_end)
                        del season_ranges[season]
                        season_ranges["default_14d"] = (new_start, new_end)
                        applied = True
                        expanded = True
                        break
                if not applied and not expanded:
                    season_ranges["default_14d"] = (winter_14_start, winter_14_end)
                    applied = True

            missing_ranges[name] = season_ranges
            if applied:
                default_applied = True  # Set to True if applied for any location

        return missing_ranges, table_truncated, default_applied
    except Exception as e:
        logger.error(f"Failed to retrieve missing date ranges from dataset: {e}")
        # fallback: all winter dates for all locations, grouped by season year
        missing_ranges = {}
        for loc in locations:
            latitude = loc["lat"]
            winter_months = get_winter_spring_months_for_lat(latitude)
            seasons = {}
            for d in [d.date() for d in pd.date_range(start_date, end_date) if d.month in winter_months]:
                season_year = get_winter_spring_season_year(d, latitude)
                seasons.setdefault(season_year, []).append(d)
            season_ranges = {
                year: (min(ds), max(ds)) for year, ds in seasons.items()
            }
            missing_ranges[loc["name"]] = season_ranges
        return missing_ranges, False, {loc["name"]: False for loc in locations}


def get_relevant_models(country: str) -> list[str]:
    """Return only the model set needed for each country comparison."""
    comparison_pool = list(GLOBAL_COMPARISON_MODELS)
    comparison_pool.extend(COUNTRY_MODEL_PRIORITY.get(country, []))

    deduped_models = []
    for model in comparison_pool:
        if model not in deduped_models:
            deduped_models.append(model)

    return deduped_models


def _chunk_models(models: list[str], chunk_size: int) -> list[list[str]]:
    return [models[i:i + chunk_size] for i in range(0, len(models), chunk_size)]


def _wait_for_open_meteo_slot() -> None:
    global _LAST_OPEN_METEO_REQUEST_TS
    now = tyme.monotonic()
    elapsed = now - _LAST_OPEN_METEO_REQUEST_TS
    if elapsed < OPEN_METEO_MIN_REQUEST_SPACING_SECONDS:
        tyme.sleep(OPEN_METEO_MIN_REQUEST_SPACING_SECONDS - elapsed)
    _LAST_OPEN_METEO_REQUEST_TS = tyme.monotonic()


def _merge_open_meteo_payload(combined: dict | None, new_data: dict) -> dict:
    if combined is None:
        return new_data

    combined_daily = combined.setdefault("daily", {})
    new_daily = new_data.get("daily", {})

    if combined_daily.get("time") and new_daily.get("time") and combined_daily["time"] != new_daily["time"]:
        raise ValueError("Open-Meteo daily time arrays did not match across model batches")

    combined_daily.update(new_daily)
    combined.setdefault("daily_units", {}).update(new_data.get("daily_units", {}))
    return combined


def _compute_retry_wait_seconds(response, attempt: int) -> int:
    retry_after = None
    if response is not None:
        retry_after = response.headers.get("Retry-After")

    if retry_after:
        try:
            return max(1, int(float(retry_after)))
        except ValueError:
            pass

    return min(OPEN_METEO_BACKOFF_SECONDS * attempt, 300)


def _request_open_meteo_batch(location, start_date, end_date, models: list[str]) -> dict | None:
    global _GLOBAL_RETRY_COUNT
    resort_elevation = location.get("resort_elevation")
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(REQUESTED_DAILY_VARIABLES),
        "models": ",".join(models),
        "timezone": location["timezone"],
        "cell_selection": "land",
    }
    if resort_elevation is not None:
        params["elevation"] = resort_elevation

    for attempt in range(1, OPEN_METEO_RETRY_ATTEMPTS + 1):
        response = None
        try:
            _wait_for_open_meteo_slot()
            response = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=OPEN_METEO_REQUEST_TIMEOUT)
            logger.debug(f"API request URL: {response.url}")
            logger.debug(f"Response status code: {response.status_code}")

            if response.status_code == 429:
                _GLOBAL_RETRY_COUNT += 1
                if _GLOBAL_RETRY_COUNT > GLOBAL_MAX_RETRIES:
                    logger.error("Global retry limit exceeded. Halting pipeline.")
                    raise GlobalRateLimitReached("Too many 429 rate limits hit globally.")

                wait_seconds = _compute_retry_wait_seconds(response, attempt)
                logger.warning(
                    "Open-Meteo rate limit hit | location=%s | models=%s | attempt=%s/%s | wait_s=%s",
                    location["name"],
                    ",".join(models),
                    attempt,
                    OPEN_METEO_RETRY_ATTEMPTS,
                    wait_seconds,
                )
                tyme.sleep(wait_seconds)
                continue

            response.raise_for_status()
            data = response.json()
            if data.get("error"):
                logger.error(
                    "API error for %s | models=%s | reason=%s",
                    location["name"],
                    ",".join(models),
                    data.get("reason"),
                )
                return None
            return data

        except requests.RequestException as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            if status_code == 429:
                _GLOBAL_RETRY_COUNT += 1
                if _GLOBAL_RETRY_COUNT > GLOBAL_MAX_RETRIES:
                    logger.error("Global retry limit exceeded on RequestException. Halting pipeline.")
                    raise GlobalRateLimitReached("Too many 429 rate limits hit globally.")
                
                if attempt < OPEN_METEO_RETRY_ATTEMPTS:
                    wait_seconds = _compute_retry_wait_seconds(getattr(e, "response", None), attempt)
                    logger.warning(
                        "Open-Meteo 429 retry scheduled | location=%s | models=%s | attempt=%s/%s | wait_s=%s",
                        location["name"],
                        ",".join(models),
                        attempt,
                        OPEN_METEO_RETRY_ATTEMPTS,
                        wait_seconds,
                    )
                    tyme.sleep(wait_seconds)
                    continue

            logger.error(
                f"Request failed for {location['name']} ({resort_elevation}m) | models={','.join(models)}: {e}"
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error for {location['name']} ({resort_elevation}m) | models={','.join(models)}: {e}"
            )
            return None

    logger.error(
        "Exceeded Open-Meteo retry limit | location=%s | models=%s | start_date=%s | end_date=%s",
        location["name"],
        ",".join(models),
        start_date,
        end_date,
    )
    return None


def fetch_snowfall_data(location, start_date, end_date):
    """Fetch comparison-model Open-Meteo daily fields with smaller batched requests."""
    resort_elevation = location.get("resort_elevation")
    logger.debug(
        f"Fetching data for {location['name']} from {start_date} to {end_date} at resort elevation {resort_elevation}m"
    )
    country = location.get("country")
    comparison_models = get_relevant_models(country)
    model_batches = _chunk_models(comparison_models, OPEN_METEO_MAX_MODELS_PER_REQUEST)

    combined_data = None
    for batch_index, batch in enumerate(model_batches, start=1):
        logger.info(
            "Requesting Open-Meteo batch %s/%s for %s | models=%s | range=%s to %s",
            batch_index,
            len(model_batches),
            location["name"],
            ",".join(batch),
            start_date,
            end_date,
        )
        batch_data = _request_open_meteo_batch(location, start_date, end_date, batch)
        if batch_data is None:
            logger.warning(
                "Skipping failed comparison-model batch for %s | models=%s",
                location["name"],
                ",".join(batch),
            )
            continue

        combined_data = _merge_open_meteo_payload(combined_data, batch_data)

    if combined_data is None:
        return None

    logger.info(
        "Received data for %s (resort %sm): %s daily records",
        location["name"],
        resort_elevation,
        len(combined_data.get("daily", {}).get("time", [])),
    )
    return combined_data

@dlt.resource(write_disposition="merge", name="ski_field_lookup", primary_key=["name"])
def ski_field_lookup_resource(new_locations):
    """Yield ski field lookup table only for new locations."""
    for field in SKI_FIELDS:
        if field["name"] in new_locations:
            yield field

@dlt.source
def snowfall_source(logger: logging.Logger, dataset, run_from_date: date | None = None):
    """
    DLT source for snow data from ski fields in NZ and Australia.
    Fetches daily snowfall and temperature data for all locations, winter and spring months only.
    Uses row_max_min to avoid reprocessing existing data.
    """
    @dlt.resource(write_disposition="merge", name="ski_field_snowfall", 
                  primary_key=["location", "date"])
    def ski_field_data():
        state = dlt.current.source_state().setdefault("snowfall", {
            "Daily_Requests": {},
            "Processed_Ranges": {},
            "Known_Locations": [],
            "Daily_default": {},
        })

        today = date.today()
        today_str = str(today)
        end_date = today - timedelta(days=5)
        state["Daily_Requests"] = {today_str: state.get("Daily_Requests", {}).get(today_str, 0)}

        # Reset Daily_default to only today
        state["Daily_default"] = {today_str: state["Daily_default"].get(today_str, True)}

        Known_Locations = set(state.setdefault("Known_Locations", []))
        state["Known_Locations"] = list(Known_Locations)
        new_locations = set()

        # Pass the boolean for today to the missing range function
        if run_from_date is not None:
            logger.info(
                f"Start date provided: collecting winter+spring-only data from {run_from_date} to {end_date} for all locations"
            )
            # Force a winter+spring-only reload for the specified date window.
            missing_ranges_by_location, table_truncated, default_applied = get_all_missing_date_ranges_by_season(
                logger, SKI_FIELDS, run_from_date, end_date, None, daily_default=False
            )
        else:
            missing_ranges_by_location, table_truncated, default_applied = get_all_missing_date_ranges_by_season(
                logger, SKI_FIELDS, START_DATE, end_date, dataset, daily_default=state["Daily_default"][today_str]
            )

        if table_truncated:
            state["Processed_Ranges"] = {}

        logger.info("Starting snowfall data collection for ski fields")

        abort_pipeline = False

        for location in SKI_FIELDS:
            if abort_pipeline:
                break
                
            location_name = location["name"]
            country = location["country"]

            if location_name not in state["Known_Locations"]:
                new_locations.add(location_name)
            logger.info(f"Processing {location_name}, {country}")

            season_ranges = missing_ranges_by_location.get(location_name, {})
            if not season_ranges:
                logger.info(f"No missing ranges for {location_name}, skipping.")
                continue

            for season_year, (start_date, end_date) in season_ranges.items():
                logger.info(f"Requesting data for {location_name} {season_year} winter: {start_date} to {end_date}")

                try:
                    data = fetch_snowfall_data(location, str(start_date), str(end_date))
                    state["Daily_Requests"][today_str] = state["Daily_Requests"].get(today_str, 0) + 1

                    if not data or "daily" not in data or not data["daily"].get("time"):
                        logger.warning(f"No data returned for {location_name} ({start_date} to {end_date})")
                        continue

                    # --- Process raw daily snowpack-model inputs at resort elevation ---
                    grid_elevation = data.get("elevation")
                    daily_data = data["daily"]
                    models = get_relevant_models(country)
                    merged = build_daily_weather_frame(daily_data, models)
                    merged["location"] = location_name
                    merged["country"] = country
                    merged["resort_elevation"] = location.get("resort_elevation")
                    merged["grid_elevation"] = grid_elevation

                    # Yield in batches using the range approach
                    for i in range(0, len(merged), BATCH_SIZE):
                        yield merged.iloc[i:i+BATCH_SIZE].to_dict(orient="records")

                except GlobalRateLimitReached as e:
                    logger.error(f"Gracefully exiting generator to preserve yielded data: {e}")
                    abort_pipeline = True
                    break
                except Exception as e:
                    logger.error(f"Error processing {start_date} to {end_date} for {location_name}: {e}")
                tyme.sleep(1)

        if new_locations:
            state["Known_Locations"] = list(Known_Locations.union(new_locations))

        # If the default was applied, set it to False for today so it doesn't run again
        if default_applied:
            state["Daily_default"][today_str] = False

        logger.info("Completed snowfall data collection for all ski fields")

    return ski_field_data

if __name__ == "__main__":

    # Set up argument parser
    parser = argparse.ArgumentParser(description="Run the snowfall data pipeline")
    parser.add_argument(
        "--start-date",
        type=str,
        help="Optional start date for fetching snowfall data (format: YYYY-MM-DD)"
    )
    args = parser.parse_args()

    # Parse the optional start date
    run_from_date = None
    if args.start_date:
        try:
            run_from_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
            logger.info(f"Will run pipeline from specified start date: {run_from_date}")
        except ValueError as e:
            logger.error(f"Invalid start date format: {e}. Please use YYYY-MM-DD.")
            raise

    # Set up DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="snowfall_pipeline",
        destination=os.getenv("DLT_DESTINATION", "motherduck"),
        dataset_name="skifields",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )

    # Only fetch the remote dataset if the user did NOT pass a --start-date.
    # If a start date is provided we bypass the expensive/timeout-prone .df() call
    # and pass dataset=None into the source (the source logic already handles that).
    dataset = None
    known_locations = set()

    if run_from_date is None:
        try:
            ds = pipeline.dataset()
            # cheap probe: check the table exists before trying to download it
            try:
                dataset = ds["ski_field_snowfall"].df()
                if dataset is not None:
                    known_locations = set(dataset["location"].unique())
            except Exception as e:
                # If fetching the full table times out or fails, treat as empty
                logger.warning(f"Could not load full 'ski_field_snowfall' table (continuing with empty dataset): {e}")
                dataset = None
        except PipelineNeverRan:
            logger.warning("⚠️ No previous runs found for this pipeline. Assuming first run.")
        except DatabaseUndefinedRelation:
            logger.warning("⚠️ Table Doesn't Exist. Assuming truncation.")
        except Exception as e:
            logger.warning(f"Unexpected error while probing dataset; continuing with empty dataset: {e}")
            dataset = None
    else:
        logger.info("Start date provided; skipping remote dataset fetch and running with dataset=None.")

    # Run the pipeline and handle errors
    try:
        logger.info("Running snowfall pipeline...")
        source = snowfall_source(logger, dataset, run_from_date)
        load_info = pipeline.run(source)

        state = source.state.get('snowfall', {})
        logger.info(f"Daily Requests: {state.get('Daily_Requests', {})}")
        logger.info(f"Processed Ranges: {sum(len(v) for v in state.get('Processed_Ranges', {}).values())}")
        logger.info(f"Pipeline run completed. Load Info: {load_info}")

        state_locations = set(state.get("Known_Locations", []))
        new_locations = state_locations - known_locations

        # If new locations were found, yield the lookup table resource
        if new_locations:
            logger.info(f"New locations found: {new_locations}. Updating lookup table.")
            pipeline.run(ski_field_lookup_resource(new_locations))
        else:
            logger.info("No new locations found. Ski field lookup table remains unchanged.")

    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
        raise