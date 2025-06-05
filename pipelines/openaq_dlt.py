import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from typing import Iterator, Dict
from dlt.sources.helpers import requests
from project_path import get_project_paths, set_dlt_env_vars
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
import dlt
import os
import time

# Load environment variables and set DLT config
paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
DBT_DIR = paths["DBT_DIR"]

load_dotenv(dotenv_path=ENV_FILE)

# List of dicts: city, sensor_id, country
CITIES = [
    # Paris, France
    {"city": "Paris", "sensor_id": 10042, "country": "FR"},
    {"city": "Paris", "sensor_id": 10043, "country": "FR"},
    {"city": "Paris", "sensor_id": 10044, "country": "FR"},
    # Berlin, Germany
    {"city": "Berlin", "sensor_id": 10077, "country": "DE"},
    {"city": "Berlin", "sensor_id": 10078, "country": "DE"},
    {"city": "Berlin", "sensor_id": 10079, "country": "DE"},
    # Milan, Italy
    {"city": "Milan", "sensor_id": 10234, "country": "IT"},
    {"city": "Milan", "sensor_id": 10235, "country": "IT"},
    {"city": "Milan", "sensor_id": 10236, "country": "IT"},
    # London, United Kingdom
    # Madrid, Spain
    {"city": "Madrid", "sensor_id": 10014, "country": "ES"},
    {"city": "Madrid", "sensor_id": 10016, "country": "ES"},
    # Vienna, Austria
    {"city": "Vienna", "sensor_id": 10510, "country": "AT"},
    {"city": "Vienna", "sensor_id": 10511, "country": "AT"},
    # Other existing sensors
    {"city": "Liège", "sensor_id": 12345, "country": "BE"},
    {"city": "Warsaw", "sensor_id": 10123, "country": "PL"},
    {"city": "Budapest", "sensor_id": 10345, "country": "HU"},
    # Test sensor known to return no data (example: 9999999)
    # {"city": "NoDataTest", "sensor_id": 9999999, "country": "XX"},
    # Add more cities and sensors as needed
    {"city": "Barcelona", "sensor_id": 10018, "country": "ES"}
]
START_DATE = datetime(2024, 3, 1).date()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
if not OPENAQ_API_KEY:
    raise RuntimeError("OPENAQ_API_KEY environment variable is not set.")



def get_dates(logger, sensor_id, start_date, end_date):
    """
    Returns a tuple:
      - list of missing dates for a given sensor_id between start_date and end_date
      - boolean: True if any data exists for this sensor in the range, False otherwise
    """
    try:
        pipeline = dlt.current.pipeline()
        with pipeline.sql_client() as client:
            result = client.execute_sql(
                f"""
                WITH RECURSIVE all_dates AS (
                    SELECT DATE '{start_date}' AS date
                    UNION ALL
                    SELECT date + INTERVAL 1 DAY
                    FROM all_dates
                    WHERE date + INTERVAL 1 DAY <= DATE '{end_date}'
                ),
                existing_dates AS (
                    SELECT CAST(datetime AS DATE) AS dt
                    FROM camonairflow.main.openaq_daily
                    WHERE sensor_id = {sensor_id}
                      AND datetime >= '{start_date}' AND datetime <= '{end_date}'
                ),
                stats AS (
                    SELECT COUNT(*) AS total_dates FROM all_dates
                ),
                existing_count AS (
                    SELECT COUNT(*) AS existing_dates_count FROM existing_dates
                )
                SELECT 
                    a.date AS missing_date,
                    (SELECT existing_dates_count FROM existing_count) > 0 AS has_data
                FROM all_dates a
                LEFT JOIN existing_dates e ON a.date = e.dt
                WHERE e.dt IS NULL
                ORDER BY a.date
                """)
            if result:
                missing_dates = [row[0] for row in result]
                has_data = any(row[1] for row in result)
                return missing_dates, has_data
            else:
                return [], False
    except Exception as e:
        logger.error(f"Failed to retrieve missing dates from the database: {e}")
        # Fallback: return all dates in range, and False for has_data
        return [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)], False

@dlt.source
def openaq_source(logger: logging.Logger):
    """
    DLT source for OpenAQ daily sensor air quality data.
    Fetches all daily summaries for each sensor and date range.
    """
    @dlt.resource(write_disposition="append", name="openaq_daily")
    def sensor_air_quality() -> Iterator[Dict]:
        # Initialize or retrieve persistent state for tracking requests and flagged dates
        state = dlt.current.source_state().setdefault("open_aq", {
            "Daily_Requests": {},
            "Flagged_Requests": {},
            "No_Data_Sensors": {},
        }) 

        # logger.info(f"State: {state}")

        today = datetime.now(timezone.utc).date()
        today_str = str(today)
        end_date = today - timedelta(days=2)
        start_date = START_DATE

        # Only keep today's request count in state to avoid unbounded growth
        state["Daily_Requests"] = {
            k: v for k, v in state["Daily_Requests"].items()
            if k == today_str
        }
        if today_str not in state["Daily_Requests"]:
            state["Daily_Requests"][today_str] = 0

        REQUESTS_PER_MINUTE = 60
        SAFETY_MARGIN = 5  # Make it 55/minute to be safe
        REQUESTS_LIMIT = REQUESTS_PER_MINUTE - SAFETY_MARGIN
        request_times = []

        logger.info("Starting OpenAQ daily sensor data extraction.")

        for city_info in CITIES:
            city = city_info["city"]
            sensor_id = city_info["sensor_id"]
            country = city_info["country"]

            logger.info(f"Processing sensor {sensor_id} for {city}, {country}.")

            # Always use string keys for state to avoid mismatches
            sensor_id_str = str(sensor_id)
            flagged_dates = set(state.get("Flagged_Requests", {}).get(sensor_id_str, []))
            # Ensure state is always a list for this sensor
            state["Flagged_Requests"][sensor_id_str] = list(flagged_dates)
            new_flagged = set()

            # --- Check if sensor is in No_Data_Sensors and adjust dates if needed ---
            adjusted_start_date, adjusted_end_date = adjust_dates_for_no_data_sensor(
                sensor_id_str, start_date, end_date, state
            )
            if adjusted_start_date is None or adjusted_end_date is None:
                # All dates already checked, skip this sensor
                continue

            # Get missing dates for this sensor from the DB, minus already flagged dates
            missing_dates, has_data = get_dates(logger, sensor_id, adjusted_start_date, adjusted_end_date)
            logger.debug(f"Flagged dates for sensor {sensor_id} ({city}): {flagged_dates}")
            missing_dates = [d for d in missing_dates if d.isoformat() not in flagged_dates]

            if not missing_dates:
                logger.info(f"No missing dates for sensor {sensor_id} ({city}). Skipping.")
                continue

            # Group missing dates into contiguous ranges for efficient API requests
            missing_ranges = []
            if missing_dates:
                start = prev = missing_dates[0]
                for d in missing_dates[1:]:
                    if d == prev + timedelta(days=1):
                        prev = d
                    else:
                        missing_ranges.append((start, prev))
                        start = prev = d
                missing_ranges.append((start, prev))

            logger.info(f"Found {len(missing_ranges)} missing range(s) for sensor {sensor_id} ({city}).")
            logger.debug(f"Date ranges to be requested for sensor {sensor_id} ({city}): {missing_ranges}")


            yielded_dates = set()  # Track dates yielded in this run for this sensor

            for date_from, date_to in missing_ranges:
                # --- Rate limiting logic ---
                now = time.time()
                request_times = [t for t in request_times if now - t < 60]
                if len(request_times) >= REQUESTS_LIMIT:
                    sleep_time = 60 - (now - request_times[0]) + 0.5
                    logger.info(f"Rate limit hit, sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                    now = time.time()
                    request_times = [t for t in request_times if now - t < 60]

                # Prepare API request for a date range
                url = f"https://api.openaq.org/v3/sensors/{sensor_id}/days"
                params = {
                    "date_from": str(date_from),
                    "date_to": str(date_to + timedelta(days=1)),  # API expects exclusive end
                    "limit": 1000,
                }
                headers = {"x-api-key": OPENAQ_API_KEY}
                logger.info(f"Requesting sensor {sensor_id} ({city}) {date_from} to {date_to}")

                try:
                    logger.debug(f"Making request for sensor {sensor_id} ({city}) params {params} header {headers}")
                    resp = requests.get(url, params=params, headers=headers)
                    request_times.append(time.time())
                    state["Daily_Requests"][today_str] += 1
                    if state["Daily_Requests"][today_str] >= 2000:
                        logger.warning("Sent 2000 requests for today, exiting early.")
                        return
                    resp.raise_for_status()
                    results = resp.json().get("results", [])

                    if results:
                        logger.info(f"Received {len(results)} records for sensor {sensor_id} ({city}) {date_from} to {date_to}.")
                    else:
                        logger.info(f"No data returned for sensor {sensor_id} ({city}) {date_from} to {date_to}.")

                    # Process each result row, only yielding rows for dates in the requested range
                    yielded_dates = set()

                    for r in results:
                        logger.debug(f"Processing row for sensor {sensor_id} {date_from} to {date_to}: {r}")
                        period = r["period"]["datetimeFrom"]
                        # Parse both UTC and local as datetimes
                        dt_from_utc = datetime.fromisoformat(period["utc"].replace("Z", "+00:00"))
                        dt_from_local = None
                        logger.debug(f"UTC datetime for sensor {sensor_id} {date_from} to {date_to}: {dt_from_utc}")
                        if "local" in period and period["local"]: 
                            try:
                                logger.debug(f"Local datetime for sensor {sensor_id} {date_from} to {date_to}: {period['local']}")
                                dt_from_local = datetime.fromisoformat(period["local"])
                            except Exception:
                                logger.error(f"Failed to parse local datetime for sensor {sensor_id} {date_from} to {date_to}: {period['local']}")
                                dt_from_local = None

                        # Use the local date if available, else UTC date
                        dt_from = dt_from_local.date() if dt_from_local else dt_from_utc.date()
                        logger.debug(f"dt_from for sensor {sensor_id} {date_from} to {date_to}: {dt_from}")
                        if not (date_from <= dt_from <= date_to):
                            logger.debug(f"Skipping row for sensor {sensor_id} {dt_from}: outside requested range {date_from} to {date_to}")
                            continue

                        row = {
                            "datetime": dt_from,
                            "city": city,
                            "country": country,
                            "sensor_id": sensor_id,
                            "parameter": r["parameter"]["name"],
                            "value": r.get("value"),
                            "unit": r["parameter"]["units"],
                            "summary": r.get("summary"),
                            "coverage": r.get("coverage"),
                        }
                        # logger.info(f"Yielding row for sensor {sensor_id} {dt_from}: {row}")
                        yield row
                        yielded_dates.add(dt_from.isoformat())


                    # After processing all results for this range:
                    all_dates = set((date_from + timedelta(days=i)).isoformat() for i in range((date_to - date_from).days + 1))
                    missing_in_range = all_dates - yielded_dates
                    if missing_in_range:
                        logger.info(f"No data yielded for sensor {sensor_id} ({city}) {date_from} to {date_to} for dates: {sorted(missing_in_range)}")
                        new_flagged.update(missing_in_range)

                except requests.RequestException as e:
                    logger.error(f"Request failed for sensor {sensor_id} {date_from} to {date_to}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error for sensor {sensor_id} {date_from} to {date_to}: {e}")

            logger.info(f"Completed processing sensor {sensor_id} ({city}).") 

            # --- Update flagged dates in state for this sensor ---
            # If the sensor has never returned data (not in DB and not this run), do NOT store all flagged dates.
            # Instead, store only min/max attempted in No_Data_Sensors for tracking.
            if not yielded_dates and not has_data:
                logger.info(f"Sensor {sensor_id} ({city}) has never returned data. Not storing flagged dates, only min/max attempted.")
                state["Flagged_Requests"][sensor_id_str] = []
                min_date = min(missing_dates).isoformat() if missing_dates else None
                max_date = max(missing_dates).isoformat() if missing_dates else None
                if "No_Data_Sensors" not in state:
                    state["No_Data_Sensors"] = {}
                prev = state["No_Data_Sensors"].get(sensor_id_str)
                # Only update if we have new min/max, otherwise keep previous
                if prev:
                    # Both prev and new min/max must not be None to compare
                    min_attempted = min(
                        [d for d in [prev.get("min_attempted"), min_date] if d is not None]
                    )
                    max_attempted = max(
                        [d for d in [prev.get("max_attempted"), max_date] if d is not None]
                    )
                else:
                    min_attempted = min_date
                    max_attempted = max_date
                state["No_Data_Sensors"][sensor_id_str] = {
                    "min_attempted": min_attempted,
                    "max_attempted": max_attempted,
                }
            else:
                # Otherwise, store flagged dates as usual
                state["Flagged_Requests"][sensor_id_str] = list(flagged_dates.union(new_flagged))
                logger.debug(f"Flagged dates for sensor {sensor_id} ({city}): {state['Flagged_Requests'][sensor_id_str]}")

            if yielded_dates:
                # If this sensor was previously in No_Data_Sensors, remove it
                if "No_Data_Sensors" in state and sensor_id_str in state["No_Data_Sensors"]:
                    logger.info(f"Sensor {sensor_id} ({city}) returned data after being marked as no-data. Removing from No_Data_Sensors.")
                    del state["No_Data_Sensors"][sensor_id_str]

        logger.info("Completed OpenAQ daily sensor data extraction.")

    return sensor_air_quality

def adjust_dates_for_no_data_sensor(sensor_id_str, start_date, end_date, state):
    """
    If the sensor is in No_Data_Sensors, adjust the start and end date to only search new, unchecked dates.
    If all dates have already been checked, return None, None to indicate skipping.
    """
    no_data_info = state.get("No_Data_Sensors", {}).get(sensor_id_str)
    if not no_data_info or not no_data_info.get("min_attempted") or not no_data_info.get("max_attempted"):
        # Not in No_Data_Sensors, proceed as normal
        return start_date, end_date

    # Convert to date for safe comparison
    prev_min = datetime.fromisoformat(no_data_info["min_attempted"]).date()
    prev_max = datetime.fromisoformat(no_data_info["max_attempted"]).date()

    # If the current search range is fully covered by previous attempts, skip
    if start_date >= prev_min and end_date <= prev_max:
        logger.info(
            f"Sensor {sensor_id_str} has no data. Already checked {prev_min} to {prev_max}. Skipping."
        )
        return None, None

    # Otherwise, adjust the search range to only new dates
    if start_date < prev_min and end_date > prev_max:
        # Search both before and after previous range (rare, but possible)
        # Here, just search before the previous min
        logger.info(
            f"Sensor {sensor_id_str} has no data for previous range {prev_min} to {prev_max}. "
            f"Now searching {start_date} to {prev_min - timedelta(days=1)}."
        )
        return start_date, prev_min - timedelta(days=1)
    elif start_date < prev_min:
        logger.info(
            f"Sensor {sensor_id_str} has no data for previous range {prev_min} to {prev_max}. "
            f"Now searching {start_date} to {prev_min - timedelta(days=1)}."
        )
        return start_date, prev_min - timedelta(days=1)
    elif end_date > prev_max:
        logger.info(
            f"Sensor {sensor_id_str} has no data for previous range {prev_min} to {prev_max}. "
            f"Now searching {prev_max + timedelta(days=1)} to {end_date}."
        )
        return prev_max + timedelta(days=1), end_date
    else:
        # Should not reach here, but fallback to skipping
        logger.info(
            f"Sensor {sensor_id_str} has no data. Already checked {prev_min} to {prev_max}. Skipping."
        )
        return None, None

if __name__ == "__main__":
    logger.info("Starting OpenAQ DLT pipeline run.")
    # Set up DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="air_quality_pipeline",
        destination=os.getenv("DLT_DESTINATION"),
        dataset_name="main",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )

    # Try to load existing dataset to avoid duplicate requests
    # try:
    #     dataset = pipeline.dataset()["openaq_daily"].df()
        
    # except PipelineNeverRan:
    #     logger.warning("⚠️ No previous runs found for this pipeline. Assuming first run.")
    #     dataset = None
    # except DatabaseUndefinedRelation:
    #     logger.warning("⚠️ Table Doesn't Exist. Assuming truncation.")
    #     dataset = None


    # Run the pipeline and handle errors
    try:
        logger.info("Running OpenAQ pipeline...")
        source = openaq_source(logger)
        load_info = pipeline.run(source)

        x = source.state.get('open_aq', {})
        logger.info(f"Daily Requests: {x.get('Daily_Requests', {})}")
        logger.info(f"Flagged Requests: {x.get('Flagged_Requests', {})}")
        logger.info(f"No Data Sensors: {x.get('No_Data_Sensors', {})}")
        logger.info(f"Pipeline run completed. Load Info: {load_info}")
    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
        raise

# Wasteful operations:
# - The flagged_dates set is re-created from state for each sensor on every run, but this is necessary for correct state tracking.
# - The SQL in get_dates is executed for every sensor on every run, which could be slow for many sensors or a large date range.
#   Consider optimizing by caching results or batching sensors if performance becomes an issue.