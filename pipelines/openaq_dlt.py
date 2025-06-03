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
    {"city": "Liège", "sensor_id": 12345, "country": "BE"},
    {"city": "Madrid", "sensor_id": 10014, "country": "ES"},
    {"city": "Paris", "sensor_id": 10042, "country": "FR"},
    {"city": "Berlin", "sensor_id": 10077, "country": "DE"},
    {"city": "Warsaw", "sensor_id": 10123, "country": "PL"},
    {"city": "Milan", "sensor_id": 10234, "country": "IT"},
    {"city": "Budapest", "sensor_id": 10345, "country": "HU"}
]
START_DATE = datetime(2024, 12, 1).date()

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

        logger.info(f"State: {state}")

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

            # Alternative Approach Using Python Pipeline Dataset
            # sensor_dates = set(df[df["sensor_id"] == sensor_id]["datetime"].dt.date)
            # all_dates = set(start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1))
            # missing_dates = sorted(all_dates - sensor_dates)

            # Get missing dates for this sensor from the DB, minus already flagged dates
            missing_dates, has_data = get_dates(logger, sensor_id, start_date, end_date)
            # logger.info(f"Missing dates for sensor {sensor_id} ({city}): {missing_dates}")
            logger.debug(f"Flagged dates for sensor {sensor_id} ({city}): {flagged_dates}")
            missing_dates = [d for d in missing_dates if d.isoformat() not in flagged_dates]

            if not missing_dates:
                logger.info(f"No missing dates for sensor {sensor_id} ({city}). Skipping.")
                continue

            # Track if any data was ever yielded for this sensor
            sensor_has_data = False

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
                    rows_yielded = 0
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
                        rows_yielded += 1
                        sensor_has_data = True  # <-- Set this if any row is yielded

                    # If no rows were yielded for this range, flag all dates in the range as checked and empty
                    if not results or rows_yielded == 0:
                        flagged = [
                            (date_from + timedelta(days=i)).isoformat()
                            for i in range((date_to - date_from).days + 1)
                        ]
                        logger.info(f"No data yielded for sensor {sensor_id} ({city}) {date_from} to {date_to}, flagging dates: {flagged}")
                        new_flagged.update(flagged)

                except requests.RequestException as e:
                    logger.error(f"Request failed for sensor {sensor_id} {date_from} to {date_to}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error for sensor {sensor_id} {date_from} to {date_to}: {e}")

            logger.info(f"Completed processing sensor {sensor_id} ({city}).") 

            # --- Update flagged dates in state for this sensor ---
            # If the sensor has never returned data (not in DB and not this run), do NOT store all flagged dates.
            # Instead, store only min/max attempted in No_Data_Sensors for tracking.
            if not sensor_has_data and not has_data:
                logger.info(f"Sensor {sensor_id} ({city}) has never returned data. Not storing flagged dates, only min/max attempted.")
                state["Flagged_Requests"][sensor_id_str] = []
                min_date = min(missing_dates).isoformat() if missing_dates else None
                max_date = max(missing_dates).isoformat() if missing_dates else None
                state["No_Data_Sensors"][sensor_id_str] = {"min_attempted": min_date, "max_attempted": max_date}
            else:
                # Otherwise, store flagged dates as usual
                state["Flagged_Requests"][sensor_id_str] = list(flagged_dates.union(new_flagged))
                logger.debug(f"Flagged dates for sensor {sensor_id} ({city}): {state['Flagged_Requests'][sensor_id_str]}")

        logger.info("Completed OpenAQ daily sensor data extraction.")

    return sensor_air_quality

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
        logger.info(f"Pipeline run completed. Load Info: {load_info}")
    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
        raise

# Wasteful operations:
# - The flagged_dates set is re-created from state for each sensor on every run, but this is necessary for correct state tracking.
# - The SQL in get_dates is executed for every sensor on every run, which could be slow for many sensors or a large date range.
#   Consider optimizing by caching results or batching sensors if performance becomes an issue.