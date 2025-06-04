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
    {"city": "Melbourne", "sensor_id": 12345, "country": "AU"},
    # {"city": "Sydney", "sensor_id": 2392564, "country": "AU"},
    # {"city": "Brisbane", "sensor_id": 67890, "country": "AU"},
    # {"city": "Perth", "sensor_id": 54321, "country": "AU"},
    # {"city": "Auckland", "sensor_id": 98765, "country": "NZ"},
]
START_DATE = datetime(2025, 1, 1).date()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
if not OPENAQ_API_KEY:
    raise RuntimeError("OPENAQ_API_KEY environment variable is not set.")



def get_dates(logger, sensor_id, start_date, end_date):
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
                    )
                    SELECT a.date AS missing_date
                    FROM all_dates a
                    LEFT JOIN existing_dates e ON a.date = e.dt
                    WHERE e.dt IS NULL
                    ORDER BY a.date
                    """)
            return [row[0] for row in result] if result else []
    except Exception as e:
        logger.info("Failed to retrieve missing dates from the database.")
        return []



@dlt.source
def openaq_source(logger: logging.Logger, row_counts_dict: set):
    """
    DLT source for OpenAQ daily sensor air quality data.
    Fetches all daily summaries for each sensor and date range.
    """
    @dlt.resource(write_disposition="append", name="openaq_daily")
    def sensor_air_quality() -> Iterator[Dict]:
        state = dlt.current.source_state().setdefault("open_aq", {
            "Daily_Requests": {},
            "Flagged_Requests": {},
        }) 

        logger.info(f"State: {state}")


        today = datetime.now(timezone.utc).date()
        today_str = str(today)
        end_date = today - timedelta(days=2)
        start_date = START_DATE

        # Clean up Daily_Requests: keep only today's entry
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

            # Get flagged dates as a set of strings
            flagged_dates = set(state["Flagged_Requests"].get(sensor_id, []))

            # Use get_dates to get missing dates for this sensor
            missing_dates = get_dates(logger, sensor_id, start_date, end_date)
            # Remove flagged dates from missing_dates 
            logger.info(f"Missing dates for sensor {sensor_id} ({city}): {missing_dates}")
            logger.info(f"Flagged dates for sensor {sensor_id} ({city}): {flagged_dates}")
            missing_dates = [d for d in missing_dates if d.isoformat() not in flagged_dates]

            if not missing_dates:
                logger.info(f"No missing dates for sensor {sensor_id} ({city}). Skipping.")
                continue

            logger.info(f"Found {len(missing_dates)} missing date(s) for sensor {sensor_id} ({city}): {missing_dates}")

            for missing_date in missing_dates:
                # --- Rate limiting logic ---
                now = time.time()
                request_times = [t for t in request_times if now - t < 60]
                if len(request_times) >= REQUESTS_LIMIT:
                    sleep_time = 60 - (now - request_times[0]) + 0.5
                    logger.info(f"Rate limit hit, sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                    now = time.time()
                    request_times = [t for t in request_times if now - t < 60]

                # --- Prepare API request for a single date ---
                url = f"https://api.openaq.org/v3/sensors/{sensor_id}/days"
                params = {
                    "date_from": str(missing_date),
                    "date_to": str(missing_date + timedelta(days=1)),  # API expects exclusive end
                    "limit": 1000,
                }
                headers = {"x-api-key": OPENAQ_API_KEY}
                logger.info(f"Requesting sensor {sensor_id} ({city}) for {missing_date}")

                try:
                    # --- Make API request ---
                    logger.info(f"Making request for sensor {sensor_id} ({city}) params {params} header {headers}")
                    resp = requests.get(url, params=params, headers=headers)
                    request_times.append(time.time())
                    state["Daily_Requests"][today_str] += 1
                    if state["Daily_Requests"][today_str] >= 2000:
                        logger.warning("Sent 2000 requests for today, exiting early.")
                        return
                    resp.raise_for_status()
                    results = resp.json().get("results", [])

                    if results:
                        logger.info(f"Received {len(results)} records for sensor {sensor_id} ({city}) {missing_date}.")
                    else:
                        logger.info(f"No data returned for sensor {sensor_id} ({city}) {missing_date}.")

                    # --- Process each result row ---
                    for r in results:
                        logger.info(f"Processing row for sensor {sensor_id} {missing_date}: {r}")
                        period = r["period"]["datetimeFrom"]
                        # Parse both UTC and local as datetimes
                        dt_from_utc = datetime.fromisoformat(period["utc"].replace("Z", "+00:00"))
                        dt_from_local = None
                        logger.info(f"UTC datetime for sensor {sensor_id} {missing_date}: {dt_from_utc}")
                        if "local" in period and period["local"]: 
                            try:
                                logger.info(f"Local datetime for sensor {sensor_id} {missing_date}: {period['local']}")
                                dt_from_local = datetime.fromisoformat(period["local"])
                            except Exception:
                                logger.error(f"Failed to parse local datetime for sensor {sensor_id} {missing_date}: {period['local']}")
                                dt_from_local = None

                        # Use the local date if available, else UTC date
                        dt_from = dt_from_local.date() if dt_from_local else dt_from_utc.date()
                        logger.info(f"dt_from for sensor {sensor_id} {missing_date}: {dt_from}")
                        if dt_from != missing_date:
                            logger.debug(f"Skipping row for sensor {sensor_id} {dt_from}: not matching requested date {missing_date}")
                            continue
                        row_key = (sensor_id, dt_from)
                        logger.info(f"Row Key for sensor {sensor_id} {dt_from}: {row_key}")
                        if row_key in row_counts_dict:
                            logger.info(f"Row for sensor {sensor_id} {dt_from} already exists, skipping.")
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
                        logger.info(f"Yielding row for sensor {sensor_id} {dt_from}: {row}")
                        yield row

                    if not results:
                        # Flag this date as checked and empty (as ISO string)
                        flagged = [missing_date.isoformat()]
                        logger.info(f"Flagging this date {flagged} for this sensor id {sensor_id}")
                        existing = set(state["Flagged_Requests"].setdefault(sensor_id, []))
                        existing.update(flagged)
                        state["Flagged_Requests"][sensor_id] = list(existing)
                        logger.info(f"Flagged empty date for sensor {sensor_id} ({city}) {missing_date}.")
                
                except requests.RequestException as e:
                    logger.error(f"Request failed for sensor {sensor_id} {missing_date}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error for sensor {sensor_id} {missing_date}: {e}")

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

    # Initialize row_counts_dict to ensure it is always defined
    row_counts_dict = set()
    # Try to load existing dataset to avoid duplicate requests
    try:
        dataset = pipeline.dataset()["openaq_daily"].df()
        if dataset is not None:
            logger.info(f"Loaded existing dataset with {len(dataset)} rows.")
            # Convert all datetimes to date for consistent comparison
            dataset["datetime"] = dataset["datetime"].apply(lambda x: x.date() if hasattr(x, "date") else x)
            row_counts_dict = set(zip(dataset["sensor_id"], dataset["datetime"]))
    except PipelineNeverRan:
        logger.warning("⚠️ No previous runs found for this pipeline. Assuming first run.")
    except DatabaseUndefinedRelation:
        logger.warning("⚠️ Table Doesn't Exist. Assuming truncation.")


    # Run the pipeline and handle errors
    try:
        logger.info("Running OpenAQ pipeline...")
        source = openaq_source(logger, row_counts_dict)
        load_info = pipeline.run(source)

        x = source.state.get('open_aq', {})
        logger.info(f"Daily Requests: {x.get('Daily_Requests', {})}")
        logger.info(f"Flagged Requests: {x.get('Flagged_Requests', {})}")
        logger.info(f"Pipeline run completed. Load Info: {load_info}")
    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
        raise