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

CITIES = ["Melbourne", "Sydney", "Brisbane", "Perth", "Auckland"]
PARAMETERS = ["pm25", "pm10", "no2", "so2", "co", "o3"]
START_DATE = "2025-01-01"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Get OpenAQ API key from environment
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
if not OPENAQ_API_KEY:
    raise RuntimeError("OPENAQ_API_KEY environment variable is not set.")

@dlt.source
def openaq_source(logger: logging.Logger, row_counts_dict: set):
    """
    DLT source for OpenAQ daily city air quality data.
    Fetches all aggregations for each city/date and widens the data to one row per city/date.
    """
    @dlt.resource(write_disposition="append", name="openaq_daily_wide")
    def city_air_quality() -> Iterator[Dict]:

        # Use DLT state to track API requests per day
        state = dlt.current.source_state().setdefault("open_aq", {
            "Daily_Requests": {},
            "City_Stats": {}
        })

        today = datetime.now(timezone.utc).date()
        today_str = str(today)
        end_date = today - timedelta(days=2)
        start_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()

        # Clean up Daily_Requests: keep only today's entry
        state["Daily_Requests"] = {
            k: v for k, v in state["Daily_Requests"].items()
            if k == today_str
        }
        if today_str not in state["Daily_Requests"]:
            state["Daily_Requests"][today_str] = 0

        # Set up rate limiting: 55 requests per minute (OpenAQ v3 default is 60/min)
        REQUESTS_PER_MINUTE = 60
        SAFETY_MARGIN = 5  # Make it 55/minute to be safe
        REQUESTS_LIMIT = REQUESTS_PER_MINUTE - SAFETY_MARGIN
        request_times = []

        # Iterate over each city and date
        for city in CITIES:
            date = start_date
            while date <= end_date:
                # Skip if this city/date is already in the dataset
                if (city, date) in row_counts_dict:
                    logger.debug(f"Skipping {city} {date} (already in dataset)")
                    date += timedelta(days=1)
                    continue

                # Prepare row for this city/date
                row = {
                    "city": city,
                    "date": date,  # Store as date object
                }
                # Fetch each parameter for this city/date
                for param in PARAMETERS:
                    try:
                        # Rate limiting: ensure no more than REQUESTS_LIMIT per minute
                        now = time.time()
                        request_times = [t for t in request_times if now - t < 60]
                        if len(request_times) >= REQUESTS_LIMIT:
                            sleep_time = 60 - (now - request_times[0]) + 0.5  # +0.5s buffer
                            logger.info(f"Rate limit hit, sleeping for {sleep_time:.2f} seconds")
                            time.sleep(sleep_time)
                            now = time.time()
                            request_times = [t for t in request_times if now - t < 60]

                        # Build v3 API request
                        url = "https://api.openaq.org/v3/measurements"
                        params = {
                            "city": city,
                            "parameter": param,
                            "date_from": str(date),
                            "date_to": str(date),
                            "temporal": "day",  # v3: use 'temporal' for aggregation
                            "limit": 1,
                        }
                        headers = {
                            "x-api-key": OPENAQ_API_KEY
                        }
                        logger.debug(f"Requesting {city} {date} {param} with params: {params}")
                        resp = requests.get(url, params=params, headers=headers)
                        request_times.append(time.time())
                        state["Daily_Requests"][today_str] += 1  # Increment for each request sent
                        if state["Daily_Requests"][today_str] >= 2000:
                            logger.info("Sent 2000 requests for today, exiting early.")
                            return
                        resp.raise_for_status()
                        results = resp.json().get("results", [])
                        if results:
                            # v3: value is in 'value', units in 'unit'
                            row[f"{param}_value"] = results[0].get("value")
                            row[f"{param}_unit"] = results[0].get("unit")
                            logger.debug(f"Received data for {city} {date} {param}: {results[0]}")
                        else:
                            row[f"{param}_value"] = None
                            row[f"{param}_unit"] = None
                            logger.debug(f"No data for {city} {date} {param}")
                    except requests.RequestException as e:
                        if hasattr(e, "response") and e.response is not None and e.response.status_code == 404:
                            logger.debug(f"No data for {city} {date} {param} (404 Not Found)")
                        else:
                            logger.error(f"Request failed for {city} {date} {param}: {e}")
                        row[f"{param}_value"] = None
                        row[f"{param}_unit"] = None
                    except Exception as e:
                        logger.error(f"Unexpected error for {city} {date} {param}: {e}")
                        row[f"{param}_value"] = None
                        row[f"{param}_unit"] = None

                logger.info(f"Yielding row for {city} {date}: {row}")
                yield row
                date += timedelta(days=1)

    return city_air_quality

if __name__ == "__main__":
    # Set up DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="myshiptracking",
        destination=os.getenv("DLT_DESTINATION"),
        dataset_name="main",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )

    # Try to load existing dataset to avoid duplicate requests
    try:
        dataset = pipeline.dataset()["openaq_daily_wide"].df()
        if dataset is not None:
            logger.info(f"Grouped Row Counts:\n{dataset}")
    except PipelineNeverRan:
        logger.warning(
            "⚠️ No previous runs found for this pipeline. Assuming first run.")
        dataset = None
    except DatabaseUndefinedRelation:
        logger.warning(
            "⚠️ Table Doesn't Exist. Assuming truncation.")
        dataset = None

    # Build set of (city, date) tuples already present in the dataset
    if dataset is not None:
        row_counts_dict = {(row["city"], row["date"]) for _, row in dataset.iterrows()}
    else:
        logger.warning(
            "⚠️ No tables found yet in dataset — assuming first run.")
        row_counts_dict = set()

    # Run the pipeline and handle errors
    try:
        source = openaq_source(logger, row_counts_dict)
        load_info = pipeline.run(source)
        logger.info(f"Load Info: {load_info}")
    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
        raise