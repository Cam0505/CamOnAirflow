#!/usr/bin/env python
import logging
from dotenv import load_dotenv
from datetime import datetime, date, timedelta
import pandas as pd
import dlt
from dlt.sources.helpers import requests
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
import os
import time as tyme
from project_path import get_project_paths, set_dlt_env_vars

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

# Output directory for any exported data
RESULTS_DIR = "snowfall_data"
os.makedirs(RESULTS_DIR, exist_ok=True)

# Ski field locations with dynamic last_updated
def get_ski_fields_with_timestamp():
    now = datetime.utcnow().isoformat()
    return [
        {**field, "last_updated": now}
        for field in [
            # New Zealand 
            {"name": "Remarkables", "country": "NZ", "lat": -45.0579, "lon": 168.8194, "timezone": "Pacific/Auckland"},
            {"name": "Cardrona", "country": "NZ", "lat": -44.8746, "lon": 168.9481, "timezone": "Pacific/Auckland"},
            {"name": "Treble Cone", "country": "NZ", "lat": -44.6335, "lon": 168.8972, "timezone": "Pacific/Auckland"},
            {"name": "Mount Hutt", "country": "NZ", "lat": -43.4707, "lon": 171.5306, "timezone": "Pacific/Auckland"},
            {"name": "Ohau", "country": "NZ", "lat": -44.2255, "lon": 169.7747, "timezone": "Pacific/Auckland"},
            {"name": "Coronet Peak", "country": "NZ", "lat": -44.9206, "lon": 168.7349, "timezone": "Pacific/Auckland"},
            {"name": "Whakapapa", "country": "NZ", "lat": -39.2546, "lon": 175.5456, "timezone": "Pacific/Auckland"},
            {"name": "Turoa", "country": "NZ", "lat": -39.3067, "lon": 175.5289, "timezone": "Pacific/Auckland"},
            # Australia
            {"name": "Thredbo", "country": "AU", "lat": -36.5040, "lon": 148.2987, "timezone": "Australia/Sydney"},
            {"name": "Perisher", "country": "AU", "lat": -36.4058, "lon": 148.4134, "timezone": "Australia/Sydney"},
            {"name": "Mt Buller", "country": "AU", "lat": -37.1467, "lon": 146.4473, "timezone": "Australia/Melbourne"},
            {"name": "Falls Creek", "country": "AU", "lat": -36.8655, "lon": 147.2861, "timezone": "Australia/Melbourne"},
            {"name": "Mt Hotham", "country": "AU", "lat": -36.9762, "lon": 147.1359, "timezone": "Australia/Melbourne"},
        ]
    ]

SKI_FIELDS = get_ski_fields_with_timestamp()
START_DATE = date(2008, 1, 1)
BATCH_SIZE = 150  # Number of rows to yield at once

def get_all_missing_dates(logger, locations, start_date, end_date, dataset):
    """
    Returns:
      missing_dates: dict of location_name -> set of missing dates (as date objects)
      db_info: dict of location_name -> dict with min_date, max_date, row_count
      table_truncated: bool, True if the table is empty (truncated)
    """
    try:
        missing_dates = {}
        db_info = {}
        all_dates = set(pd.date_range(start_date, end_date).date)
        table_truncated = dataset is None or dataset.empty

        for loc in locations:
            name = loc["name"]
            if table_truncated or name not in dataset["location"].unique():
                missing_dates[name] = all_dates
                db_info[name] = {"min_date": None, "max_date": None, "row_count": 0}
            else:
                loc_df = dataset[dataset["location"] == name]
                existing_dates = set(pd.to_datetime(loc_df["date"]).dt.date)
                missing_dates[name] = all_dates - existing_dates
                db_info[name] = {
                    "min_date": loc_df["date"].min(),
                    "max_date": loc_df["date"].max(),
                    "row_count": len(loc_df)
                }
        return missing_dates, db_info, table_truncated
    except Exception as e:
        logger.error(f"Failed to retrieve missing dates from dataset: {e}")
        missing_dates = {loc["name"]: set(pd.date_range(start_date, end_date).date) for loc in locations}
        db_info = {loc["name"]: {"min_date": None, "max_date": None, "row_count": 0} for loc in locations}
        return missing_dates, db_info, False

def fetch_snowfall_data(location, start_date, end_date):
    """Fetch historical snowfall data for a specific location."""
    logger.debug(f"Fetching data for {location['name']} from {start_date} to {end_date}")
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(["snowfall_sum", "temperature_2m_mean"]),
        "timezone": location["timezone"]
    }
    
    try:
        
        response = requests.get(url, params=params, timeout=30)
        logger.debug(f"API request URL: {response.url}")
        logger.debug(f"Response status code: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        
        logger.info(f"Received data for {location['name']}: {len(data.get('daily', {}).get('time', []))} records")
        if "error" in data:
            logger.error(f"API error: {data.get('reason')}")
            return None
        
        return data
    
    except requests.RequestException as e:
        logger.error(f"Request failed for {location['name']}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error for {location['name']}: {e}")
        return None

@dlt.resource(write_disposition="merge", name="ski_field_lookup", primary_key=["name"])
def ski_field_lookup_resource(new_locations):
    """Yield ski field lookup table only for new locations."""
    for field in SKI_FIELDS:
        if field["name"] in new_locations:
            yield field

@dlt.source
def snowfall_source(logger: logging.Logger, dataset):
    """
    DLT source for snow data from ski fields in NZ and Australia.
    Fetches daily snowfall data for all locations since 1990.
    Uses row_max_min to avoid reprocessing existing data.
    """
    @dlt.resource(write_disposition="merge", name="ski_field_snowfall", 
                  primary_key=["location", "date"])
    def ski_field_data():
        # Initialize or retrieve persistent state
        state = dlt.current.source_state().setdefault("snowfall", {
            "Daily_Requests": {},
            "Failed_Locations": {},
            "Processed_Ranges": {},
            "Known_Locations": [],
        })
        
        today = date.today()
        today_str = str(today)
        end_date = today - timedelta(days=2)  # Allow time for data processing
        
        # Only keep today's request count in state to avoid unbounded growth
        state["Daily_Requests"] = {today_str: state["Daily_Requests"].get(today_str, 0)}

        Known_Locations = set(state.setdefault("Known_Locations", []))
        state["Known_Locations"] = list(Known_Locations)
        new_locations = set()

        # Get missing dates, db_info, and table truncation status
        missing_dates_by_location, db_info, table_truncated = get_all_missing_dates(
            logger, SKI_FIELDS, START_DATE, end_date, dataset
        )

        # If table truncated, reset processed/failed ranges
        if table_truncated:
            state["Processed_Ranges"] = {}
            state["Failed_Locations"] = {}

        logger.info("Starting snowfall data collection for ski fields")
        processed_info = {}
        for location in SKI_FIELDS:
            location_name = location["name"]
            country = location["country"]
            location_key = f"{country}_{location_name.replace(' ', '_')}"
            
            if location_name not in state["Known_Locations"]:
                new_locations.add(location_name)
            logger.info(f"Processing {location_name}, {country}")

            missing_dates = sorted(missing_dates_by_location.get(location_name, []))
            if not missing_dates:
                logger.info(f"No missing dates for {location_name}, skipping.")
                continue

            chunk_size = timedelta(days=365)
            chunk_start = missing_dates[0]
            chunk_end = min(chunk_start + chunk_size, missing_dates[-1])

            total_rows = 0
            failed_dates = []
            while chunk_start <= missing_dates[-1]:
                chunk_dates = [d for d in missing_dates if chunk_start <= d <= chunk_end]
                if not chunk_dates:
                    chunk_start = chunk_end + timedelta(days=1)
                    chunk_end = min(chunk_start + chunk_size, missing_dates[-1])
                    continue

                start_str = chunk_dates[0].strftime("%Y-%m-%d")
                end_str = chunk_dates[-1].strftime("%Y-%m-%d")
                logger.info(f"Requesting data for {location_name} from {start_str} to {end_str}")

                try:
                    data = fetch_snowfall_data(location, start_str, end_str)
                    state["Daily_Requests"][today_str] = state["Daily_Requests"].get(today_str, 0) + 1

                    if not data or "daily" not in data or not data["daily"].get("time"):
                        logger.warning(f"No data returned for {location_name} ({start_str} to {end_str})")
                        failed_dates.extend(chunk_dates)
                        chunk_start = chunk_end + timedelta(days=1)
                        chunk_end = min(chunk_start + chunk_size, missing_dates[-1])
                        continue

                    daily_data = data["daily"]
                    batch = []
                    for i in range(len(daily_data["time"])):
                        date_val = daily_data["time"][i]
                        if not isinstance(date_val, date):
                            date_val = date.fromisoformat(date_val)
                        row = {
                            "date": date_val,
                            "location": location_name,
                            "snowfall": daily_data["snowfall_sum"][i],
                            "temperature_mean": daily_data["temperature_2m_mean"][i]
                        }
                        batch.append(row)
                        total_rows += 1
                        if len(batch) >= BATCH_SIZE:
                            yield batch
                            batch = []
                    if batch:
                        yield batch

                except Exception as e:
                    logger.error(f"Error processing chunk {start_str} to {end_str} for {location_name}: {e}")
                    failed_dates.extend(chunk_dates)

                chunk_start = chunk_end + timedelta(days=1)
                chunk_end = min(chunk_start + chunk_size, missing_dates[-1])
                tyme.sleep(1)

            # After processing, update processed_info with absolute min/max and row count from db_info
            abs_min = db_info.get(location_name, {}).get("min_date", None)
            abs_max = db_info.get(location_name, {}).get("max_date", None)
            abs_count = db_info.get(location_name, {}).get("row_count", 0)
            if total_rows > 0 or abs_count > 0:
                processed_info[location_key] = [{
                    "start": str(abs_min) if abs_min else str(min(missing_dates)),
                    "end": str(abs_max) if abs_max else str(max(missing_dates)),
                    "timestamp": datetime.now().isoformat(),
                    "row_count": abs_count
                }]

            # Overwrite failed ranges for this run
            if failed_dates:
                state["Failed_Locations"][location_key] = {
                    "failed_ranges": [
                        {
                            "start": str(d),
                            "end": str(d),
                            "timestamp": datetime.now().isoformat(),
                            "reason": "No data returned"
                        }
                        for d in failed_dates
                    ]
                }

        # Update processed ranges in state
        state["Processed_Ranges"].update(processed_info)
        if new_locations:
            state["Known_Locations"] = list(Known_Locations.union(new_locations))

        logger.info("Completed snowfall data collection for all ski fields")

    return ski_field_data

if __name__ == "__main__":
    logger.info("=== Starting Snowfall Data Pipeline ===")
    
    # Set up DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="snowfall_pipeline",
        destination=os.getenv("DLT_DESTINATION", "motherduck"),
        dataset_name="skifields",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )
    dataset = None
    known_locations = set()
    try:
        dataset = pipeline.dataset()["ski_field_snowfall"].df()
        if dataset is not None:
            known_locations = set(dataset["location"].unique())
    except PipelineNeverRan:
        logger.warning(
            "⚠️ No previous runs found for this pipeline. Assuming first run.")
    except DatabaseUndefinedRelation:
        logger.warning(
            "⚠️ Table Doesn't Exist. Assuming truncation.")

    # Run the pipeline and handle errors
    try:
        logger.info("Running snowfall pipeline...")
        source = snowfall_source(logger, dataset)
        load_info = pipeline.run(source)

        state = source.state.get('snowfall', {})
        logger.info(f"Daily Requests: {state.get('Daily_Requests', {})}")
        logger.info(f"Failed Locations: {len(state.get('Failed_Locations', {}))}")
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