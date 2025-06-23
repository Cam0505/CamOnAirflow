#!/usr/bin/env python
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
def get_ice_climbing_with_timestamp():
    now = datetime.now(timezone.utc).isoformat()
    return [
        {**field, "last_updated": now}
        for field in [
            # New Zealand 
            {"name": "Remarkables", "country": "NZ", "lat": -45.0716, "lon": 168.8030, "timezone": "Pacific/Auckland"},
            {"name": "Black Peak", "country": "NZ", "lat": -44.5841, "lon": 168.8309, "timezone": "Pacific/Auckland"},
            {"name": "Dasler Pinnacles", "country": "NZ", "lat": -43.9568, "lon": 169.8682, "timezone": "Pacific/Auckland"}
            # Australia
        ]
    ]

ICE_CLIMBING = get_ice_climbing_with_timestamp()
RAINFALL_MONTHS = set(range(3, 8))  # March to July inclusive
START_DATE = date(1998, 1, 1)
BATCH_SIZE = 200  # Number of rows to yield at once

def get_all_missing_dates(logger, locations, start_date, end_date, dataset):
    """
    Returns:
      missing_dates: dict of location_name -> set of missing dates (as date objects)
      table_truncated: bool, True if the table is empty (truncated)
    """
    try:
        missing_dates = {}
        all_dates = set(pd.date_range(start_date, end_date).date)
        table_truncated = dataset is None or dataset.empty

        for loc in locations:
            name = loc["name"]
            if table_truncated or name not in dataset["location"].unique():
                missing_dates[name] = all_dates
            else:
                loc_df = dataset[dataset["location"] == name]
                existing_dates = set(pd.to_datetime(loc_df["date"]).dt.date)
                missing_dates[name] = all_dates - existing_dates
        return missing_dates, table_truncated
    except Exception as e:
        logger.error(f"Failed to retrieve missing dates from dataset: {e}")
        missing_dates = {loc["name"]: set(pd.date_range(start_date, end_date).date) for loc in locations}
        return missing_dates, False


@dlt.resource(write_disposition="merge", name="ice_climbing_lookup", primary_key=["name"])
def ice_climbing_lookup_resource(new_locations):
    """Yield ski field lookup table only for new locations."""
    for field in ICE_CLIMBING:
        if field["name"] in new_locations:
            yield field


@dlt.source
def rainfall_source(logger: logging.Logger, dataset):
    """
    DLT source for rainfall data from ice climbing locations in NZ.
    Fetches daily rainfall and precipitation data for all locations, March-July only.
    Uses row_max_min to avoid reprocessing existing data.
    """
    @dlt.resource(write_disposition="merge", name="ice_climbing_rainfall", 
                  primary_key=["location", "date"])
    def ice_climbing_data():
        state = dlt.current.source_state().setdefault("rainfall", {
            "Daily_Requests": {},
            "Processed_Ranges": {},
            "Known_Locations": [],
        })

        today = date.today()
        today_str = str(today)
        end_date = today - timedelta(days=2)
        state["Daily_Requests"] = {today_str: state["Daily_Requests"].get(today_str, 0)}

        Known_Locations = set(state.setdefault("Known_Locations", []))
        state["Known_Locations"] = list(Known_Locations)
        new_locations = set()

        def is_rainfall_season(d):
            return d.month in RAINFALL_MONTHS

        missing_dates_by_location, table_truncated = get_all_missing_dates(
            logger, ICE_CLIMBING, START_DATE, end_date, dataset
        )
        # Filter missing_dates_by_location to only March-July
        for loc in missing_dates_by_location:
            missing_dates_by_location[loc] = {d for d in missing_dates_by_location[loc] if is_rainfall_season(d)}

        if table_truncated:
            state["Processed_Ranges"] = {}

        logger.info("Starting rainfall data collection for ice climbing locations")
        processed_info = {}

        for location in ICE_CLIMBING:
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

            # Group missing dates by year
            seasons = {}
            for d in missing_dates:
                # Assign to that year as the "season"
                seasons.setdefault(d.year, []).append(d)
            for season_year, season_dates in seasons.items():
                chunk_dates = sorted(season_dates)
                start_str = chunk_dates[0].strftime("%Y-%m-%d")
                end_str = chunk_dates[-1].strftime("%Y-%m-%d")
                logger.info(f"Requesting rainfall data for {location_name} {season_year} (March-July): {start_str} to {end_str}")

                try:
                    data = fetch_rainfall_data(location, start_str, end_str)
                    state["Daily_Requests"][today_str] = state["Daily_Requests"].get(today_str, 0) + 1

                    if not data or "daily" not in data or not data["daily"].get("time"):
                        logger.warning(f"No data returned for {location_name} ({start_str} to {end_str})")
                        continue

                    # --- Process daily rainfall/precipitation ---
                    daily_data = data["daily"]
                    dates = pd.to_datetime(daily_data["time"]).date
                    rain_sum = daily_data["rain_sum"]
                    precip_sum = daily_data["precipitation_sum"]

                    records = [
                        {
                            "date": d,
                            "location": location_name,
                            "country": country,
                            "rain_sum": r,
                            "precipitation_sum": p
                        }
                        for d, r, p in zip(dates, rain_sum, precip_sum)
                    ]

                    for i in range(0, len(records), BATCH_SIZE):
                        yield records[i:i+BATCH_SIZE]

                    # Store processed range for this season
                    processed_info.setdefault(location_key, []).append({
                        "season_year": season_year,
                        "start": str(chunk_dates[0]),
                        "end": str(chunk_dates[-1]),
                        "timestamp": datetime.now().isoformat()
                    })

                except Exception as e:
                    logger.error(f"Error processing {start_str} to {end_str} for {location_name}: {e}")
                tyme.sleep(1)

        # After all processing, update the state once:
        state["Processed_Ranges"] = processed_info

        if new_locations:
            state["Known_Locations"] = list(Known_Locations.union(new_locations))

        logger.info("Completed rainfall data collection for all ice climbing locations")

    return ice_climbing_data

def fetch_rainfall_data(location, start_date, end_date):
    """Fetch historical rainfall and precipitation data for a specific location."""
    logger.debug(f"Fetching rainfall data for {location['name']} from {start_date} to {end_date}")

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(["rain_sum", "precipitation_sum"]),
        "timezone": location["timezone"]
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        logger.debug(f"API request URL: {response.url}")
        logger.debug(f"Response status code: {response.status_code}")
        response.raise_for_status()
        data = response.json()

        logger.info(f"Received data for {location['name']}: {len(data.get('daily', {}).get('time', []))} daily records")
        if "error" in data:
            logger.error(f"API error: {data.get('reason')}")
            return None

        return data

    except requests.RequestException as e:
        logger.error(f"Request failed for {location['name']} ({url}): {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error for {location['name']}: {e}")
        return None
    


if __name__ == "__main__":
    
    # Set up DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="rainfall_pipeline",
        destination=os.getenv("DLT_DESTINATION", "motherduck"),
        dataset_name="rainfall",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )
    dataset = None
    known_locations = set()
    try:
        dataset = pipeline.dataset()["ice_climbing_rainfall"].df()
        if dataset is not None:
            known_locations = set(dataset["location"].unique())
    except PipelineNeverRan:
        logger.warning(
            "⚠️ No previous runs found for this pipeline. Assuming first run.")
    except DatabaseUndefinedRelation:
        logger.warning(
            "⚠️ Table Doesn't Exist. Assuming truncation.")
    except ValueError as ve:
        logger.warning(
            f"⚠️ ValueError: {ve}. Assuming first run or empty dataset.")

    # Run the pipeline and handle errors
    try:
        
        logger.info("Running rainfall pipeline...")
        source = rainfall_source(logger, dataset)
        load_info = pipeline.run(source)

        state = source.state.get('rainfall', {})
        logger.info(f"Daily Requests: {state.get('Daily_Requests', {})}")
        logger.info(f"Processed Ranges: {sum(len(v) for v in state.get('Processed_Ranges', {}).values())}")
        logger.info(f"Pipeline run completed. Load Info: {load_info}")

        state_locations = set(state.get("Known_Locations", []))
        new_locations = state_locations - known_locations
        
        # If new locations were found, yield the lookup table resource
        if new_locations:
            logger.info(f"New locations found: {new_locations}. Updating lookup table.")
            pipeline.run(ice_climbing_lookup_resource(new_locations))
        else:
            logger.info("No new locations found. Ski field lookup table remains unchanged.")

    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
        raise