#!/usr/bin/env python
"""
Hourly Snow Data Pipeline with Strict API Rate Limiting

Fetches hourly snowfall, snow depth, and temperature data from 1978 onwards.
Implements aggressive rate limiting to avoid API throttling (max 200k records).
Processes data in small chunks with delays between requests.
"""

import logging
from dotenv import load_dotenv
from datetime import datetime, date, timedelta
import pyarrow as pa
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

# Rate limiting configuration - conservative for Open-Meteo API
MAX_DAILY_REQUESTS = 1000  # Start conservative
MAX_RECORDS_PER_REQUEST = 8760  # ~1 year of hourly data (365 * 24)
REQUEST_DELAY_SECONDS = 1.0  # 1 second delay to be safe
CHUNK_SIZE_DAYS = 365  # Process 1 year at a time (~2,190 winter records)
BATCH_SIZE = 2000  # Larger batches for better performance

# Ski field locations - hardcoded, no database queries!
SKI_FIELDS = [
    {"name": "Remarkables", "country": "NZ", "lat": -45.0661, "lon": 168.8196, "timezone": "Pacific/Auckland"},
    {"name": "Thredbo", "country": "AU", "lat": -36.5040, "lon": 148.2987, "timezone": "Australia/Sydney"},
    {"name": "Mount Hutt", "country": "NZ", "lat": -43.4707, "lon": 171.5306, "timezone": "Pacific/Auckland"},
]
START_DATE = date(1978, 1, 1)

class APIRateLimiter:
    """Manages API rate limiting with daily quotas and request delays"""

    def __init__(self, max_daily_requests=MAX_DAILY_REQUESTS, delay_seconds=REQUEST_DELAY_SECONDS):
        self.max_daily_requests = max_daily_requests
        self.delay_seconds = delay_seconds
        self.requests_today = 0
        self.last_request_time = 0
        self.current_date = date.today()

    def can_make_request(self):
        """Check if we can make another request today"""
        today = date.today()
        if today != self.current_date:
            # New day - reset counter
            self.current_date = today
            self.requests_today = 0

        return self.requests_today < self.max_daily_requests

    def wait_for_next_request(self):
        """Wait appropriate time before next request"""
        current_time = tyme.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.delay_seconds:
            wait_time = self.delay_seconds - time_since_last
            logger.info(f"Rate limiting: waiting {wait_time:.1f} seconds")
            tyme.sleep(wait_time)

        self.last_request_time = tyme.time()
        self.requests_today += 1
        logger.debug(f"API request #{self.requests_today} today")

# Global rate limiter
rate_limiter = APIRateLimiter()

def get_missing_seasonal_chunks(logger, locations, start_year, end_year, end_date, dataset):
    """
    Returns missing seasonal chunks (winter seasons) for hourly data processing.
    Much more efficient than date-based chunking - queries only winter months per year.
    Always reprocesses last 14 days if they fall within winter season.
    """
    try:
        table_truncated = dataset is None or (hasattr(dataset, 'empty') and dataset.empty)
        missing_chunks = {}

        # Calculate the most recent 14 days that need reprocessing (simplified)
        current_year = end_date.year
        current_winter_start = date(current_year, 6, 1)
        current_winter_end = date(current_year, 11, 30)
        reprocess_chunk = None
        reprocess_start = max(current_winter_start, end_date - timedelta(days=14))
        reprocess_end = min(current_winter_end, end_date)
        if reprocess_start <= reprocess_end:
            reprocess_chunk = (current_year, reprocess_start, reprocess_end)

        # Generate all winter seasons from start_year to end_year
        all_seasons = []
        for year in range(start_year, end_year + 1):
            # Winter season: June 1 to November 30
            season_start = date(year, 6, 1)
            season_end = date(year, 11, 30)

            # For current year, don't go beyond end_date
            if year == end_year:
                season_end = min(season_end, end_date)
                # Skip if season hasn't started yet
                if season_start > end_date:
                    continue

            all_seasons.append((year, season_start, season_end))

        for loc in locations:
            name = loc["name"]

            if table_truncated or name not in dataset["location"].unique():
                # All seasons are missing, plus reprocess chunk if applicable
                missing_seasons = all_seasons.copy()
                if reprocess_chunk:
                    missing_seasons.append(reprocess_chunk)
            else:
                # Find missing seasons by checking if we have data for each winter
                loc_df = dataset[dataset["location"] == name]
                existing_years = set()

                for dt_str in loc_df["datetime"]:
                    # Extract year from datetime string
                    year = int(dt_str.split('-')[0])
                    existing_years.add(year)

                # Include only missing seasons (not reprocessing entire years)
                missing_seasons = [
                    (year, start_date, end_date) 
                    for year, start_date, end_date in all_seasons 
                    if year not in existing_years
                ]

                # Always add the recent 14-day reprocess chunk if it exists
                if reprocess_chunk:
                    missing_seasons.append(reprocess_chunk)

            missing_chunks[name] = missing_seasons
            reprocess_info = " + last 14 days" if reprocess_chunk else ""
            logger.info(f"{name}: {len(missing_seasons)} winter seasons needed{reprocess_info}")

        return missing_chunks, table_truncated

    except Exception as e:
        logger.error(f"Failed to calculate missing seasonal chunks: {e}")
        # Fallback: all seasons for all locations
        fallback_seasons = []
        for year in range(start_year, end_year + 1):
            season_start = date(year, 6, 1)
            season_end = date(year, 11, 30)

            # For current year, respect end_date
            if year == end_year:
                season_end = min(season_end, end_date)
                if season_start > end_date:
                    continue

            fallback_seasons.append((year, season_start, season_end))

        missing_chunks = {loc["name"]: fallback_seasons for loc in locations}
        return missing_chunks, False

def fetch_seasonal_snow_data(location, year, start_date, end_date):
    """
    Fetch hourly snow data for a specific winter season using PyArrow.
    Only queries winter months (June-November) so no filtering needed.
    """
    if not rate_limiter.can_make_request():
        logger.warning(f"Daily API limit reached ({rate_limiter.max_daily_requests}). Skipping request.")
        return None

    rate_limiter.wait_for_next_request()

    logger.info(f"Fetching {year} winter data for {location['name']} ({start_date} to {end_date})")

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "start_date": str(start_date),
        "end_date": str(end_date),
        "hourly": ",".join([
            "snowfall",
            "snow_depth", 
            "temperature_2m"
        ]),
        "timezone": location["timezone"]
    }

    try:
        response = requests.get(url, params=params, timeout=60)
        logger.debug(f"API request URL: {response.url}")
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            logger.error(f"API error for {location['name']}: {data.get('reason')}")
            return None

        hourly_data = data.get("hourly", {})
        if not hourly_data.get("time"):
            logger.warning(f"No hourly data returned for {location['name']}")
            return None

        # Use PyArrow for efficient processing - no filtering needed!
        datetime_strings = hourly_data["time"]

        # Create PyArrow table
        table = pa.table({
            "datetime": pa.array(datetime_strings),
            "snowfall": pa.array(hourly_data.get("snowfall", [None] * len(datetime_strings))),
            "snow_depth": pa.array(hourly_data.get("snow_depth", [None] * len(datetime_strings))),
            "temperature": pa.array(hourly_data.get("temperature_2m", [None] * len(datetime_strings))),
            "location": pa.array([location["name"]] * len(datetime_strings))
        })

        records_count = len(table)
        logger.info(f"✅ {location['name']} {year}: {records_count} records processed")

        # Convert to Python dicts for DLT
        return table.to_pylist()

    except requests.RequestException as e:
        logger.error(f"Request failed for {location['name']}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error for {location['name']}: {e}")
        return None

@dlt.source
def snowfall_hourly_source(logger: logging.Logger, dataset):
    """
    DLT source for hourly snow data with strict rate limiting.
    Processes data in small chunks to avoid API throttling.
    """
    @dlt.resource(write_disposition="merge", name="ski_field_snowfall_hourly",
                  primary_key=["location", "datetime"])
    def ski_field_hourly_data():
        state = dlt.current.source_state().setdefault("snowfall_hourly", {
            "Daily_Requests": {},
            "Processed_Chunks": {},
            "Known_Locations": [],
            "Last_Run_Date": None
        })

        today = date.today()
        today_str = str(today)
        end_date = today - timedelta(days=2)  # Allow for data lag

        # Reset daily counter if new day
        if state.get("Last_Run_Date") != today_str:
            state["Daily_Requests"] = {today_str: 0}
            state["Last_Run_Date"] = today_str

        Known_Locations = set(state.setdefault("Known_Locations", []))
        new_locations = set()

        # Get missing seasonal chunks for each location
        START_YEAR = 1978
        END_YEAR = end_date.year  # Use the calculated end_date, not hardcoded 2025
        missing_chunks_by_location, table_truncated = get_missing_seasonal_chunks(
            logger, SKI_FIELDS, START_YEAR, END_YEAR, end_date, dataset
        )

        if table_truncated:
            state["Processed_Chunks"] = {}

        logger.info("🏂 Starting hourly snowfall data collection (1978-2025)")
        logger.info(f"Rate limiter: {rate_limiter.requests_today}/{rate_limiter.max_daily_requests} requests used today")

        total_chunks = sum(len(chunks) for chunks in missing_chunks_by_location.values())
        total_years = END_YEAR - START_YEAR + 1

        logger.info(f"📊 Collection scope: {len(SKI_FIELDS)} locations, ~{total_years} years, {total_chunks} seasons")
        logger.info(f"⏱️  Estimated time: ~{total_chunks * 1.2:.1f} seconds ({total_chunks * 1.2 / 60:.1f} minutes)")

        processed_chunks = 0

        for location in SKI_FIELDS:
            location_name = location["name"]
            country = location["country"]

            if location_name not in state["Known_Locations"]:
                new_locations.add(location_name)

            seasonal_chunks = missing_chunks_by_location.get(location_name, [])
            if not seasonal_chunks:
                logger.info(f"No missing seasons for {location_name}")
                continue

            logger.info(f"Processing {location_name}, {country} - {len(seasonal_chunks)} seasons")

            for year, chunk_start, chunk_end in seasonal_chunks:
                if not rate_limiter.can_make_request():
                    logger.error(f"Daily API limit reached. Stopping processing. {location_name} {year} winter ({chunk_start} to {chunk_end})")
                    return

                try:
                    processed_chunks += 1
                    logger.info(f"Season {processed_chunks}/{total_chunks}: {location_name} {year} winter ({chunk_start} to {chunk_end})")

                    data = fetch_seasonal_snow_data(location, year, chunk_start, chunk_end)

                    if data is not None and len(data) > 0:
                        # Yield data in batches
                        for i in range(0, len(data), BATCH_SIZE):
                            batch = data[i:i + BATCH_SIZE]
                            yield batch

                        # Track processed chunk
                        chunk_key = f"{location_name}_{year}_{chunk_start}_{chunk_end}"
                        state["Processed_Chunks"][chunk_key] = str(datetime.now())

                        logger.info(f"✅ Processed {len(data)} records for {location_name} {year}")
                    else:
                        logger.warning(f"No data returned for {location_name} {year} winter")

                except Exception as e:
                    logger.error(f"Error processing {year} winter for {location_name}: {e}")

                # Update request counter
                state["Daily_Requests"][today_str] = rate_limiter.requests_today

        # Update known locations
        if new_locations:
            state["Known_Locations"] = list(Known_Locations.union(new_locations))

        logger.info(f"Completed hourly data collection. Processed {processed_chunks} chunks.")
        logger.info(f"API usage: {rate_limiter.requests_today}/{rate_limiter.max_daily_requests} requests")

    return ski_field_hourly_data

if __name__ == "__main__":
    # Set up DLT pipeline - use existing skifields schema
    pipeline = dlt.pipeline(
        pipeline_name="snowfall_hourly_pipeline",
        destination=os.getenv("DLT_DESTINATION", "motherduck"),
        dataset_name="skifields",  # Use existing schema, not skifields_hourly
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )

    dataset = None
    known_locations = set()

    try:
        # Load existing data to identify gaps
        dataset = pipeline.dataset()["ski_field_snowfall_hourly"].df()
        if dataset is not None and not dataset.empty:
            known_locations = set(dataset["location"].unique())
            logger.info(f"Found existing data for: {sorted(known_locations)}")
            logger.info(f"Existing dataset size: {len(dataset):,} records")
        else:
            logger.info("No existing data found - first run")

    except (PipelineNeverRan, DatabaseUndefinedRelation):
        logger.warning("⚠️ No previous data found. Starting fresh collection.")
    except Exception as e:
        logger.warning(f"⚠️ Could not load existing data: {e}. Proceeding with full collection.")

    # Run the pipeline
    try:
        logger.info("🏂 Running hourly snowfall pipeline...")
        source = snowfall_hourly_source(logger, dataset)
        load_info = pipeline.run(source)

        state = source.state.get('snowfall_hourly', {})
        logger.info(f"Daily Requests: {state.get('Daily_Requests', {})}")
        logger.info(f"Processed Chunks: {len(state.get('Processed_Chunks', {}))}")
        logger.info(f"Pipeline completed: {load_info}")

        # Note: Using existing ski field lookup table, no need to update it
        state_locations = set(state.get("Known_Locations", []))
        new_locations = state_locations - known_locations
        if new_locations:
            logger.info(f"New locations processed: {new_locations}")
        else:
            logger.info("No new locations found.")

    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
        raise
