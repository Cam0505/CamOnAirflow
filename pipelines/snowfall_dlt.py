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
def get_ski_fields_with_timestamp():
    now = datetime.now(timezone.utc).isoformat()
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
START_DATE = date(1988, 1, 1)
BATCH_SIZE = 500  # Number of rows to yield at once
FORCE_SNOW_DEPTH_RELOAD = False  # <-- Set to False after one-off load

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

def fetch_snowfall_data(location, start_date, end_date):
    """Fetch historical snowfall and snow depth data for a specific location."""
    logger.debug(f"Fetching data for {location['name']} from {start_date} to {end_date}")

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(["snowfall_sum", "temperature_2m_mean"]),
        "hourly": "snow_depth",
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
    Fetches daily snowfall and snow depth data for all locations, June-Nov only.
    Uses row_max_min to avoid reprocessing existing data.
    """
    @dlt.resource(write_disposition="merge", name="ski_field_snowfall", 
                  primary_key=["location", "date"])
    def ski_field_data():
        state = dlt.current.source_state().setdefault("snowfall", {
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

        # Only consider June to November for all years
        def is_winter_month(d):
            return 6 <= d.month <= 11

        missing_dates_by_location, table_truncated = get_all_missing_dates(
            logger, SKI_FIELDS, START_DATE, end_date, dataset
        )
        # Filter missing_dates_by_location to only June-Nov
        for loc in missing_dates_by_location:
            missing_dates_by_location[loc] = {d for d in missing_dates_by_location[loc] if is_winter_month(d)}

        if table_truncated:
            state["Processed_Ranges"] = {}

        logger.info("Starting snowfall data collection for ski fields")
        processed_info = {}

        # <-- Set to False after one-off load
        # if FORCE_SNOW_DEPTH_RELOAD:
        #     for loc in missing_dates_by_location:
        #         if dataset is not None:
        #             loc_df = dataset[dataset["location"] == loc]
        #             all_june_nov_dates = set(
        #                 pd.to_datetime(loc_df["date"])
        #                 .dt.date[loc_df["date"].apply(lambda d: 6 <= d.month <= 11)]
        #             )
        #             missing_dates_by_location[loc] = all_june_nov_dates

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

            # Group missing dates by winter season (year)
            # This is required as i only want filter data, if i 
            # do min max of missing dates per location it will fetch 
            # non winter months as well
            seasons = {}
            for d in missing_dates:
                seasons.setdefault(d.year, []).append(d)
            for season_year, season_dates in seasons.items():
                chunk_dates = sorted(season_dates)
                start_str = chunk_dates[0].strftime("%Y-%m-%d")
                end_str = chunk_dates[-1].strftime("%Y-%m-%d")
                logger.info(f"Requesting data for {location_name} {season_year} winter: {start_str} to {end_str}")

                try:
                    data = fetch_snowfall_data(location, start_str, end_str)
                    state["Daily_Requests"][today_str] = state["Daily_Requests"].get(today_str, 0) + 1

                    if not data or "daily" not in data or not data["daily"].get("time"):
                        logger.warning(f"No data returned for {location_name} ({start_str} to {end_str})")
                        continue

                    # --- Process daily snowfall/temperature ---
                    daily_data = data["daily"]
                    daily_df = pd.DataFrame({
                        "date": pd.to_datetime(daily_data["time"]).date,
                        "snowfall": daily_data["snowfall_sum"],
                        "temperature_mean": daily_data["temperature_2m_mean"]
                    })

                    # --- Process hourly snow depth ---
                    if "hourly" in data and data["hourly"].get("time"):
                        hourly_df = pd.DataFrame({
                            "datetime": pd.to_datetime(data["hourly"]["time"]),
                            "snow_depth": data["hourly"]["snow_depth"]
                        })
                        hourly_df["date"] = hourly_df["datetime"].dt.date
                        # Only keep June-Nov
                        hourly_df = hourly_df[hourly_df["datetime"].dt.month.between(6, 11)]
                        # Average over all hours for each day
                        avg_depth = hourly_df.groupby("date")["snow_depth"].mean().reset_index()
                        avg_depth.rename(columns={"snow_depth": "avg_snow_depth"}, inplace=True)
                    else:
                        avg_depth = pd.DataFrame(columns=["date", "avg_snow_depth"])

                    # --- Merge daily and snow depth ---
                    merged = pd.merge(daily_df, avg_depth, on="date", how="left")
                    merged["location"] = location_name
                    merged["country"] = country

                    # Yield in batches using the range approach
                    for i in range(0, len(merged), BATCH_SIZE):
                        yield merged.iloc[i:i+BATCH_SIZE].to_dict(orient="records")

                    # Store processed range for this winter season
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

        logger.info("Completed snowfall data collection for all ski fields")

    return ski_field_data

if __name__ == "__main__":
    
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