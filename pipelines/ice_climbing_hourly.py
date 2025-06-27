import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo 
import pandas as pd
import dlt
from dlt.sources.helpers import requests
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
import os
import time as tyme
from project_path import get_project_paths, set_dlt_env_vars
import concurrent.futures

# Load environment variables and set DLT config
paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
DBT_DIR = paths["DBT_DIR"]

load_dotenv(dotenv_path=ENV_FILE)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Configurable time window at the top
END_DT_LAG_DAYS = 3  # Open-Meteo archive is usually 1-2 days behind
DATA_WINDOW_DAYS = 1600 
BATCH_SIZE = 1500

end_dt = datetime.now(timezone.utc) - timedelta(days=END_DT_LAG_DAYS)
start_dt = end_dt - timedelta(days=DATA_WINDOW_DAYS)

def get_ice_climbing_with_thresholds():
    # Forming Temp: The temperature (°C) below which an hour counts as "below freezing" for ice formation.
    # Forming Hours: Minimum number of hours per day below forming_temp required for a day to be considered a "forming day".
    # Forming Days: Number of recent days to look back for calculating the fraction of "forming days".
    # Formed Days: Number of days to use for the rolling mean of the forming score to determine if ice has formed.
    # Degrade Temp: Temperature (°C) above which an hour counts as "above freezing" for ice degrading.
    # Degrade Hours: Minimum number of hours per day above degrade_temp required for a day to be considered "degrading".
    return [
        {
            "name": "Remarkables", "country": "NZ", "lat": -45.0716, "lon": 168.8030, "timezone": "Pacific/Auckland",
            "forming_temp": -1.5, "forming_hours": 11, "forming_days": 5, "formed_days": 21, "degrade_temp": 1.8, "degrade_hours": 4,
        },
        {
            "name": "Black Peak", "country": "NZ", "lat": -44.5841, "lon": 168.8309, "timezone": "Pacific/Auckland",
            "forming_temp": -2.0, "forming_hours": 12, "forming_days": 6, "formed_days": 28, "degrade_temp": 1.8, "degrade_hours": 5,
        },
        {
            "name": "Dasler Pinnacles", "country": "NZ", "lat": -43.9568, "lon": 169.8682, "timezone": "Pacific/Auckland",
            "forming_temp": -1.0, "forming_hours": 11, "forming_days": 5, "formed_days": 21, "degrade_temp": 1.8, "degrade_hours": 4,
        },
        {
            "name": "Milford Sound", "country": "NZ", "lat": -44.7726, "lon": 168.0389, "timezone": "Pacific/Auckland",
            "forming_temp": -1.5, "forming_hours": 12, "forming_days": 5, "formed_days": 21, "degrade_temp": 1.8, "degrade_hours": 5,
        },
        {
            "name": "Bush Stream", "country": "NZ", "lat": -43.8487, "lon": 170.0439, "timezone": "Pacific/Auckland",
            "forming_temp": -1.5, "forming_hours": 11, "forming_days": 5, "formed_days": 21, "degrade_temp": 2.0, "degrade_hours": 4,
        }
    ]


ICE_CLIMBING = get_ice_climbing_with_thresholds()



def get_missing_datetime_ranges_sql(logger, locations, start_dt, end_dt, pipeline):
    """Return missing datetime ranges for all locations using a single SQL query."""
    missing_ranges = {loc["name"]: [] for loc in locations}
    table_truncated = False
    
    try:
        # Prepare a VALUES clause for all locations
        location_values = ",\n".join(
            f"('{loc['name']}', '{loc['timezone']}')" for loc in locations
        )
        
        # Format the timestamps in UTC
        start_str = start_dt.strftime("%Y-%m-%d %H:00:00")
        end_str = end_dt.strftime("%Y-%m-%d %H:00:00")
        
        sql = f"""
        WITH locations(name, timezone) AS (
            VALUES
            {location_values}
        ),
        -- Get ACTUAL data bounds in UTC from your table
        actual_data_range AS (
            SELECT 
                location AS name,
                MIN(datetime) AS min_utc,
                MAX(datetime) AS max_utc
            FROM ice_climbing.weather_hourly_raw
            GROUP BY location
        ),
        -- Define the TEST range we want to analyze (in UTC)
        test_range AS (
            SELECT
                l.name,
                l.timezone,
                CAST('{start_str}' AS TIMESTAMP) AS test_start_utc,
                CAST('{end_str}' AS TIMESTAMP) AS test_end_utc,
                COALESCE(a.min_utc, CAST('{start_str}' AS TIMESTAMP)) AS data_start_utc,
                COALESCE(a.max_utc, CAST('{end_str}' AS TIMESTAMP)) AS data_end_utc
            FROM locations l
            LEFT JOIN actual_data_range a ON l.name = a.name
        ),
        -- Generate all hours in the TEST range (UTC)
        all_hours_utc AS (
            SELECT
                t.name,
                t.timezone,
                (t.test_start_utc + (generate_series * INTERVAL '1 hour')) AS utc_datetime
            FROM test_range t, generate_series(
                0,
                CAST((epoch(t.test_end_utc) - epoch(t.test_start_utc)) / 3600 AS INTEGER)
            )
            WHERE (t.test_start_utc + (generate_series * INTERVAL '1 hour')) <= t.test_end_utc
        ),
        -- Find missing hours (comparing against ACTUAL data)
        missing AS (
            SELECT 
                a.name,
                a.timezone,
                a.utc_datetime,
                CASE
                    WHEN a.utc_datetime < r.data_start_utc THEN 'before_data'
                    WHEN a.utc_datetime > r.data_end_utc THEN 'after_data'
                    ELSE 'gap_in_data'
                END AS gap_type
            FROM all_hours_utc a
            JOIN test_range r ON a.name = r.name
            LEFT JOIN ice_climbing.weather_hourly_raw e 
                ON a.name = e.location AND a.utc_datetime = e.datetime
            WHERE e.datetime IS NULL
        ),
        -- Group contiguous missing hours by gap type
        gap_groups AS (
            SELECT 
                name,
                timezone,
                gap_type,
                utc_datetime,
                epoch(utc_datetime) - 
                    (ROW_NUMBER() OVER (PARTITION BY name, gap_type ORDER BY utc_datetime) * 3600) AS grp
            FROM missing
        )
        -- Final results with clear gap categorization
        SELECT 
            name,
            gap_type,
            MIN(utc_datetime) AS utc_start,
            MAX(utc_datetime) AS utc_end,
            COUNT(*) AS missing_hours
        FROM gap_groups
        GROUP BY name, timezone, gap_type, grp
        HAVING COUNT(*) > 1  
        ORDER BY name, gap_type, utc_start;
        """
        
        with pipeline.sql_client() as client:
            result = client.execute_sql(sql)
            for row in result:
                loc_name, gap_type, start_dt, end_dt, missing_count = row
                missing_ranges[loc_name].append({
                    "start": start_dt,
                    "end": end_dt,
                    "count": missing_count,
                    "type": gap_type
                })
            
            # Check if table is empty (no actual data ranges found)
            check_empty_sql = "SELECT COUNT(*) FROM ice_climbing.weather_hourly_raw"
            try:
                result = client.execute_sql(check_empty_sql)
                # Get the first row from the result (which might be a list or cursor)
                row_count = 0
                for row in result:
                    row_count = row[0]  # Get the count from the first column
                    break
                table_truncated = (row_count == 0)
            except Exception as e:
                logger.warning(f"Error checking if table exists: {e}")
                table_truncated = True  # Assume table is empty if we can't check
            
            # Log results
            for loc_name in missing_ranges:
                ranges = missing_ranges[loc_name]
                total_missing = sum(r["count"] for r in ranges)
                num_ranges = len(ranges)
                logger.info(f"Found {total_missing} missing timestamps in {num_ranges} range(s) for {loc_name}")
                
        return missing_ranges, table_truncated
        
    except Exception as e:
        logger.error(f"Failed to retrieve missing datetime ranges: {e}")
        # Return empty ranges and set table_truncated to true to force full range fetch
        return {}, True
    


def fetch_hourly_data(location, start_dt, end_dt, max_retries=3, retry_delay=2):
    """Fetch data from Open-Meteo with retry logic for rate limiting."""
    logger.info(f"Fetching hourly data for {location['name']} from {start_dt} to {end_dt}")
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    for attempt in range(1, max_retries + 1):
        try:
            params = {
                "latitude": location["lat"],
                "longitude": location["lon"],
                "start_date": start_dt.date().isoformat(),
                "end_date": end_dt.date().isoformat(),
                "hourly": ",".join([
                    "temperature_2m", "precipitation", "snowfall", "cloudcover", "windspeed_10m",
                    "dew_point_2m", "surface_pressure", "relative_humidity_2m",
                    "shortwave_radiation", "sunshine_duration", "is_day", "wind_gusts_10m"
                ]),
                "timezone": location["timezone"]
            }
            
            response = requests.get(url, params=params, timeout=60)
            logger.debug(f"API request URL: {response.url}")
            
            # Check for rate limiting (429 status)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', retry_delay * attempt))
                logger.warning(f"Rate limited, waiting {retry_after}s (server-suggested) before retry")
                tyme.sleep(retry_after)
                continue
                
            response.raise_for_status()
            data = response.json()
            
            if "error" in data:
                logger.error(f"API error: {data.get('reason')}")
                return None
                
            return data
            
        except Exception as e:
            if attempt < max_retries:
                wait_time = retry_delay * attempt
                logger.warning(f"Request failed for {location['name']}: {e}. Retrying in {wait_time}s ({attempt}/{max_retries})")
                tyme.sleep(wait_time)
            else:
                logger.error(f"Request failed for {location['name']} after {max_retries} attempts: {e}")
                return None


@dlt.source
def ice_climbing_hourly_source(logger: logging.Logger):
    @dlt.resource(write_disposition="merge", name="weather_hourly_raw", primary_key=["location", "datetime"])
    def hourly_data_raw():
        """Fetch and store raw hourly weather data from Open-Meteo without enrichment."""
        state = dlt.current.source_state().setdefault("hourly_weather", {
            "Processed_Ranges": {}
        })
        processed = 0

        # Use SQL-based missing datetimes to offload computation to database
        missing_ranges_by_loc, table_truncated = get_missing_datetime_ranges_sql(
            logger, ICE_CLIMBING, start_dt, end_dt, dlt.current.pipeline()
        )

        # Collect locations that need data fetching
        locations_to_fetch = []
        for location in ICE_CLIMBING:
            location_name = location["name"]
            missing_ranges = missing_ranges_by_loc.get(location_name, [])
            
            # Check table_truncated FIRST
            if table_truncated:
                # If table is truncated, ALWAYS use full range
                fetch_start = start_dt
                fetch_end = end_dt
                logger.info(f"Table truncated, using full date range for {location_name}")
                
                # Store a single range for the entire period
                location_copy = location.copy()
                location_copy["fetch_start"] = fetch_start
                location_copy["fetch_end"] = fetch_end
                locations_to_fetch.append(location_copy)
            elif not missing_ranges:
                logger.info(f"No missing data points for {location_name}, skipping.")
                continue
            else:
                # Process each range separately to avoid large API requests
                for date_range in missing_ranges:
                    if date_range["type"] == "before_data":
                        logger.info(f"Fetching initial data for {location_name} before recorded data")
                    elif date_range["type"] == "after_data":
                        logger.info(f"Fetching new data for {location_name} after recorded data")
                    elif date_range["type"] == "gap_in_data":
                        logger.info(f"Filling data gap for {location_name}")
                    fetch_start = date_range["start"]
                    fetch_end = date_range["end"]
                    
                    # Skip extremely small ranges (optional optimization)
                    if date_range["count"] < 2:  # Skip single-hour gaps if desired
                        continue
                        
                    # Ensure timezone is set properly
                    try:
                        tz = ZoneInfo(location["timezone"])
                        fetch_start = fetch_start.replace(tzinfo=tz) if fetch_start.tzinfo is None else fetch_start
                        fetch_end = fetch_end.replace(tzinfo=tz) if fetch_end.tzinfo is None else fetch_end
                    except Exception as e:
                        logger.warning(f"Error with timezone {location['timezone']}, using UTC: {e}")
                        fetch_start = fetch_start.replace(tzinfo=timezone.utc) if fetch_start.tzinfo is None else fetch_start
                        fetch_end = fetch_end.replace(tzinfo=timezone.utc) if fetch_end.tzinfo is None else fetch_end
                    
                    # Validate fetch range
                    if fetch_start > fetch_end:
                        logger.warning(f"Invalid date range: {fetch_start} > {fetch_end}. Skipping.")
                        continue
                        
                    # Store the fetch range with the location
                    location_copy = location.copy()
                    location_copy["fetch_start"] = fetch_start
                    location_copy["fetch_end"] = fetch_end
                    locations_to_fetch.append(location_copy)
        
        # Fetch data for multiple locations in parallel
        if locations_to_fetch:
            logger.info(f"Fetching data for {len(locations_to_fetch)} locations in parallel")
            location_results = {}
            
            # Process locations in batches to avoid overwhelming the API
            # Remove the batch loop entirely
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                # Submit all fetch tasks with custom date ranges
                future_to_location = {
                    executor.submit(
                        fetch_hourly_data, 
                        loc, 
                        loc["fetch_start"], 
                        loc["fetch_end"]
                    ): loc for loc in locations_to_fetch
                }
                
                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_location):
                    location = future_to_location[future]
                    location_name = location["name"]
                    try:
                        data = future.result()
                        location_results[location_name] = {
                            "data": data,
                            "location": location
                        }
                        logger.info(f"Successfully fetched data for {location_name}")
                    except Exception as e:
                        logger.error(f"Failed to fetch data for {location_name}: {e}")
                        location_results[location_name] = {
                            "data": None, 
                            "location": location
                        }
            
            # Process the results
            for location_name, result in location_results.items():
                data = result["data"]
                location = result["location"]
                
                if not data or "hourly" not in data or not data["hourly"].get("time"):
                    logger.warning(f"No data returned for {location_name}, skipping.")
                    continue
                
                # Process the data
                h = data["hourly"]
                df = pd.DataFrame({"datetime": pd.to_datetime(h["time"])})
                
                # Add all columns from the API response
                for col in [
                    "temperature_2m", "precipitation", "snowfall", "cloudcover", "windspeed_10m",
                    "dew_point_2m", "surface_pressure", "relative_humidity_2m", 
                    "shortwave_radiation", "sunshine_duration", "is_day", "wind_gusts_10m"
                ]:
                    if col in h:
                        df[col] = h.get(col)
                
                # Add metadata
                df["location"] = location_name
                df["country"] = location["country"]
                df["timezone"] = location["timezone"]
                df["date"] = df["datetime"].dt.date  # Store date for easier filtering in DBT

                if df.empty:
                    logger.info(f"No new data to process for {location_name}, skipping.")
                    continue

                CHUNK_THRESHOLD = 50000  

                if len(df) > CHUNK_THRESHOLD:
                    # Memory-efficient chunked processing
                    chunk_size = 10000
                    for i in range(0, len(df), chunk_size):
                        chunk = df.iloc[i:i+chunk_size]
                        records = chunk.to_dict("records")
                        for j in range(0, len(records), BATCH_SIZE):
                            yield records[j:j+BATCH_SIZE]
                else:
                    # Faster small-batch processing
                    records = df.to_dict("records")
                    for i in range(0, len(records), BATCH_SIZE):
                        yield records[i:i+BATCH_SIZE]
                    
                processed += 1
                logger.info(f"Processed and yielded raw data for {location_name}.")

                # Update processed range in state
                if not df.empty:
                    min_date = df["datetime"].min()
                    max_date = df["datetime"].max()
                    state["Processed_Ranges"][location_name] = {
                        "min": str(min_date),
                        "max": str(max_date)
                    }
        else:
            logger.info("No locations need data fetching.")
                
        logger.info(f"Raw hourly data resource finished. {processed} location(s) processed.")
    return hourly_data_raw



@dlt.resource(write_disposition="merge", name="ice_climbing_thresholds", primary_key=["name"])
def ice_climbing_thresholds_resource(logger, thresholds_dataset):
    threshold_fields = ["forming_temp", "forming_hours", "forming_days", "formed_days", "degrade_temp", "degrade_hours"]
    changes = 0

    for loc in ICE_CLIMBING:
        # Find the latest thresholds for this location, if any
        if thresholds_dataset is not None and not thresholds_dataset.empty:
            latest = (
                thresholds_dataset[thresholds_dataset["name"] == loc["name"]]
                .sort_values("name", ascending=False)
                .head(1)
            )
            latest = latest.iloc[0] if not latest.empty else None
        else:
            latest = None

        # Only yield if changed or not present
        try:
            changed = False
            if latest is None:
                changed = True
                logger.info(f"First threshold record for {loc['name']}")
            else:
                # Compare values safely
                for field in threshold_fields:
                    try:
                        if pd.isna(latest[field]) or float(latest[field]) != float(loc[field]):
                            logger.info(f"Threshold change for {loc['name']}: {field} changed from {latest[field]} to {loc[field]}")
                            changed = True
                            break
                    except (TypeError, ValueError) as e:
                        logger.warning(f"Error comparing {field} values for {loc['name']}: {e}")
                        changed = True
                        break

            if changed:
                changes += 1
                logger.info(f"Thresholds changed for {loc['name']}. Writing new version.")
                yield {
                    "name": loc["name"],
                    "country": loc["country"],
                    "lat": float(loc["lat"]),
                    "lon": float(loc["lon"]),
                    "forming_temp": float(loc["forming_temp"]),
                    "forming_hours": float(loc["forming_hours"]),
                    "forming_days": float(loc["forming_days"]),
                    "formed_days": float(loc["formed_days"]),
                    "degrade_temp": float(loc["degrade_temp"]),
                    "degrade_hours": float(loc["degrade_hours"]),
                }
            else:
                logger.info(f"No threshold change for {loc['name']}.")
        except Exception as e:
            logger.error(f"Error processing thresholds for {loc['name']}: {e}")
    
    logger.info(f"Threshold resource finished. {changes} location(s) updated.")




# In your main pipeline run:
if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="ice_climbing_raw_pipeline",
        destination=os.getenv("DLT_DESTINATION", "motherduck"),
        dataset_name="ice_climbing",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )
    
    # Try to load existing data
    # weather_dataset = None
    thresholds_dataset = None
    try:
        # weather_dataset = pipeline.dataset()["weather_hourly_raw"].df()
        thresholds_dataset = pipeline.dataset()["ice_climbing_thresholds"].df()
    except (PipelineNeverRan, DatabaseUndefinedRelation, ValueError, KeyError):
        logger.warning("No previous runs or table found. Assuming first run or empty DB.")
    
    try:
        # Run hourly data pipeline
        logger.info("Running ice climbing raw data pipeline.")
        source = ice_climbing_hourly_source(logger)
        load_info = pipeline.run(source)
        logger.info(f"Pipeline run completed. Load Info: {load_info}")
        
        # Log processed ranges
        state = source.state.get('hourly_weather', {}).get('Processed_Ranges', {})
        if state:
            logger.info(f"Processed date ranges: {state}")
        else:
            logger.info("No processed date ranges found in state.")

        # Run thresholds resource
        logger.info("Running thresholds resource...")
        thresholds_resource = pipeline.run(ice_climbing_thresholds_resource(logger, thresholds_dataset))
        
        # Add indexes after successful pipeline run
        logger.info("Creating indexes for better query performance...")
        with pipeline.sql_client() as client:
            # Create index on (location, date) for faster filtering
            client.execute_sql("""
            CREATE INDEX IF NOT EXISTS idx_weather_location_date 
            ON ice_climbing.weather_hourly_raw(location, date);
            """)
            
            # Create index on date alone for date-based queries
            client.execute_sql("""
            CREATE INDEX IF NOT EXISTS idx_weather_date 
            ON ice_climbing.weather_hourly_raw(date);
            """)

            client.execute_sql("""
            CREATE INDEX IF NOT EXISTS idx_weather_location_datetime 
            ON ice_climbing.weather_hourly_raw(location, datetime);
            """)
            
            logger.info("Indexes created successfully")
            
    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
        raise