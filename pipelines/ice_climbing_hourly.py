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
BATCH_SIZE = 1000

API_HOURLY_MEASURES = [
    "temperature_2m", "precipitation", "snowfall", "cloudcover", "windspeed_10m",
    "dew_point_2m", "surface_pressure", "relative_humidity_2m",
    "shortwave_radiation", "sunshine_duration", "is_day", "wind_gusts_10m", "snow_depth"
]

start_dt = datetime(2021, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
end_dt = datetime.now(timezone.utc) - timedelta(days=END_DT_LAG_DAYS)

# Forming Temp: The temperature (°C) below which an hour counts as "below freezing" for ice formation.
# Forming Hours: Minimum number of hours per day below forming_temp required for a day to be considered a "forming day".
# Forming Days: Number of recent days to look back for calculating the fraction of "forming days".
# Formed Days: Number of days to use for the rolling mean of the forming score to determine if ice has formed.
# Degrade Temp: Temperature (°C) above which an hour counts as "above freezing" for ice degrading.
# Degrade Hours: Minimum number of hours per day above degrade_temp required for a day to be considered "degrading".
ICE_CLIMBING = [
    {
        "name": "Remarkables", "country": "NZ", "lat": -45.0716, "lon": 168.8030, "timezone": "Pacific/Auckland",
        "forming_temp": -1.5, "forming_hours": 11, "forming_days": 5, "formed_days": 21, "degrade_temp": 1.8, "degrade_hours": 4,
    },
    {
        "name": "Dasler Pinnacles", "country": "NZ", "lat": -43.9590, "lon": 169.8635, "timezone": "Pacific/Auckland",
        "forming_temp": -1.5, "forming_hours": 11, "forming_days": 5, "formed_days": 21, "degrade_temp": 1.8, "degrade_hours": 4,
    },
    {
        "name": "Milford Sound", "country": "NZ", "lat": -44.7700, "lon": 168.0372, "timezone": "Pacific/Auckland",
        "forming_temp": -1.5, "forming_hours": 12, "forming_days": 5, "formed_days": 21, "degrade_temp": 1.8, "degrade_hours": 5,
    },
    {
        "name": "Bush Stream", "country": "NZ", "lat": -43.8487, "lon": 170.0439, "timezone": "Pacific/Auckland",
        "forming_temp": -1.5, "forming_hours": 11, "forming_days": 5, "formed_days": 21, "degrade_temp": 2.0, "degrade_hours": 4,
    }
]


def check_table_status(logger, client, schema: str, table: str) -> tuple[bool, bool, bool]:
    """
    Returns (table_exists, table_empty, table_missing) for the given schema and table.
    - table_exists: whether the table exists at all.
    - table_empty: table exists but is empty (truncated).
    - table_missing: shorthand for not table_exists
    """
    try:
        count_query = f"SELECT COUNT(*) FROM {schema}.{table}"
        count_result = client.execute_sql(count_query)
        row_count = next(iter(count_result))[0]
        table_empty = (row_count == 0)
        return True, table_empty, False
    except DatabaseUndefinedRelation:
        logger.warning(f"Table {schema}.{table} does not exist. Assuming full fetch.")
        return False, False, True
    except Exception as e:
        logger.error(f"Unexpected error checking {schema}.{table}: {e}")
        raise



def get_missing_datetime_ranges_sql(logger, locations, start_dt, end_dt, pipeline):
    """Return either full backfill (if missing columns) or missing datetime ranges per location."""
    issues_by_location = {loc["name"]: [] for loc in locations}

    try:
        location_values = ",\n".join(f"('{loc['name']}', '{loc['timezone']}')" for loc in locations)
        api_field_values = ",\n".join(f"('{f}')" for f in API_HOURLY_MEASURES)
        start_str = start_dt.strftime("%Y-%m-%d %H:00:00")
        end_str = end_dt.strftime("%Y-%m-%d %H:00:00")

        sql = f"""
        WITH 
        locations(name, timezone) AS (
            VALUES {location_values}
        ),
        api_fields(column_name) AS (
            VALUES {api_field_values}
        ),
        actual_columns AS (
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'ice_climbing' AND table_name = 'weather_hourly_raw'
        ),
        missing_columns AS (
            SELECT
                l.name AS location,
                COUNT(*) AS missing_count
            FROM locations l
            CROSS JOIN api_fields af
            LEFT JOIN actual_columns ac ON af.column_name = ac.column_name
            WHERE ac.column_name IS NULL
            GROUP BY l.name
        ),
        actual_data_range AS (
            SELECT 
                location AS name,
                MIN(datetime) AS min_utc,
                MAX(datetime) AS max_utc
            FROM ice_climbing.weather_hourly_raw
            GROUP BY location
        ),
        test_range AS (
            SELECT
                l.name,
                l.timezone,
                CAST('{start_str}' AS TIMESTAMP) AS data_start_utc,
                CAST('{end_str}' AS TIMESTAMP) AS data_end_utc
            FROM locations l
        ),
        all_hours_utc AS (
            SELECT
                t.name,
                t.timezone,
                (t.data_start_utc + (generate_series * INTERVAL '1 hour')) AS utc_datetime
            FROM test_range t, generate_series(
                0,
                CAST((extract(epoch FROM t.data_end_utc) - extract(epoch FROM t.data_start_utc)) / 3600 AS INTEGER)
            )
            WHERE (t.data_start_utc + (generate_series * INTERVAL '1 hour')) <= t.data_end_utc
        ),
        missing_hours AS (
            SELECT 
                a.name,
                a.timezone,
                a.utc_datetime,
                CASE
                    WHEN a.utc_datetime < ad.min_utc THEN 'before_data'
                    WHEN a.utc_datetime > ad.max_utc THEN 'after_data'
                    ELSE 'gap_in_data'
                END AS gap_type
            FROM all_hours_utc a
            JOIN test_range r ON a.name = r.name
            LEFT JOIN actual_data_range ad ON a.name = ad.name
            LEFT JOIN ice_climbing.weather_hourly_raw e 
                ON a.name = e.location AND a.utc_datetime = e.datetime
            WHERE e.datetime IS NULL
        ),
        gap_groups AS (
            SELECT 
                name,
                timezone,
                gap_type,
                utc_datetime,
                extract(epoch FROM utc_datetime) - 
                    (ROW_NUMBER() OVER (PARTITION BY name, gap_type ORDER BY utc_datetime) * 3600) AS grp
            FROM missing_hours
        ),
        missing_gaps AS (
            SELECT 
                name AS location,
                gap_type,
                MIN(utc_datetime) AS range_start,
                MAX(utc_datetime) AS range_end,
                COUNT(*) AS missing_hours
            FROM gap_groups
            GROUP BY name, timezone, gap_type, grp
            HAVING COUNT(*) > 1
        ),
        missing_cols AS (
            SELECT
                location,
                TRUE AS has_missing_columns
            FROM missing_columns
            WHERE missing_count > 0
        )

        SELECT
            mg.location,
            mg.gap_type,
            mg.range_start,
            mg.range_end,
            mg.missing_hours,
            FALSE AS missing_columns_flag
        FROM missing_gaps mg
        LEFT JOIN missing_cols mc ON mg.location = mc.location

        WHERE mc.has_missing_columns IS NULL  -- no missing columns, include missing gaps

        UNION ALL

        SELECT
            mc.location,
            'missing_column_backfill' AS gap_type,
            CAST('{start_str}' AS TIMESTAMP) AS range_start,
            CAST('{end_str}' AS TIMESTAMP) AS range_end,
            CAST((extract(epoch FROM CAST('{end_str}' AS TIMESTAMP)) - extract(epoch FROM CAST('{start_str}' AS TIMESTAMP))) / 3600 AS INTEGER) AS missing_hours,
            TRUE AS missing_columns_flag
        FROM missing_cols mc
        """

        with pipeline.sql_client() as client:
            result = client.execute_sql(sql)
            for row in result:
                location, gap_type, range_start, range_end, missing_hours, missing_columns_flag = row
                issues_by_location[location].append({
                    "type": gap_type,
                    "start": range_start,
                    "end": range_end,
                    "count": missing_hours,
                    "missing_columns": missing_columns_flag
                })

            table_exists, table_empty, table_missing = check_table_status(
                logger, client, "ice_climbing", "weather_hourly_raw"
            )

        return issues_by_location, table_exists, table_empty, table_missing

    except Exception as e:
        logger.error(f"SQL error in get_missing_datetime_ranges_sql: {e}")
        return {}, True, True, True

    


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
                "hourly": ",".join(API_HOURLY_MEASURES),
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
        missing_ranges_by_loc, table_exists, table_empty, table_missing = get_missing_datetime_ranges_sql(
            logger, ICE_CLIMBING, start_dt, end_dt, dlt.current.pipeline()
        )

        locations_to_fetch = []

        if table_missing:
            logger.info("Table does not exist - performing full fetch for all locations.")
            for location in ICE_CLIMBING:
                location_copy = location.copy()
                location_copy["fetch_start"] = start_dt
                location_copy["fetch_end"] = end_dt
                locations_to_fetch.append(location_copy)

        elif table_empty:
            logger.info("Table exists but is empty (truncated) - performing full fetch for all locations.")
            for location in ICE_CLIMBING:
                location_copy = location.copy()
                location_copy["fetch_start"] = start_dt
                location_copy["fetch_end"] = end_dt
                locations_to_fetch.append(location_copy)

        else:
            # Table exists and has data - fetch only missing ranges
            for location in ICE_CLIMBING:
                location_name = location["name"]
                missing_ranges = missing_ranges_by_loc.get(location_name, [])
                
                if not missing_ranges:
                    logger.info(f"No missing data points for {location_name}, skipping.")
                    continue
                
                for date_range in missing_ranges:
                    if date_range["count"] < 2:  # skip very small gaps if desired
                        continue
                    
                    gap_type = date_range["type"]
                    if gap_type == "before_data":
                        logger.info(f"Fetching initial data for {location_name} before recorded data")
                    elif gap_type == "after_data":
                        logger.info(f"Fetching new data for {location_name} after recorded data")
                    elif gap_type == "gap_in_data":
                        logger.info(f"Filling data gap for {location_name}")

                    fetch_start = date_range["start"]
                    fetch_end = date_range["end"]

                    # Fix timezone if missing
                    try:
                        tz = ZoneInfo(location["timezone"])
                        fetch_start = fetch_start.replace(tzinfo=tz) if fetch_start.tzinfo is None else fetch_start
                        fetch_end = fetch_end.replace(tzinfo=tz) if fetch_end.tzinfo is None else fetch_end
                    except Exception as e:
                        logger.warning(f"Error with timezone {location['timezone']}, using UTC: {e}")
                        fetch_start = fetch_start.replace(tzinfo=timezone.utc) if fetch_start.tzinfo is None else fetch_start
                        fetch_end = fetch_end.replace(tzinfo=timezone.utc) if fetch_end.tzinfo is None else fetch_end

                    if fetch_start > fetch_end:
                        logger.warning(f"Invalid date range {fetch_start} > {fetch_end} for {location_name}, skipping.")
                        continue

                    location_copy = location.copy()
                    location_copy["fetch_start"] = fetch_start
                    location_copy["fetch_end"] = fetch_end
                    locations_to_fetch.append(location_copy)


        if locations_to_fetch:
            logger.info(f"Fetching data for {len(locations_to_fetch)} locations in parallel")
            location_results = {}

            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                future_to_location = {
                    executor.submit(
                        fetch_hourly_data,
                        loc,
                        loc["fetch_start"],
                        loc["fetch_end"]
                    ): loc for loc in locations_to_fetch
                }

                for future in concurrent.futures.as_completed(future_to_location):
                    location = future_to_location[future]
                    location_name = location["name"]
                    try:
                        data = future.result()
                        location_results[location_name] = {"data": data, "location": location}
                        logger.info(f"Successfully fetched data for {location_name}")
                    except Exception as e:
                        logger.error(f"Failed to fetch data for {location_name}: {e}")
                        location_results[location_name] = {"data": None, "location": location}
            
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
                for col in API_HOURLY_MEASURES:
                    if col in h:
                        df[col] = h.get(col)
                
                # Add metadata
                df["location"] = location_name
                df["country"] = location["country"]
                df["timezone"] = location["timezone"]
                df["date"] = df["datetime"].dt.date  # Store date for easier filtering in DBT


                logger.info(f"Fetched {len(df)} records for {location_name} with fields: {list(df.columns)}")

                if df.empty:
                    logger.info(f"No new data to process for {location_name}, skipping.")
                    continue

                CHUNK_THRESHOLD = 50000
                chunk_size = 10000 if len(df) > CHUNK_THRESHOLD else len(df)

                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i+chunk_size].to_dict("records")
                    for j in range(0, len(chunk), BATCH_SIZE):
                        yield chunk[j:j+BATCH_SIZE]
                    
                processed += 1
                logger.info(f"Processed and yielded raw data for {location_name}.")
            state["Processed_Ranges"] = {
                loc["name"]: {
                    "start": loc["fetch_start"].isoformat(),
                    "end": loc["fetch_end"].isoformat()
                } for loc in locations_to_fetch
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