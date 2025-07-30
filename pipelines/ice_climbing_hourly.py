#!/usr/bin/env python
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo 
import pyarrow as pa
import dlt
from dlt.sources.helpers import requests
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation

import os
import time as tyme
from project_path import get_project_paths, set_dlt_env_vars
# import concurrent.futures
import math
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass

# Load environment variables and set DLT config
paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
DBT_DIR = paths["DBT_DIR"]

load_dotenv(dotenv_path=ENV_FILE)

# Custom exceptions for better error handling
class WeatherPipelineError(Exception):
    """Base exception for weather pipeline errors"""
    pass

class APIError(WeatherPipelineError):
    """Raised when external API calls fail"""
    pass

class SchemaValidationError(WeatherPipelineError):
    """Raised when database schema validation fails"""
    pass

class DataProcessingError(WeatherPipelineError):
    """Raised when data processing fails"""
    pass

@dataclass
class PipelineConfig:
    """Configuration for the weather pipeline"""
    END_DT_LAG_DAYS: int = 3
    DATA_WINDOW_DAYS: int = 1600
    BATCH_SIZE: int = 1000
    MAX_WORKERS: int = 3
    CHUNK_THRESHOLD: int = 50000
    CHUNK_SIZE: int = 10000
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 6
    API_TIMEOUT: int = 60
    ELEVATION_TIMEOUT: int = 10
    USE_CONNECTORX: bool = True  # Enable ConnectorX for database operations

# Create configuration instance
config = PipelineConfig()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Configurable time window at the top
END_DT_LAG_DAYS = config.END_DT_LAG_DAYS
DATA_WINDOW_DAYS = config.DATA_WINDOW_DAYS
BATCH_SIZE = config.BATCH_SIZE

# Comprehensive hourly weather metrics for spectral analysis and PCA
API_HOURLY_MEASURES = [
    "temperature_2m", "relative_humidity_2m", "dew_point_2m", "rain", "snowfall", 
    "snow_depth", "weather_code", "surface_pressure", "wind_speed_10m", 
    "wind_direction_10m", "soil_temperature_0_to_7cm", "soil_moisture_0_to_7cm", 
    "is_day", "sunshine_duration", "cloud_cover_low", 
    "shortwave_radiation", "precipitation", "cloudcover", "wind_gusts_10m"
]

start_dt = datetime(2016, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
end_dt = datetime.now(timezone.utc) - timedelta(days=END_DT_LAG_DAYS)

# Locations for comprehensive weather analysis - focused on snow/ice environments
WEATHER_LOCATIONS = [
    {"name": "Wye Creek", "country": "NZ", "lat": -45.087551, "lon": 168.810442, "timezone": "Pacific/Auckland", "venue_type": "ice_climbing"},
    {"name": "Island Gully", "country": "NZ", "lat": -42.133076, "lon": 172.755765, "timezone": "Pacific/Auckland", "venue_type": "ice_climbing"},
    {"name": "Milford Sound", "country": "NZ", "lat": -44.770974, "lon": 168.036796, "timezone": "Pacific/Auckland", "venue_type": "ice_climbing"},
    {"name": "Bush Stream", "country": "NZ", "lat": -43.8487, "lon": 170.0439, "timezone": "Pacific/Auckland", "venue_type": "ice_climbing"},
    {"name": "Shrimpton Ice", "country": "NZ", "lat": -44.222395, "lon": 169.307676, "timezone": "Pacific/Auckland", "venue_type": "ice_climbing"}
]

# Elevation and terrain analysis functions (adapted from Spectral_Analysis.py pattern)
def get_elevation(lat: float, lon: float) -> Optional[float]:
    """
    Fetch elevation for a single point using Open Elevation API.

    Args:
        lat: Latitude coordinate
        lon: Longitude coordinate

    Returns:
        Elevation in meters or None if failed

    Raises:
        APIError: If the API request fails after retries
    """
    url = "https://api.open-elevation.com/api/v1/lookup"
    params = {"locations": f"{lat},{lon}"}

    for attempt in range(config.MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=config.ELEVATION_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            if "results" not in data or not data["results"]:
                logger.warning(f"No elevation data returned for {lat}, {lon}")
                return None

            return data["results"][0]["elevation"]

        except requests.Timeout:
            logger.warning(f"Timeout fetching elevation for {lat}, {lon} (attempt {attempt + 1})")
        except requests.RequestException as e:
            logger.warning(f"Request error fetching elevation for {lat}, {lon}: {e} (attempt {attempt + 1})")
        except (KeyError, IndexError, ValueError) as e:
            logger.warning(f"Invalid response format for elevation {lat}, {lon}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching elevation for {lat}, {lon}: {e}")
            if attempt == config.MAX_RETRIES - 1:
                raise APIError(f"Failed to fetch elevation for {lat}, {lon} after {config.MAX_RETRIES} attempts")

        if attempt < config.MAX_RETRIES - 1:
            tyme.sleep(config.RETRY_DELAY * (attempt + 1))

    return None

def haversine_distance(coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
    """
    Calculate distance in meters between two (lat, lon) points using Haversine formula.

    Args:
        coord1: First coordinate as (lat, lon) tuple
        coord2: Second coordinate as (lat, lon) tuple

    Returns:
        Distance in meters

    Raises:
        ValueError: If coordinates are invalid
    """
    try:
        R = 6371000  # radius of Earth in meters
        lat1, lon1 = math.radians(coord1[0]), math.radians(coord1[1])
        lat2, lon2 = math.radians(coord2[0]), math.radians(coord2[1])

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))

        return R * c
    except (TypeError, ValueError, OverflowError) as e:
        raise ValueError(f"Invalid coordinates: {coord1}, {coord2}") from e

def get_site_elevation_and_slope(lat: float, lon: float, epsilon: float = 0.00018) -> Tuple[Optional[float], Optional[float]]:
    """
    Fetch elevation at center and surrounding points to calculate mean elevation and local slope.

    Args:
        lat: Center latitude
        lon: Center longitude  
        epsilon: Distance offset for sampling points

    Returns:
        Tuple of (mean_elevation, max_slope) where slope is max difference / distance
        Both values can be None if elevation lookup fails

    Raises:
        APIError: If all elevation lookups fail
        ValueError: If coordinates are invalid
    """
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        raise ValueError(f"Invalid coordinates: lat={lat}, lon={lon}")

    try:
        # Define sample points
        center = (lat, lon)
        north = (lat + epsilon, lon)
        south = (lat - epsilon, lon)
        east = (lat, lon + epsilon)
        west = (lat, lon - epsilon)
        points = [center, north, south, east, west]

        elevations = []
        failed_lookups = 0

        for pt in points:
            try:
                elevation = get_elevation(pt[0], pt[1])
                if elevation is not None:
                    elevations.append(elevation)
                else:
                    failed_lookups += 1
            except APIError:
                failed_lookups += 1
                continue

        # Require at least center point for calculation
        if not elevations or failed_lookups == len(points):
            logger.error(f"All elevation lookups failed for {lat}, {lon}")
            return None, None

        if failed_lookups > 0:
            logger.warning(f"{failed_lookups}/{len(points)} elevation lookups failed for {lat}, {lon}")

        mean_elev = sum(elevations) / len(elevations)

        # Calculate slope if we have the center point and at least one other point
        if len(elevations) < 2:
            return mean_elev, None

        center_elev = elevations[0]
        max_slope = 0.0

        for i, (pt, elev) in enumerate(zip(points[1:], elevations[1:]), 1):
            try:
                dist = haversine_distance(center, pt)
                if dist > 0:
                    slope = abs(center_elev - elev) / dist
                    max_slope = max(max_slope, slope)
            except ValueError as e:
                logger.warning(f"Failed to calculate slope for point {i}: {e}")
                continue

        return mean_elev, max_slope

    except Exception as e:
        logger.error(f"Unexpected error in elevation/slope calculation for {lat}, {lon}: {e}")
        raise DataProcessingError(f"Failed to calculate elevation and slope for {lat}, {lon}") from e



def check_table_status(logger: logging.Logger, client: Any, schema: str, table: str) -> Tuple[bool, bool, bool]:
    """
    Check the existence and status of a database table.

    Args:
        logger: Logger instance
        client: Database client
        schema: Database schema name
        table: Table name

    Returns:
        Tuple of (table_exists, table_empty, table_missing)
        - table_exists: whether the table exists at all
        - table_empty: table exists but is empty (truncated) 
        - table_missing: shorthand for not table_exists

    Raises:
        SchemaValidationError: If unexpected database error occurs
    """
    try:
        count_query = f"SELECT COUNT(*) FROM {schema}.{table}"
        count_result = client.execute_sql(count_query)
        row_count = next(iter(count_result))[0]
        table_empty = (row_count == 0)
        logger.info(f"Table {schema}.{table} exists with {row_count} rows")
        return True, table_empty, False

    except DatabaseUndefinedRelation:
        logger.warning(f"Table {schema}.{table} does not exist. Assuming full fetch.")
        return False, False, True

    except Exception as e:
        logger.error(f"Unexpected error checking {schema}.{table}: {e}")
        raise SchemaValidationError(f"Failed to check table status for {schema}.{table}") from e

def get_missing_datetime_ranges_sql(
    logger: logging.Logger, 
    locations: List[Dict[str, Any]], 
    start_dt: datetime, 
    end_dt: datetime, 
    pipeline: Any
) -> Tuple[Dict[str, List[Dict[str, Any]]], bool, bool, bool]:
    """
    Analyze missing data ranges and schema discrepancies using SQL.

    Args:
        logger: Logger instance
        locations: List of location dictionaries with name, timezone, etc.
        start_dt: Start datetime for analysis range
        end_dt: End datetime for analysis range  
        pipeline: DLT pipeline instance

    Returns:
        Tuple of (issues_by_location, table_exists, table_empty, table_missing)
        - issues_by_location: Dict mapping location names to lists of missing ranges
        - table_exists: Whether the target table exists
        - table_empty: Whether the table is empty
        - table_missing: Whether the table is missing (inverse of table_exists)

    Raises:
        SchemaValidationError: If SQL execution or table checking fails
        DataProcessingError: If data processing fails
    """
    if not locations:
        raise ValueError("Locations list cannot be empty")

    if start_dt >= end_dt:
        raise ValueError(f"Invalid date range: start_dt ({start_dt}) >= end_dt ({end_dt})")

    issues_by_location: Dict[str, List[Dict[str, Any]]] = {loc["name"]: [] for loc in locations}

    try:
        # Validate required fields in locations
        for loc in locations:
            required_fields = ["name", "timezone"]
            missing_fields = [field for field in required_fields if field not in loc]
            if missing_fields:
                raise ValueError(f"Location missing required fields {missing_fields}: {loc}")

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
            WHERE table_schema = 'weather_analysis' AND table_name = 'weather_hourly_enriched'
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
                CAST(MIN(datetime) AS TIMESTAMP) AS min_utc,
                CAST(MAX(datetime) AS TIMESTAMP) AS max_utc
            FROM weather_analysis.weather_hourly_enriched
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
            LEFT JOIN weather_analysis.weather_hourly_enriched e 
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
            COALESCE(mc.has_missing_columns, FALSE) AS missing_columns_flag
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
            logger.info("Executing missing ranges analysis SQL query")
            result = client.execute_sql(sql)

            processed_rows = 0
            for row in result:
                try:
                    location, gap_type, range_start, range_end, missing_hours, missing_columns_flag = row

                    # Validate extracted data
                    if not location:
                        logger.warning(f"Skipping row with empty location: {row}")
                        continue

                    if location not in issues_by_location:
                        logger.warning(f"Unknown location in SQL result: {location}")
                        continue

                    issues_by_location[location].append({
                        "type": gap_type,
                        "start": range_start,
                        "end": range_end,
                        "count": missing_hours,
                        "missing_columns": missing_columns_flag
                    })
                    processed_rows += 1

                except (ValueError, TypeError) as e:
                    logger.error(f"Error processing SQL result row {row}: {e}")
                    continue

            logger.info(f"Processed {processed_rows} missing range records from SQL")

            table_exists, table_empty, table_missing = check_table_status(
                logger, client, "weather_analysis", "weather_hourly_enriched"
            )

        return issues_by_location, table_exists, table_empty, table_missing

    except (ValueError, TypeError) as e:
        logger.error(f"Input validation error in get_missing_datetime_ranges_sql: {e}")
        raise DataProcessingError(f"Invalid input parameters: {e}") from e

    except Exception as e:
        logger.error(f"SQL execution error in get_missing_datetime_ranges_sql: {e}")
        # Return safe defaults on error
        return {}, False, True, True




def fetch_hourly_data(
    location: Dict[str, Any], 
    start_dt: datetime, 
    end_dt: datetime, 
    max_retries: Optional[int] = None, 
    retry_delay: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """
    Fetch hourly weather data from Open-Meteo API with retry logic.

    Args:
        location: Location dictionary with name, lat, lon, timezone
        start_dt: Start datetime for data fetch
        end_dt: End datetime for data fetch
        max_retries: Maximum number of retry attempts (uses config default if None)
        retry_delay: Delay between retries in seconds (uses config default if None)

    Returns:
        API response data as dictionary, or None if all attempts failed

    Raises:
        APIError: If all retry attempts fail with critical errors
        ValueError: If location data or date range is invalid
    """
    if max_retries is None:
        max_retries = config.MAX_RETRIES
    if retry_delay is None:
        retry_delay = config.RETRY_DELAY

    # Validate inputs
    required_location_fields = ["name", "lat", "lon", "timezone"]
    missing_fields = [field for field in required_location_fields if field not in location]
    if missing_fields:
        raise ValueError(f"Location missing required fields {missing_fields}: {location}")

    if start_dt >= end_dt:
        raise ValueError(f"Invalid date range for {location['name']}: {start_dt} >= {end_dt}")

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

            response = requests.get(url, params=params, timeout=config.API_TIMEOUT)
            logger.debug(f"API request URL: {response.url}")

            # Check for rate limiting (429 status)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', retry_delay * attempt))
                logger.warning(f"Rate limited for {location['name']}, waiting {retry_after}s (server-suggested)")
                tyme.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            # Validate response format
            if not isinstance(data, dict):
                logger.error(f"Invalid response format for {location['name']}: expected dict, got {type(data)}")
                continue

            if "error" in data:
                error_msg = data.get('reason', 'Unknown API error')
                logger.error(f"API error for {location['name']}: {error_msg}")
                if attempt == max_retries:
                    raise APIError(f"API error for {location['name']}: {error_msg}")
                continue

            # Validate required response structure
            if "hourly" not in data or not data["hourly"]:
                logger.warning(f"No hourly data in response for {location['name']}")
                continue

            if "time" not in data["hourly"] or not data["hourly"]["time"]:
                logger.warning(f"No time data in hourly response for {location['name']}")
                continue

            logger.info(f"Successfully fetched {len(data['hourly']['time'])} records for {location['name']}")
            return data

        except requests.Timeout:
            logger.warning(f"Timeout for {location['name']} (attempt {attempt}/{max_retries})")
        except requests.RequestException as e:
            logger.warning(f"Request error for {location['name']}: {e} (attempt {attempt}/{max_retries})")
        except (ValueError, KeyError) as e:
            logger.error(f"Data validation error for {location['name']}: {e}")
            if attempt == max_retries:
                raise APIError(f"Data validation failed for {location['name']}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error for {location['name']}: {e}")
            if attempt == max_retries:
                raise APIError(f"Unexpected error for {location['name']}: {e}")

        # Wait before retry (except on last attempt)
        if attempt < max_retries:
            wait_time = retry_delay * attempt
            logger.info(f"Retrying {location['name']} in {wait_time}s ({attempt}/{max_retries})")
            tyme.sleep(wait_time)

    logger.error(f"All {max_retries} attempts failed for {location['name']}")
    return None


def process_weather_data_pyarrow(data: Dict[str, Any], location: Dict[str, Any]) -> pa.Table:
    """
    Process weather data using PyArrow for optimal performance.

    Args:
        data: API response data containing hourly weather measurements
        location: Location metadata dictionary

    Returns:
        PyArrow Table with processed and enriched weather data

    Raises:
        DataProcessingError: If data processing fails
    """
    try:
        h = data["hourly"]

        # Create PyArrow arrays directly from API data for better performance
        arrays = {}

        arrays["datetime"] = pa.array(h["time"], type=pa.string())

        # Add all API columns efficiently
        for col in API_HOURLY_MEASURES: 
            if col in h and h[col] is not None:
                if col in ["weather_code", "is_day"]:  # Integer columns
                    arrays[col] = pa.array(h[col], type=pa.int32())
                elif col in ["precip_type", "location", "country", "timezone"]:  # String columns
                    arrays[col] = pa.array(h[col], type=pa.string())
                else:  # DOUBLE columns
                    arrays[col] = pa.array(h[col], type=pa.float64())

        # Add metadata columns
        num_rows = len(h["time"])
        arrays["location"] = pa.array([location["name"]] * num_rows)
        arrays["country"] = pa.array([location["country"]] * num_rows)
        arrays["timezone"] = pa.array([location["timezone"]] * num_rows)

        # Temperature enrichments (if temperature data exists)
        if "temperature_2m" in arrays:
            temp_values = h["temperature_2m"]
            arrays["temp_celsius"] = pa.array(temp_values, type=pa.float64())
            arrays["temp_freezing"] = pa.array([t <= 0 if t is not None else None for t in temp_values], type=pa.bool_())
            arrays["temp_below_minus5"] = pa.array([t <= -5 if t is not None else None for t in temp_values], type=pa.bool_())

        # Precipitation enrichments
        if "rain" in h and "snowfall" in h:
            rain_vals = [r if r is not None else 0.0 for r in h["rain"]]
            snow_vals = [s if s is not None else 0.0 for s in h["snowfall"]]

            total_precip = [r + s for r, s in zip(rain_vals, snow_vals)]
            arrays["total_precipitation"] = pa.array(total_precip, type=pa.float64())

            # Create precipitation type categorization
            precip_types = []
            for r, s in zip(rain_vals, snow_vals):
                if r > 0 and s > 0:
                    precip_types.append("mixed")
                elif r > 0:
                    precip_types.append("rain")
                elif s > 0:
                    precip_types.append("snow")
                else:
                    precip_types.append("none")

            arrays["precip_type"] = pa.array(precip_types, type=pa.string())

        # Wind component calculations (vectorized)
        if "wind_speed_10m" in h and "wind_direction_10m" in h:
            wind_speed = h["wind_speed_10m"]
            wind_dir = h["wind_direction_10m"]

            wind_u = []
            wind_v = []

            for speed, direction in zip(wind_speed, wind_dir):
                if speed is not None and direction is not None:
                    wind_dir_rad = direction * math.pi / 180
                    wind_u.append(-speed * math.sin(wind_dir_rad))
                    wind_v.append(-speed * math.cos(wind_dir_rad))
                else:
                    wind_u.append(None)
                    wind_v.append(None)

            arrays["wind_u"] = pa.array(wind_u, type=pa.float64())
            arrays["wind_v"] = pa.array(wind_v, type=pa.float64())

        # Create PyArrow table
        table = pa.table(arrays)

        logger.info(f"Processed {len(table)} records with {len(table.columns)} columns using PyArrow")
        return table

    except Exception as e:
        logger.error(f"PyArrow processing failed: {e}")
        raise DataProcessingError(f"Failed to process weather data with PyArrow: {e}") from e


def split_into_yearly_ranges(start_dt: datetime, end_dt: datetime) -> List[Tuple[datetime, datetime]]:
    """Split a datetime range into a list of (start, end) tuples, each covering up to one year."""
    ranges = []
    current = start_dt
    while current < end_dt:
        next_year = min(
            datetime(current.year + 1, 1, 1, tzinfo=current.tzinfo),
            end_dt
        )
        ranges.append((current, next_year))
        current = next_year
    return ranges

@dlt.source
def comprehensive_weather_source(logger: logging.Logger):
    @dlt.resource(write_disposition="merge", name="weather_hourly_enriched", primary_key=["location", "datetime"])
    def hourly_weather_data():
        """Fetch and store enriched hourly weather data from Open-Meteo for spectral analysis."""
        state = dlt.current.source_state().setdefault("hourly_weather", {
            "Processed_Ranges": {},
            "elevation_data": {}
        })
        processed = 0

        missing_ranges_by_loc, table_exists, table_empty, table_missing = get_missing_datetime_ranges_sql(
            logger, WEATHER_LOCATIONS, start_dt, end_dt, dlt.current.pipeline()
        )

        locations_to_fetch = []

        if table_missing:
            logger.info("Table does not exist - performing full fetch for all locations.")
            for location in WEATHER_LOCATIONS:
                location_copy = location.copy()
                location_copy["fetch_start"] = start_dt
                location_copy["fetch_end"] = end_dt
                locations_to_fetch.append(location_copy)

        elif table_empty:
            logger.info("Table exists but is empty (truncated) - performing full fetch for all locations.")
            for location in WEATHER_LOCATIONS:
                location_copy = location.copy()
                location_copy["fetch_start"] = start_dt
                location_copy["fetch_end"] = end_dt
                locations_to_fetch.append(location_copy)

        else:
            # Table exists and has data - fetch only missing ranges
            for location in WEATHER_LOCATIONS:
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
                        if fetch_start.tzinfo is None:
                            fetch_start = fetch_start.replace(tzinfo=tz)
                        else:
                            fetch_start = fetch_start.astimezone(tz)
                        if fetch_end.tzinfo is None:
                            fetch_end = fetch_end.replace(tzinfo=tz)
                        else:
                            fetch_end = fetch_end.astimezone(tz)
                    except Exception as e:
                        logger.warning(f"Error with timezone {location['timezone']}, using UTC: {e}")
                        fetch_start = fetch_start.astimezone(timezone.utc)
                        fetch_end = fetch_end.astimezone(timezone.utc)

                    if fetch_start > fetch_end:
                        logger.warning(f"Invalid date range {fetch_start} > {fetch_end} for {location_name}, skipping.")
                        continue

                    location_copy = location.copy()
                    location_copy["fetch_start"] = fetch_start
                    location_copy["fetch_end"] = fetch_end
                    locations_to_fetch.append(location_copy)


        if locations_to_fetch:
            logger.info(f"Fetching data for {len(locations_to_fetch)} locations year by year")

            for location in locations_to_fetch:
                location_name = location["name"]
                yearly_ranges = split_into_yearly_ranges(location["fetch_start"], location["fetch_end"])
                for yr_start, yr_end in yearly_ranges:
                    try:
                        data = fetch_hourly_data(location, yr_start, yr_end)
                        if not data or "hourly" not in data or not data["hourly"].get("time"):
                            logger.warning(f"No data returned for {location_name} {yr_start.year}, skipping.")
                            continue

                        table = process_weather_data_pyarrow(data, location)
                        logger.info(f"Processed {table.num_rows} records for {location_name} {yr_start.year}")

                        if table.num_rows == 0:
                            logger.info(f"No new data to process for {location_name} {yr_start.year}, skipping.")
                            continue

                        CHUNK_THRESHOLD = config.CHUNK_THRESHOLD
                        chunk_size = config.CHUNK_SIZE if table.num_rows > CHUNK_THRESHOLD else table.num_rows

                        for batch in table.to_batches(max_chunksize=chunk_size):
                            yield batch  # Yield PyArrow RecordBatch directly

                        processed += 1
                        logger.info(f"Processed and yielded data for {location_name} {yr_start.year}.")

                    except APIError as e:
                        logger.error(f"API error for {location_name} {yr_start.year}: {e}")
                        logger.error("Aborting further fetches for this location due to API error.")
                        break
                    except Exception as e:
                        logger.error(f"Unexpected error for {location_name} {yr_start.year}: {e}")
                        logger.error("Aborting further fetches for this location due to unexpected error.")
                        break

            logger.info(f"Raw hourly data resource finished. {processed} location-year(s) processed.")

        else:
            logger.info("No locations need data fetching.")

    return hourly_weather_data


@dlt.resource(write_disposition="merge", name="location_metadata", primary_key=["name"])
def location_metadata_resource(logger: logging.Logger):
    """
    Create enriched location metadata with elevation and terrain analysis.

    Args:
        logger: Logger instance for tracking progress

    Yields:
        Dictionary containing location metadata with elevation and slope data

    Raises:
        DataProcessingError: If critical elevation data cannot be obtained
    """
    state = dlt.current.source_state().setdefault("location_metadata", {
        "elevation_data": {}
    })

    processed_locations = 0
    failed_locations = 0

    for loc in WEATHER_LOCATIONS:
        try:
            location_name = loc["name"]

            # Check if we already have elevation data for this location
            if location_name not in state["elevation_data"]:
                logger.info(f"Fetching elevation and terrain data for {location_name}")
                try:
                    elevation, slope = get_site_elevation_and_slope(loc["lat"], loc["lon"])

                    # Store in state to avoid re-fetching
                    state["elevation_data"][location_name] = {
                        "elevation": elevation,
                        "slope": slope
                    }
                    logger.info(f"Successfully cached elevation data for {location_name}: {elevation}m elevation, {slope} slope")

                except (APIError, DataProcessingError, ValueError) as e:
                    logger.error(f"Failed to fetch elevation for {location_name}: {e}")
                    # Store None values to avoid retrying every run
                    state["elevation_data"][location_name] = {
                        "elevation": None,
                        "slope": None
                    }
                    failed_locations += 1
            else:
                logger.info(f"Using cached elevation data for {location_name}")

            elevation = state["elevation_data"][location_name]["elevation"]
            slope = state["elevation_data"][location_name]["slope"]

            # Validate required location fields
            required_fields = ["name", "country", "lat", "lon", "timezone", "venue_type"]
            missing_fields = [field for field in required_fields if field not in loc]
            if missing_fields:
                logger.error(f"Location {location_name} missing required fields: {missing_fields}")
                continue

            processed_locations += 1
            yield {
                "name": location_name,
                "country": loc["country"],
                "lat": float(loc["lat"]),
                "lon": float(loc["lon"]),
                "timezone": loc["timezone"],
                "venue_type": loc["venue_type"],
                "elevation": elevation,
                "slope": slope
            }

        except Exception as e:
            logger.error(f"Unexpected error processing location {loc.get('name', 'unknown')}: {e}")
            failed_locations += 1
            continue

    logger.info(f"Location metadata resource finished. {processed_locations} location(s) processed, {failed_locations} failed.")

    if processed_locations == 0:
        raise DataProcessingError("No locations were successfully processed")




# In your main pipeline run:
if __name__ == "__main__":
    pipeline_start_time = tyme.time()
    logger.info(f"Weather pipeline started at {datetime.now()}")

    try:
        # Validate required environment variables
        required_env_vars = ["DLT_DESTINATION"]
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise WeatherPipelineError(f"Missing required environment variables: {missing_vars}")

        pipeline = dlt.pipeline(
            pipeline_name="comprehensive_weather_pipeline",
            destination=os.getenv("DLT_DESTINATION", "motherduck"),
            dataset_name="weather_analysis",
            pipelines_dir=str(DLT_PIPELINE_DIR),
            dev_mode=False
        )

        # Configure DLT for optimal performance with PyArrow
        logger.info("Using PyArrow optimizations for data processing")

        # Check existing pipeline state
        try:
            existing_tables = pipeline.dataset().table_names
            logger.info(f"Existing tables in dataset: {existing_tables}")
        except (PipelineNeverRan, DatabaseUndefinedRelation, ValueError, KeyError) as e:
            logger.warning(f"No previous runs or table found: {e}. Assuming first run or empty DB.")

        # Run comprehensive weather data pipeline
        logger.info("Starting comprehensive weather data pipeline...")

        try:
            source = comprehensive_weather_source(logger)
            load_info = pipeline.run(source)
            logger.info(f"Weather data pipeline completed successfully. Load Info: {load_info}")

            # Log processed ranges for monitoring
            state = source.state.get('hourly_weather', {}).get('Processed_Ranges', {})
            if state:
                logger.info(f"Processed date ranges: {state}")
            else:
                logger.info("No processed date ranges found in state.")

        except Exception as e:
            logger.error(f"Weather data pipeline failed: {e}")
            raise WeatherPipelineError(f"Weather data pipeline execution failed: {e}") from e

        # Run location metadata resource
        logger.info("Starting location metadata pipeline...")

        try:
            metadata_load_info = pipeline.run(location_metadata_resource(logger))
            logger.info(f"Location metadata pipeline completed successfully. Load Info: {metadata_load_info}")
        except Exception as e:
            logger.error(f"Location metadata pipeline failed: {e}")
            raise WeatherPipelineError(f"Location metadata pipeline execution failed: {e}") from e

        # Create database indexes for performance
        logger.info("Creating database indexes for better query performance...")

        try:
            with pipeline.sql_client() as client:
                indexes_to_create = [
                    ("idx_weather_enriched_datetime", "weather_analysis.weather_hourly_enriched(datetime)"),
                    ("idx_weather_enriched_location_datetime", "weather_analysis.weather_hourly_enriched(location, datetime)"),
                ]

                created_indexes = 0
                for index_name, index_definition in indexes_to_create:
                    try:
                        client.execute_sql(f"CREATE INDEX IF NOT EXISTS {index_name} ON {index_definition};")
                        created_indexes += 1
                        logger.debug(f"Created/verified index: {index_name}")
                    except Exception as e:
                        logger.warning(f"Failed to create index {index_name}: {e}")

                logger.info(f"Successfully created/verified {created_indexes}/{len(indexes_to_create)} indexes")

        except Exception as e:
            logger.error(f"Index creation failed: {e}")
            # Don't fail the entire pipeline for index issues
            logger.warning("Continuing despite index creation failures...")

        # Calculate and log performance metrics
        pipeline_duration = tyme.time() - pipeline_start_time
        logger.info(f"Pipeline completed successfully in {pipeline_duration:.2f} seconds")

        # Log final success metrics
        logger.info("=" * 50)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info(f"Start time: {datetime.fromtimestamp(pipeline_start_time)}")
        logger.info(f"End time: {datetime.now()}")
        logger.info(f"Duration: {pipeline_duration:.2f} seconds")
        logger.info(f"Weather data load info: {load_info}")
        logger.info(f"Metadata load info: {metadata_load_info}")
        logger.info("=" * 50)

    except WeatherPipelineError:
        # Re-raise our custom exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Unexpected pipeline failure: {e}")
        raise WeatherPipelineError(f"Pipeline failed with unexpected error: {e}") from e