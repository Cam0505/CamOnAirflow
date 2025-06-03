import dlt
from dlt.sources.helpers import requests
from typing import Iterator, Dict
import os
import logging
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from datetime import datetime, timedelta, UTC
from math import radians, sin, cos, sqrt, atan2
from airportsdata import load
from dateutil.parser import isoparse
import time

# Load environment variables

paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
DBT_DIR = paths["DBT_DIR"]

load_dotenv(dotenv_path=ENV_FILE)
# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ICAO24_LIST = [
    "a0f1bb",   # Delta Air Lines (USA, international)
    "4401d8",   # Austrian Airlines (Austria, international)
    "3c6444",   # Lufthansa (Germany, A320, international)
    "a1d0c6",   # Southwest Airlines (USA, international)
    "e8063d",   # Emirates (UAE, international)
    "394c05",   # Air France (France, international)
]

BASE_URL = "https://opensky-network.org/api/flights/aircraft"

airports = load('ICAO')  # or 'IATA'

def get_airport_coords(icao):
    info = airports.get(icao)
    if info:
        return float(info['lat']), float(info['lon'])
    return None, None

@dlt.source
def myshiptracking_source(logger: logging.Logger, latest_timestamps):
    """
    dlt source for ship/flight tracking.
    Only yields records with a timestamp newer than the latest in the dataset.
    """
    @dlt.resource(write_disposition="append", name="flight_tracking")
    def vessel_positions() -> Iterator[Dict]:
        now_ts = int(datetime.now(UTC).timestamp())
        default_start_ts = int((datetime.now(UTC) - timedelta(days=2)).timestamp())
        for icao24 in ICAO24_LIST:
            logger.info(f"Fetching flights for ICAO24: {icao24}")
            last_ts = latest_timestamps.get(str(icao24), default_start_ts) + 1
            params = {
                "icao24": icao24,
                "begin": last_ts,
                "end": now_ts
            }
            logger.debug(f"Request params: {params}")
            try:
                response = requests.get(BASE_URL, params=params)
                logger.debug(f"API URL: {response.url}")
                response.raise_for_status()
                data = response.json()
                logger.info(f"API returned {len(data) if isinstance(data, list) else 'non-list'} records for {icao24}")
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    logger.warning("Rate limit hit (429). Sleeping for 60 seconds.")
                    time.sleep(60)
                    continue
                status_code = getattr(e.response, "status_code", None)
                if status_code == 404:
                    logger.warning(f"No flight data found for {icao24} in this time window.")
                else:
                    logger.error(f"Failed to fetch data for {icao24}: {e}")
                continue
            except Exception as e:
                logger.error(f"Failed to fetch data for {icao24}: {e}")
                continue

            if not isinstance(data, list) or not data:
                logger.warning(f"No data returned for {icao24}")
                continue

            for record in data:
                logger.debug(f"Processing record: {record}")
                dep_icao = record.get("estDepartureAirport")
                arr_icao = record.get("estArrivalAirport")
                icao24 = record.get("icao24")
                callsign = record.get("callsign")
                dep_time = record.get("firstSeen")
                arr_time = record.get("lastSeen")
                # Convert UNIX timestamps to ISO datetime strings (UTC)
                dep_datetime = (
                    datetime.fromtimestamp(dep_time, UTC).isoformat() if dep_time else None
                )
                arr_datetime = (
                    datetime.fromtimestamp(arr_time, UTC).isoformat() if arr_time else None
                )
                # Only calculate if both airports are present
                if dep_icao and arr_icao:
                    dep_lat, dep_lon = get_airport_coords(dep_icao)
                    arr_lat, arr_lon = get_airport_coords(arr_icao)
                    if None not in (dep_lat, dep_lon, arr_lat, arr_lon):
                        distance_km = haversine(dep_lat, dep_lon, arr_lat, arr_lon)
                    else:
                        distance_km = None
                else:
                    distance_km = None

                yield {
                    "icao24": icao24,
                    "departure_airport": dep_icao,
                    "arrival_airport": arr_icao,
                    "callsign": callsign,
                    "departure_datetime": dep_datetime,
                    "arrival_datetime": arr_datetime,
                    "distance_km": distance_km
                }
    yield vessel_positions

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi/2)**2 + cos(phi1)*cos(phi2)*sin(dlambda/2)**2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))





if __name__ == "__main__":

    pipeline = dlt.pipeline(
        pipeline_name="opensky_flights",
        destination=os.getenv("DLT_DESTINATION"),
        dataset_name="flight_data",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )
    latest_timestamps = {}
    try:
        dataset = pipeline.dataset()["flight_tracking"].df()
        if dataset is not None:
            try:
            # Ensure 'mmsi' and 'arrival_datetime' columns exist
                for icao24, group in dataset.groupby("icao24"):
                    latest_row = group.sort_values("arrival_datetime").iloc[-1]
                    try:
                        arrival_dt = latest_row["arrival_datetime"]
                        if hasattr(arrival_dt, "timestamp"):
                            # pandas.Timestamp or datetime object
                            latest_timestamps[str(icao24)] = int(arrival_dt.timestamp())
                        else:
                            # string
                            latest_timestamps[str(icao24)] = int(isoparse(arrival_dt).timestamp())
                    except Exception as e:
                        logger.warning(f"Could not parse arrival_datetime for {icao24}: {e}")
            except Exception as e:
                logger.warning(f"Could not extract previous arrival_datetime: {e}")
    except PipelineNeverRan:
        logger.warning(
            "⚠️ No previous runs found for this pipeline. Assuming first run.")
    except DatabaseUndefinedRelation:
        logger.warning(
            "⚠️ Table Doesn't Exist. Assuming truncation.")

    try:
        source = myshiptracking_source(logger, latest_timestamps)
        load_info = pipeline.run(source)
        logger.info(f"Load Info: {load_info}")
    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
        raise