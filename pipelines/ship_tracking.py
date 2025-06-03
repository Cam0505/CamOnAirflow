import dlt
from dlt.sources.helpers import requests
from typing import Iterator, Dict
import os
import logging
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from math import radians, cos, sin, asin, sqrt

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


# Replace with your actual API key
API_KEY = "YOUR_API_KEY"

# Example list of international vessel MMSIs (replace or expand as needed)
VESSEL_MMSIS = [
    "370447000",  # Example: PANAMA flag
    "563000000",  # Example: SINGAPORE flag
    "636091234",  # Example: LIBERIA flag
]

BASE_URL = "https://www.myshiptracking.com/api/v1/positions"

def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great-circle distance between two points in km"""
    R = 6371  # Earth radius in kilometers
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))


def get_vessel_position(mmsi: str) -> Dict:
    logger.info(f"Fetching position for MMSI: {mmsi}")
    try:
        response = requests.get(BASE_URL, params={"api_key": API_KEY, "ship_id": mmsi})
        response.raise_for_status()
    except requests.HTTPError as e:
        logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
        return {}
    except requests.RequestException as e:
        logger.error(f"Request error: {e}")
        return {}

    data = response.json()
    if isinstance(data, list) and data:
        return data[0] 
    return {}

@dlt.source
def myshiptracking_source(logger: logging.Logger, previous_dataset):
    prev_positions = {}
    if previous_dataset is not None:
        try:
            for _, row in previous_dataset.iterrows():
                mmsi = str(row.get("mmsi"))
                if mmsi and row.get("lat") and row.get("lon"):
                    prev_positions[mmsi] = (row["lat"], row["lon"])
        except Exception as e:
            logger.warning(f"Could not extract previous positions: {e}")

    @dlt.resource(write_disposition="replace", name="ship_tracking")
    def vessel_positions() -> Iterator[Dict]:
        for mmsi in VESSEL_MMSIS:
            data = get_vessel_position(mmsi)
            if not data:
                continue

            current_lat = data.get("lat")
            current_lon = data.get("lon")

            # Compute distance if previous position is available
            prev = prev_positions.get(mmsi)
            if prev and current_lat is not None and current_lon is not None:
                try:
                    distance_km = haversine(prev[0], prev[1], current_lat, current_lon)
                    data["distance_km"] = round(distance_km, 3)
                    logger.info(f"MMSI {mmsi} moved {distance_km:.2f} km since last run")
                except Exception as e:
                    logger.warning(f"Could not calculate distance for MMSI {mmsi}: {e}")
                    data["distance_km"] = None
            else:
                data["distance_km"] = None

            yield data

    yield vessel_positions

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="myshiptracking",
        destination=os.getenv("DLT_DESTINATION"),
        dataset_name="maritime_data",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )

    try:
        dataset = pipeline.dataset()["ship_tracking"].df()
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

    try:
        source = myshiptracking_source(logger, dataset)
        load_info = pipeline.run(source)
        logger.info(f"Load Info: {load_info}")
    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
        raise