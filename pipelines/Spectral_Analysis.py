import dlt
import logging
from dotenv import load_dotenv
from dlt.sources.helpers import requests
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from os import getenv
import json
import math
import pandas as pd
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

SENTINEL_CLIENT_ID = getenv("SENTINEL_CLIENT_ID")
if not SENTINEL_CLIENT_ID:
    raise ValueError("Missing SENTINEL_CLIENT_ID in environment.")

SENTINEL_CLIENT_SECRET = getenv("SENTINEL_CLIENT_SECRET")
if not SENTINEL_CLIENT_SECRET:
    raise ValueError("Missing SENTINEL_CLIENT_SECRET in environment.")

LOCATIONS = [
    {"name": "Wye Creek", "lat": -45.087551, "lon": 168.810442},
    {"name": "SE Face Mount Ward", "lat": -43.867239, "lon": 169.833754},
    {"name": "Island Gully", "lat": -42.133076, "lon": 172.755765},
    {"name": "Shrimpton Ice", "lat": -44.222395, "lon": 169.307676}
]
# 12m buffer around each point
EPSILON = 0.00018

START_DATE = date(2021, 1, 1)
BUFFER_DAYS = 7  # configurable number of days to reprocess to handle late arrivals
TODAY = datetime.now(ZoneInfo("Australia/Sydney")).date()
END_DATE = TODAY - timedelta(days=2)

def json_converter(o):
    if isinstance(o, date):
        return o.isoformat()
    return str(o)

def get_access_token():
    url = "https://services.sentinel-hub.com/oauth/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": SENTINEL_CLIENT_ID,
        "client_secret": SENTINEL_CLIENT_SECRET,
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]


def get_elevation(lat, lon):
    url = "https://api.open-elevation.com/api/v1/lookup"
    params = {"locations": f"{lat},{lon}"}
    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return resp.json()["results"][0]["elevation"]
    except Exception as e:
        logging.warning(f"Failed to fetch elevation for {lat}, {lon}: {e}")
        return None

def get_site_elevation_and_slope(lat, lon, epsilon=0.00018):
    """
    Fetches the elevation at the center, N, S, E, W, and calculates mean elevation and local slope (max difference / distance).
    """
    # Define sample points
    center = (lat, lon)
    north = (lat + epsilon, lon)
    south = (lat - epsilon, lon)
    east = (lat, lon + epsilon)
    west = (lat, lon - epsilon)
    points = [center, north, south, east, west]
    elevations = []
    for pt in points:
        elevations.append(get_elevation(pt[0], pt[1]))
    # Remove any Nones (failed lookups)
    elevations = [e for e in elevations if e is not None]
    if not elevations:
        return None, None
    mean_elev = sum(elevations) / len(elevations)
    # Slope: max diff between center and other points, divided by distance (in meters)
    center_elev = elevations[0]
    max_slope = 0
    for pt, elev in zip(points[1:], elevations[1:]):
        # Approximate distance in meters
        dist = haversine_distance(center, pt)
        if dist > 0:
            slope = abs(center_elev - elev) / dist
            max_slope = max(max_slope, slope)
    return mean_elev, max_slope

def haversine_distance(coord1, coord2):
    # Returns distance in meters between two (lat, lon) points
    R = 6371000  # radius of Earth in meters
    phi1, phi2 = math.radians(coord1[0]), math.radians(coord2[0])
    d_phi = math.radians(coord2[0] - coord1[0])
    d_lambda = math.radians(coord2[1] - coord1[1])
    a = math.sin(d_phi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(d_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def fetch_stats_ndsi(access_token, bbox, start_date, end_date, logger):
    url = "https://services.sentinel-hub.com/api/v1/statistics"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    evalscript = """//VERSION=3
function setup() {
  return {
    input: [{bands: ["B03", "B08", "B11", "dataMask"]}],
    output: [
            { id: "ndsi", bands: 1, sampleType: "FLOAT32" },
            { id: "ndwi", bands: 1, sampleType: "FLOAT32" },
            { id: "ndii", bands: 1, sampleType: "FLOAT32" },
            { id: "dataMask", bands: 1 }
            ]
  };
}
function evaluatePixel(sample) {
    let ndsi = (sample.B03 + sample.B11) === 0 ? 0 : (sample.B03 - sample.B11) / (sample.B03 + sample.B11);
    let ndwi = (sample.B03 + sample.B08) === 0 ? 0 : (sample.B03 - sample.B08) / (sample.B03 + sample.B08);
    let ndii = (sample.B08 + sample.B11) === 0 ? 0 : (sample.B08 - sample.B11) / (sample.B08 + sample.B11);
    return {
        ndsi: [ndsi],
        ndwi: [ndwi],
        ndii: [ndii],
        dataMask: [sample.dataMask]
    };
}"""

    stats_request = {
        "input": {
            "bounds": {
                "bbox": bbox,
                "properties": {
                    "crs": "http://www.opengis.net/def/crs/EPSG/0/4326"
                }
            },
            "data": [{
                "type": "sentinel-2-l2a",
                "dataFilter": {
                    "timeRange": {
                        "from": f"{start_date}T00:00:00Z",
                        "to": f"{end_date}T23:59:59Z"
                    },
                    "maxCloudCoverage": 75,
                    "mosaickingOrder": "leastCC"
                }
            }]
        },
        "aggregation": {
            "timeRange": {
                "from": f"{start_date}T00:00:00Z",
                "to": f"{end_date}T23:59:59Z"
            },
            "aggregationInterval": {"of": "P1D"},
            "evalscript": evalscript,
            "resx": 10,
            "resy": 10,
            "meta": True
        },
        "calculations": {
            "ndsi": {
                "statistics": {
                    "B0": {
                        "stats": ["mean"]
                    }
                }
            },
            "ndwi": {
                "statistics": {
                    "B0": {
                        "stats": ["mean"]
                    }
                }
            },
            "ndii": {
                "statistics": {
                    "B0": {
                        "stats": ["mean"]
                    }
                }
            }
        }
    }


    try:
        response = requests.post(url, headers=headers, json=stats_request)
        response.raise_for_status()
        # print("✅ Raw response:")
        # print(json.dumps(response.json(), indent=2))
        return response.json()
    except requests.RequestException as err:
        logger.error(f"❌ HTTP Error: {err}")
        logger.error(f"❌ Response content: {err.response}")
        raise



def process_and_store(data, location, elev, slope):
    rows = []
    for t in data["data"]:
        d = t["interval"]["from"][:10]
        stats = t["outputs"]
        try:
            ndsi = stats["ndsi"]["bands"]["B0"]["stats"]["mean"]
            ndwi = stats["ndwi"]["bands"]["B0"]["stats"]["mean"]
            ndii = stats["ndii"]["bands"]["B0"]["stats"]["mean"]
        except (KeyError, IndexError):
            continue
        rows.append({"date": d, "ndsi": ndsi, "ndwi": ndwi, "ndii": ndii})

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["location"] = location
    df["ndsi_smooth"] = df["ndsi"].rolling(window=5, center=True, min_periods=1).mean()
    df["ndwi_smooth"] = df["ndwi"].rolling(window=5, center=True, min_periods=1).mean()
    df["ndii_smooth"] = df["ndii"].rolling(window=5, center=True, min_periods=1).mean()
    df["Elevation"] = elev
    df["Slope"] = slope
    
    return df.to_dict(orient="records") 


def compute_fetch_ranges(
    global_min: date,
    global_max: date,
    state_min: date | None,
    state_max: date | None,
    buffer_days: int = 7,
    truncation: bool = False
) -> list[tuple[date, date]]:
    """
    Returns a list of (fetch_start, fetch_end) tuples for needed fetches.
    If no fetching needed, returns [].
    - If truncation, always fetch (global_min, global_max).
    - If no state (first run), fetch (global_min, global_max).
    - If state_min > global_min, fetch (global_min, state_min).
    - If state_max < global_max, fetch (max(global_min, state_max-buffer), global_max).
    - If fully up-to-date, return [].
    """
    ranges = []

    if truncation:
        return [(global_min, global_max)]

    # First run, or state wiped
    if state_min is None or state_max is None:
        return [(global_min, global_max)]

    # Gap at the beginning: state doesn't cover full global_min
    if state_min > global_min:
        ranges.append((global_min, state_min))

    # Gap at the end: state doesn't reach to global_max (add buffer)
    if state_max < global_max:
        fetch_start = max(global_min, state_max - timedelta(days=buffer_days))
        if fetch_start < global_max:  # Make sure range makes sense
            ranges.append((fetch_start, global_max))

    return ranges


@dlt.source
def sentinel_source(logger: logging.Logger, token: str, locations_with_data: set):
    @dlt.resource(write_disposition="merge", name="ice_indices", 
                  primary_key=["location", "date"])
    def ice_indices_resource():
        state = dlt.current.source_state().setdefault("ice_indices", {
            "location_dates": {},
            "location_status": {},
            "run_dates": {},
            "slope&elevations": {}
        })

        for loc in LOCATIONS:
            loc_name = loc["name"]

            # Always clear run_dates for this location at the start of this run
            state["run_dates"][loc_name] = []

            # Pull relevant values from state and row_max_min_dict
            truncation = loc_name not in locations_with_data
            state_dates = state["location_dates"].get(loc_name, {})
            state_min = state_dates.get("start_date")
            state_max = state_dates.get("end_date")

            if loc_name not in state["slope&elevations"]:
                logger.info(f"Fetching elevation and slope for {loc_name}")
                elev, slope = get_site_elevation_and_slope(loc["lat"], loc["lon"], EPSILON)
                # Set elevation and slope in state
                state["slope&elevations"][loc_name] = {
                    "elevation": elev,
                    "slope": slope
                }
            else:
                logger.info(f"Using cached elevation and slope for {loc_name}")
                elev = state["slope&elevations"][loc_name]["elevation"]
                slope = state["slope&elevations"][loc_name]["slope"]

            BBOX = [loc["lon"] - EPSILON, loc["lat"] - EPSILON, loc["lon"] + EPSILON, loc["lat"] + EPSILON]


            ranges = compute_fetch_ranges(
                global_min=START_DATE,
                global_max=END_DATE,
                state_min=state_min,
                state_max=state_max,
                buffer_days=BUFFER_DAYS,
                truncation=truncation
            )

            try:
                fetched_any = False

                if ranges:
                    # Set run_dates to exactly the attempted ranges
                    state["run_dates"][loc_name] = ranges

                    # Compute min/max for location_dates in a simple way
                    run_start = min(fetch_start for fetch_start, _ in ranges)
                    run_end = max(fetch_end for _, fetch_end in ranges)

                    # Combine with previous state if present
                    new_start = min([d for d in [state_min, run_start] if d is not None])
                    new_end = max([d for d in [state_max, run_end] if d is not None])

                    state["location_dates"][loc_name] = {
                        "start_date": new_start,
                        "end_date": new_end
                    }

                    for fetch_start, fetch_end in ranges:
                        logger.info(f"Fetching {loc_name} from {fetch_start} to {fetch_end}")
                        try:
                            raw = fetch_stats_ndsi(token, BBOX, fetch_start, fetch_end, logger)
                            records = process_and_store(raw, loc_name, elev, slope)
                            if records:
                                fetched_any = True
                                yield from records
                        except Exception as e:
                            logger.error(f"❌ Failed fetching {loc_name} range {fetch_start} to {fetch_end}: {e}")
                else:
                    logger.info(f"{loc_name} is up-to-date")
                    state["run_dates"][loc_name] = []

                # Update location_status
                if fetched_any:
                    state["location_status"][loc_name] = "success"
                elif ranges:
                    state["location_status"][loc_name] = "no_data"
                else:
                    state["location_status"][loc_name] = "skipped"

            except Exception as e:
                logger.error(f"❌ Unexpected failure for {loc_name}: {e}")
                state["run_dates"][loc_name] = []
                state["location_status"][loc_name] = "failed"
            
    return ice_indices_resource


if __name__ == "__main__":
    token = get_access_token()
    print("✅ Authenticated")
    logger = logging.getLogger(__name__)
    pipeline = dlt.pipeline(
        pipeline_name="ice_quality_ndsi",
        destination="motherduck",
        dataset_name="main",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )

    locations_with_data = set()
    try:
        dataset = pipeline.dataset()["ice_indices"].df()
        if dataset is not None:
            non_empty_locations = dataset["location"].unique()
            locations_with_data = set(str(loc) for loc in non_empty_locations)
    except PipelineNeverRan:
        logger.warning(
            "⚠️ No previous runs found for this pipeline. Assuming first run.")
    except DatabaseUndefinedRelation:
        logger.warning(
            "⚠️ Table Doesn't Exist. Assuming truncation.")
    except ValueError as ve:
        logger.warning(
            f"⚠️ ValueError: {ve}. Assuming first run or empty dataset.")

    source = sentinel_source(logger, token, locations_with_data)
    try:
        load_info = pipeline.run(source)
        outcome_data = source.state.get('ice_indices', {})
        logger.info("Weather State Metadata:\n" +
                    json.dumps(outcome_data, indent=2, default=json_converter))
        logger.info(f"✅ Pipeline run completed: {load_info}")
    except Exception as e:
        logger.error(f"❌ Pipeline run failed: {e}")
