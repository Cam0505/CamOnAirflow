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
import time
import pyarrow as pa
import numpy as np
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
logging.getLogger("dlt").setLevel(logging.INFO)

SENTINEL_CLIENT_ID = getenv("SENTINEL_CLIENT_ID")
if not SENTINEL_CLIENT_ID:
    raise ValueError("Missing SENTINEL_CLIENT_ID in environment.")

SENTINEL_CLIENT_SECRET = getenv("SENTINEL_CLIENT_SECRET")
if not SENTINEL_CLIENT_SECRET:
    raise ValueError("Missing SENTINEL_CLIENT_SECRET in environment.")

LOCATIONS = [
    {"name": "Wye Creek", "lat": -45.087551, "lon": 168.810442},
    {"name": "Island Gully", "lat": -42.133076, "lon": 172.755765},
    {"name": "Milford Sound", "lat": -44.770974, "lon": 168.036796},
    {"name": "Bush Stream", "lat": -43.8487, "lon": 170.0439},
    {"name": "Shrimpton Ice", "lat": -44.222395, "lon": 169.307676}
]
# 12m buffer around each point
EPSILON = 0.00018

START_DATE = date(2015, 1, 1)
BUFFER_DAYS = 7  # configurable number of days to reprocess to handle late arrivals
TODAY = datetime.now(ZoneInfo("Pacific/Auckland")).date()
END_DATE = TODAY - timedelta(days=2)

MAX_RETRIES = 3
RETRY_DELAY = 6  # seconds
API_TIMEOUT = 60  # seconds

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


    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url, headers=headers, json=stats_request, timeout=API_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.Timeout:
            logger.warning(f"Timeout fetching Sentinel data (attempt {attempt}/{MAX_RETRIES})")
        except requests.RequestException as err:
            logger.warning(f"HTTP error fetching Sentinel data: {err} (attempt {attempt}/{MAX_RETRIES})")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY * attempt)
    logger.error(f"All {MAX_RETRIES} attempts failed for Sentinel API fetch.")
    return None



def process_and_store(data, location):
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

    if not rows:
        return

    # Convert date strings to datetime.date objects with error handling
    date_objs = []
    for idx, r in enumerate(rows):
        try:
            date_obj = datetime.strptime(r["date"], "%Y-%m-%d").date()
            date_objs.append(date_obj)
        except Exception as e:
            print(f"[ERROR] Failed to convert date '{r['date']}' at row {idx} for location '{location}': {e}")
            continue

    if len(date_objs) != len(rows):
        print(f"[WARNING] Some dates could not be converted for location '{location}'. {len(rows) - len(date_objs)} rows skipped.")

    # Only keep rows with valid dates
    valid_rows = [r for i, r in enumerate(rows) if i < len(date_objs)]

    if not date_objs or not valid_rows:
        print(f"[ERROR] No valid dates to store for location '{location}'.")
        return

    # Prepare arrays for smoothing
    ndsi_arr = np.array([r["ndsi"] for r in valid_rows], dtype=np.float32)
    ndwi_arr = np.array([r["ndwi"] for r in valid_rows], dtype=np.float32)
    ndii_arr = np.array([r["ndii"] for r in valid_rows], dtype=np.float32)

    def rolling_avg(arr, window=5):
        out = []
        for i in range(len(arr)):
            start = max(0, i - window // 2)
            end = min(len(arr), i + window // 2 + 1)
            vals = arr[start:end]
            vals = vals[~np.isnan(vals)]
            out.append(float(np.mean(vals)) if len(vals) else None)
        return out

    ndsi_smooth = rolling_avg(ndsi_arr)
    ndwi_smooth = rolling_avg(ndwi_arr)
    ndii_smooth = rolling_avg(ndii_arr)

    table = pa.table({
        "date": pa.array(date_objs, pa.date32()),
        "location": pa.array([location] * len(date_objs), pa.string()),
        "ndsi": pa.array([r["ndsi"] for r in valid_rows], pa.float32()),
        "ndsi_smooth": pa.array(ndsi_smooth, pa.float32()),
        "ndwi": pa.array([r["ndwi"] for r in valid_rows], pa.float32()),
        "ndwi_smooth": pa.array(ndwi_smooth, pa.float32()),
        "ndii": pa.array([r["ndii"] for r in valid_rows], pa.float32()),
        "ndii_smooth": pa.array(ndii_smooth, pa.float32())
    })

    return table


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
            "run_dates": {}
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
                            records = process_and_store(raw, loc_name)

                            if records is None:
                                logger.warning(f"No data returned for {loc_name} range {fetch_start} to {fetch_end}")
                                continue

                            CHUNK_THRESHOLD = 50000
                            chunk_size = 10000 if records.num_rows > CHUNK_THRESHOLD else records.num_rows

                            for batch in records.to_batches(max_chunksize=chunk_size):
                                yield batch  # Yield PyArrow RecordBatch directly
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
