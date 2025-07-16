import dlt
import logging
from dotenv import load_dotenv
from dlt.sources.helpers import requests
from os import getenv
import json
import datetime
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
    {"name": "SE Face Mount Ward", "lat": -43.867239, "lon": 169.833754}
]
# 12m buffer around each point
EPSILON = 0.00018

START_DATE = "2022-05-01"
END_DATE = "2025-07-15"

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
            "aggregationInterval": { "of": "P1D" },
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



def classify_ice_point(ndsi, ndwi, ndii):
    if ndsi > 0.40 and ndii < 0.70 and ndwi < 0.25:
        return "Good Ice Conditions"
    elif ndsi > 0.40 and (ndii >= 0.70 or ndwi >= 0.25):
        return "Wet Conditions"
    elif 0.20 < ndsi <= 0.40:
        return "Patchy Conditions"
    elif ndsi > 0.4 and ndii < 0.3:
        return "Drier Ice Conditions"
    else:
        return "Bare Rock or error"


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

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["location"] = location
    df["ndsi_smooth"] = df["ndsi"].rolling(window=5, center=True, min_periods=1).mean()
    df["ndwi_smooth"] = df["ndwi"].rolling(window=5, center=True, min_periods=1).mean()
    df["ndii_smooth"] = df["ndii"].rolling(window=5, center=True, min_periods=1).mean()
    df["label"] = df.apply(lambda row: classify_ice_point(row["ndsi_smooth"], row["ndwi_smooth"], row["ndii_smooth"]), axis=1)
    
    return df.to_dict(orient="records")


@dlt.source
def sentinel_source(logger: logging.Logger, token: str, start_date: str, end_date: str):
    @dlt.resource(write_disposition="merge", name="ice_indices", 
                  primary_key=["location", "date"])
    def ice_indices_resource():
        for loc in LOCATIONS:
            BBOX = [loc["lon"] - EPSILON, loc["lat"] - EPSILON, loc["lon"] + EPSILON, loc["lat"] + EPSILON]
            raw = fetch_stats_ndsi(token, BBOX, start_date, end_date, logger)
            yield from process_and_store(raw, loc["name"])
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

    source = sentinel_source(logger, token, START_DATE, END_DATE)
    load_info = pipeline.run(source)
    logger.info(f"✅ Pipeline run completed: {load_info}")
