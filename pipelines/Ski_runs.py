import dlt
from dlt.sources.helpers import requests
import os
import logging
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
from geopy.distance import geodesic

paths = get_project_paths()
set_dlt_env_vars(paths)
DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
load_dotenv(dotenv_path=ENV_FILE)

SKI_FIELDS = [
    {"name": "Remarkables", "country": "NZ", "lat": -45.0579, "lon": 168.8194},
    {"name": "Cardrona", "country": "NZ", "lat": -44.8746, "lon": 168.9481},
    {"name": "Treble Cone", "country": "NZ", "lat": -44.6335, "lon": 168.8972},
    {"name": "Mount Hutt", "country": "NZ", "lat": -43.4707, "lon": 171.5306},
    {"name": "Ohau", "country": "NZ", "lat": -44.2255, "lon": 169.7747},
    {"name": "Coronet Peak", "country": "NZ", "lat": -44.9206, "lon": 168.7349},
    {"name": "Whakapapa", "country": "NZ", "lat": -39.2546, "lon": 175.5456},
    {"name": "Turoa", "country": "NZ", "lat": -39.3067, "lon": 175.5289},
    {"name": "Thredbo", "country": "AU", "lat": -36.5040, "lon": 148.2987},
    {"name": "Perisher", "country": "AU", "lat": -36.4058, "lon": 148.4134},
    {"name": "Mt Buller", "country": "AU", "lat": -37.1467, "lon": 146.4473},
    {"name": "Falls Creek", "country": "AU", "lat": -36.8655, "lon": 147.2861},
    {"name": "Mt Hotham", "country": "AU", "lat": -36.9762, "lon": 147.1359},
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
BASE_URL = "https://overpass-api.de/api/interpreter"

def get_elevations_batch(coords_list):
    if not coords_list:
        return []
    BATCH_SIZE = 200
    results = []
    for i in range(0, len(coords_list), BATCH_SIZE):
        batch = coords_list[i:i+BATCH_SIZE]
        locations = "|".join(f"{lat},{lon}" for lat, lon in batch)
        url = f"https://api.open-elevation.com/api/v1/lookup?locations={locations}"
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            elevations = [res.get("elevation") for res in r.json().get("results", [])]
            while len(elevations) < len(batch):
                elevations.append(None)
        except Exception as e:
            elevations = [None] * len(batch)
        results.extend(elevations)
    return results

@dlt.source
def ski_source():
    # --------- FETCH ALL RUNS ONLY ONCE, SHARE AS VARIABLE ----------
    ski_runs_data = []
    radius_m = 3000
    for field in SKI_FIELDS:
        query = f"""
        [out:json][timeout:60];
        way["piste:type"](around:{radius_m},{field["lat"]},{field["lon"]});
        out tags geom;
        """
        logger.info(f"Fetching ski runs near {field['name']} ...")
        response = requests.post(BASE_URL, data={"data": query}, timeout=90)
        elements = response.json().get("elements", [])
        for run in elements:
            if "geometry" not in run or not run["geometry"]:
                continue
            ski_runs_data.append({
                "osm_id": run["id"],
                "resort": field["name"],
                "country_code": field["country"],
                "tags": run.get("tags", {}),
                "geometry": run["geometry"],
            })

    @dlt.resource(table_name="ski_runs", write_disposition="replace")
    def ski_runs():
        for run in ski_runs_data:
            tags = run.get("tags", {})
            coords = [(pt["lat"], pt["lon"]) for pt in run["geometry"]]
            if len(coords) < 2:
                continue
            run_length = sum(
                geodesic(coords[i], coords[i + 1]).meters
                for i in range(len(coords) - 1)
            )
            yield {
                "osm_id": run["osm_id"],
                "resort": run["resort"],
                "country_code": run["country_code"],
                "run_name": tags.get("name", ""),
                "difficulty": tags.get("piste:difficulty"),
                "piste_type": tags.get("piste:type"),
                "run_length_m": run_length,
                "n_points": len(coords),
            }

    @dlt.resource(table_name="ski_run_points", write_disposition="replace")
    def ski_run_points():
        for run in ski_runs_data:
            tags = run.get("tags", {})
            coords = [(pt["lat"], pt["lon"]) for pt in run["geometry"]]
            if len(coords) < 2:
                continue
            cum_distances = [0.0]
            for i in range(1, len(coords)):
                cum_distances.append(
                    cum_distances[-1] + geodesic(coords[i - 1], coords[i]).meters
                )
            elevations = get_elevations_batch(coords)
            for idx, ((lat, lon), dist, elev) in enumerate(zip(coords, cum_distances, elevations)):
                yield {
                    "osm_id": run["osm_id"],
                    "resort": run["resort"],
                    "country_code": run["country_code"],
                    "run_name": tags.get("name", ""),
                    "point_index": idx,
                    "lat": lat,
                    "lon": lon,
                    "distance_along_run_m": dist,
                    "elevation_m": elev,
                }

    return [ski_runs, ski_run_points]

def run_pipeline(logger):
    logger.info("Starting DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="ski_run_pipeline",
        destination=os.getenv("DLT_DESTINATION"),
        dataset_name="main",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )
    try:
        pipeline.run(ski_source())
    except Exception as e:
        logger.error(f"❌ Pipeline run failed: {e}")
        return False

if __name__ == "__main__":
    run_pipeline(logger=logger)
    logger.info("Pipeline run completed successfully.")
