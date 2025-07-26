import dlt
from dlt.sources.helpers import requests
import os
import logging
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
from geopy.distance import geodesic
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
import numpy as np

# --- ENV setup ---
paths = get_project_paths()
set_dlt_env_vars(paths)
DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
load_dotenv(dotenv_path=ENV_FILE)

SKI_FIELDS = [
    {"name": "Remarkables", "country": "NZ", "lat": -45.0579, "lon": 168.8194, "radius_m": 3000},
    {"name": "Cardrona", "country": "NZ", "lat": -44.8746, "lon": 168.9481, "radius_m": 3000},
    {"name": "Treble Cone", "country": "NZ", "lat": -44.6335, "lon": 168.8972, "radius_m": 3000},
    {"name": "Mount Hutt", "country": "NZ", "lat": -43.4707, "lon": 171.5306, "radius_m": 3000},
    {"name": "Ohau", "country": "NZ", "lat": -44.2255, "lon": 169.7747, "radius_m": 3000},
    {"name": "Coronet Peak", "country": "NZ", "lat": -44.9206, "lon": 168.7349, "radius_m": 3000},
    {"name": "Whakapapa", "country": "NZ", "lat": -39.2546, "lon": 175.5456, "radius_m": 3000},
    {"name": "Turoa", "country": "NZ", "lat": -39.3067, "lon": 175.5289, "radius_m": 3000},
    {"name": "Mount Dobson", "country": "NZ", "lat": -43.9419, "lon": 170.6648, "radius_m": 3000},
    {"name": "Mount Olympus", "country": "NZ", "lat": -43.1917, "lon": 171.6062, "radius_m": 3000},
    {"name": "Mount Cheeseman", "country": "NZ", "lat": -43.1573, "lon": 171.6683, "radius_m": 1500},
    {"name": "Temple Basin", "country": "NZ", "lat": -42.9087, "lon": 171.5766, "radius_m": 3000},
    {"name": "Porters", "country": "NZ", "lat": -43.2703, "lon": 171.6294, "radius_m": 3000},
    {"name": "RoundHill", "country": "NZ", "lat": -43.8263, "lon": 170.6617, "radius_m": 3000}
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
            logger.error(f"Error fetching elevations: {e}")
            elevations = [None] * len(batch)
        results.extend(elevations)
    return results

def smooth_steep_gradients(elevs, dists, threshold=0.25, window=5):
    elevs = np.array(elevs, dtype=float)
    dists = np.array(dists, dtype=float)
    if len(elevs) < 3:
        return elevs.tolist(), [0.0] * len(elevs)
    grads = np.gradient(elevs, dists, edge_order=2)
    smoothed = elevs.copy()
    for i in range(len(elevs)):
        if abs(grads[i]) > threshold:
            w_start = max(0, i - window//2)
            w_end = min(len(elevs), i + window//2 + 1)
            smoothed[i] = np.mean(elevs[w_start:w_end])
    smoothed_grads = np.gradient(smoothed, dists, edge_order=2)
    return smoothed.tolist(), smoothed_grads.tolist()

def trim_to_downhill(coords, elevations):
    """
    Returns the coords and elevations from the first max to the last min (inclusive).
    """
    if len(elevations) < 2:
        return coords, elevations
    max_elev = max(elevations)
    min_elev = min(elevations)
    start_idx = next(i for i, e in enumerate(elevations) if e == max_elev)
    # find last min_elev index, search from end
    end_idx = len(elevations) - 1 - next(i for i, e in enumerate(reversed(elevations)) if e == min_elev)
    if end_idx < start_idx:
        # Bad geometry, skip trimming
        return coords, elevations
    return coords[start_idx:end_idx+1], elevations[start_idx:end_idx+1]

@dlt.source
def ski_source(known_locations: set):
    ski_runs_data = []
    for field in SKI_FIELDS:
        # Skip fields that are already in the database
        if field["name"] in known_locations:
            logger.info(f"Skipping {field['name']} - already in database")
            continue

        query = f"""
        [out:json][timeout:60];
        way["piste:type"](around:{field['radius_m']},{field["lat"]},{field["lon"]});
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

    @dlt.resource(write_disposition="merge", table_name="ski_runs", primary_key=["osm_id"])
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

    @dlt.resource(write_disposition="merge", table_name="ski_run_points", primary_key=["osm_id", "point_index"])
    def ski_run_points():
        for run in ski_runs_data:
            tags = run.get("tags", {})
            coords = [(pt["lat"], pt["lon"]) for pt in run["geometry"]]
            if len(coords) < 2:
                continue
            elevations = get_elevations_batch(coords)
            # --- Prune to downhill segment before distance calculation ---
            coords, elevations = trim_to_downhill(coords, elevations)
            if len(coords) < 2:
                continue
            # Now recalculate cumulative distances
            cum_distances = [0.0]
            for i in range(1, len(coords)):
                cum_distances.append(
                    cum_distances[-1] + geodesic(coords[i - 1], coords[i]).meters
                )
            if len(elevations) >= 3:
                elevations_smooth, gradients_smooth = smooth_steep_gradients(
                    elevations, cum_distances, threshold=0.25, window=5
                )
            else:
                elevations_smooth = elevations
                gradients_smooth = [0.0] * len(elevations)
            for idx, ((lat, lon), dist, elev, elev_sm, grad_sm) in enumerate(
                zip(coords, cum_distances, elevations, elevations_smooth, gradients_smooth)
            ):
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
                    "elevation_smoothed_m": elev_sm,
                    "gradient_smoothed": grad_sm,
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
    dataset = None
    known_locations = set()

    # Try different table names that might exist
    table_name = "ski_runs"

    try:
        logger.info(f"Trying to access table: {table_name}")
        dataset = pipeline.dataset()[table_name].df()
        if dataset is not None and not dataset.empty:
            known_locations = set(dataset["resort"].unique())
            logger.info(f"Found {len(known_locations)} known locations: {known_locations}")
    except (ValueError, KeyError, DatabaseUndefinedRelation) as e:
        logger.info(f"Table {table_name} not found: {e}")
    except Exception as e:
        logger.warning(f"Error accessing table {table_name}: {e}")

    if not known_locations:
        logger.info("No existing ski runs data found, treating as first run")


    try:
        pipeline.run(ski_source(known_locations))
    except Exception as e:
        logger.error(f"❌ Pipeline run failed: {e}")
        return False

if __name__ == "__main__":
    run_pipeline(logger=logger)
    logger.info("Pipeline run completed successfully.")
