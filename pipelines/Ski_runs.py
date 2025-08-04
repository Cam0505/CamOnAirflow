import dlt
from dlt.sources.helpers import requests
import os
import logging
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
from geopy.distance import geodesic
from dlt.destinations.exceptions import DatabaseUndefinedRelation
import numpy as np
import math

# --- ENV setup ---
paths = get_project_paths()
set_dlt_env_vars(paths)
DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
load_dotenv(dotenv_path=ENV_FILE)


GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("Missing GOOGLE_API_KEY in environment.")


AVERAGE_LIFT_SPEEDS = {
    "chair_lift": 2.5,
    "gondola": 5.0,
    "drag_lift": 2.0,
    "t-bar": 2.0,
    "j-bar": 1.8,
    "rope_tow": 1.5,
    "platter": 2.0,
    "magic_carpet": 1.0,
    "mixed_lift": 3.0,
    "funicular": 5.0,
    "cable_car": 6.0
}


SKI_FIELDS = [
    {"name": "Broken River Ski Area", "country": "NZ", "region": "Canterbury"},
    {"name": "Cardrona Alpine Resort", "country": "NZ", "region": "Otago"},
    {"name": "Coronet Peak", "country": "NZ", "region": "Otago"},
    {"name": "Coronet Peak Ski Area", "country": "NZ", "region": "Otago"},
    {"name": "Craigieburn Valley Ski Area", "country": "NZ", "region": "Canterbury"},
    {"name": "Fox Peak Ski Area", "country": "NZ", "region": "Canterbury"},
    {"name": "Hanmer Springs Ski Area", "country": "NZ", "region": "Canterbury"},
    {"name": "Manganui Ski Area", "country": "NZ", "region": "Taranaki"},
    {"name": "Mount Cheeseman Ski Area", "country": "NZ", "region": "Canterbury"},
    {"name": "Mount Dobson Ski Field", "country": "NZ", "region": "Canterbury"},
    {"name": "Mount Hutt Ski Area", "country": "NZ", "region": "Canterbury"},
    {"name": "Mount Lyford Alpine Resort", "country": "NZ", "region": "Canterbury"},
    {"name": "Mount Olympus Ski Area", "country": "NZ", "region": "Canterbury"},
    {"name": "Mt Cheeseman Ski Area", "country": "NZ", "region": "Canterbury"},
    {"name": "Porters Ski Area", "country": "NZ", "region": "Canterbury"},
    {"name": "Rainbow Ski Area", "country": "NZ", "region": "Tasman"},
    {"name": "Roundhill Ski Field", "country": "NZ", "region": "Canterbury"},
    {"name": "Temple Basin", "country": "NZ", "region": "Canterbury"},
    {"name": "Temple Basin Ski Area", "country": "NZ", "region": "Canterbury"},
    {"name": "The Remarkables Ski Area", "country": "NZ", "region": "Otago"},
    {"name": "Treble Cone Ski Area", "country": "NZ", "region": "Otago"},
    {"name": "Tukino Skifield", "country": "NZ", "region": "Manawatu-Wanganui"},
    {"name": "Tūroa Ski Area", "country": "NZ", "region": "Manawatu-Wanganui"},
    {"name": "Whakapapa Ski Area", "country": "NZ", "region": "Manawatu-Wanganui"},
    {"name": "Ōhau Snow Fields", "country": "NZ", "region": "Canterbury"},
    {"name": "Charlotte Pass", "country": "AU", "region": "New South Wales"},
    {"name": "Falls Creek", "country": "AU", "region": "Victoria"},
    {"name": "Mount Baw Baw", "country": "AU", "region": "Victoria"},
    {"name": "Mount Buller", "country": "AU", "region": "Victoria"},
    {"name": "Mount Hotham", "country": "AU", "region": "Victoria"},
    {"name": "Perisher", "country": "AU", "region": "New South Wales"},
    {"name": "Selwyn Snow Resort", "country": "AU", "region": "New South Wales"},
    {"name": "Thredbo Resort", "country": "AU", "region": "New South Wales"}
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
        url = (
            f"https://maps.googleapis.com/maps/api/elevation/json"
            f"?locations={locations}&key={GOOGLE_API_KEY}"
        )
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

def smooth_steep_gradients(elevs, dists, threshold=0.80, window=7):
    elevs = np.array(elevs, dtype=float)
    dists = np.array(dists, dtype=float)
    if len(elevs) < 4:
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
    if len(elevations) < 4:
        return coords, elevations
    max_elev = max(elevations)
    min_elev = min(elevations)
    start_idx = next(i for i, e in enumerate(elevations) if e == max_elev)
    end_idx = len(elevations) - 1 - next(i for i, e in enumerate(reversed(elevations)) if e == min_elev)
    if end_idx < start_idx:
        return coords, elevations
    return coords[start_idx:end_idx+1], elevations[start_idx:end_idx+1]

def compute_turniness(coords):
    if len(coords) < 4:
        return 0.0
    headings = []
    for i in range(1, len(coords)):
        lat1, lon1 = coords[i-1]
        lat2, lon2 = coords[i]
        heading = math.atan2(lon2 - lon1, lat2 - lat1)
        headings.append(heading)
    turniness = sum(abs(headings[i] - headings[i-1]) for i in range(1, len(headings)))
    return float(turniness)

# Add this function after the existing helper functions

def get_top_bottom_coordinates(coords, elevations=None):
    """Get top and bottom coordinates for a run or lift."""
    if not coords or len(coords) < 2:
        return None, None, None, None, None, None

    if elevations and len(elevations) == len(coords):
        # For ski runs, use elevation-based top/bottom
        max_idx = elevations.index(max(elevations))
        min_idx = elevations.index(min(elevations))
        top_coords = coords[max_idx]
        bottom_coords = coords[min_idx]
        top_elevation = elevations[max_idx]
        bottom_elevation = elevations[min_idx]
    else:
        # For lifts or when no elevation data, use first/last points
        top_coords = coords[0]
        bottom_coords = coords[-1]
        top_elevation = None
        bottom_elevation = None

    return (top_coords[0], top_coords[1], top_elevation, 
            bottom_coords[0], bottom_coords[1], bottom_elevation)

@dlt.source
def ski_source(known_locations: set):
    ski_runs_data = []
    ski_lifts = []
    for field in SKI_FIELDS:
        if field["name"] in known_locations:
            logger.info(f"Skipping {field['name']} - already in database")
            continue

        query = f'''
        [out:json][timeout:60];
        area["name"="{field['name']}"]["landuse"="winter_sports"]->.a;
        (
          way["piste:type"~"^(downhill|nordic|skitour|snow_park|sled|connection)$"](area.a);
          way["aerialway"](area.a);
        );
        out tags geom;
        '''

        logger.info(f"Fetching ski runs for {field['name']} ...")
        response = requests.post(BASE_URL, data={"data": query}, timeout=90)
        if response.status_code != 200:
            logger.error(f"Overpass API error for {field['name']}: {response.status_code} {response.text}")
            continue
        elements = response.json().get("elements", [])
        for run in elements:
            tags = run.get("tags", {})
            geometry = run.get("geometry", [])
            coords = [(pt["lat"], pt["lon"]) for pt in geometry] if geometry else []
            element_length = sum(
                geodesic(coords[i], coords[i + 1]).meters
                for i in range(len(coords) - 1)
            )


            if "piste:type" in tags:
                if not coords:
                    continue
                ski_runs_data.append({
                    "osm_id": run["id"],
                    "resort": field["name"],
                    "country_code": field["country"],
                    "region": field["region"],
                    "tags": tags,
                    "coords": coords,  # store coords directly
                    "length_m": element_length,
                })
            elif "aerialway" in tags:
                # Filter out magic carpets and platter lifts
                aerialway_type = tags.get("aerialway", "").lower()
                name = tags.get("name", "unnamed").lower()
                if aerialway_type in ["station", "goods"]:
                    logger.info(f"Filtering out {aerialway_type} lift: {name} at {field['name']}")
                    continue

                # if "learner" in name or "beginner" in name:
                #     logger.info(f"Filtering out learner lift: {name} at {field['name']}")
                #     continue


                ski_lifts.append({
                    "osm_id": run["id"],
                    "resort": field["name"],
                    "country_code": field["country"],
                    "region": field["region"],
                    "lift_type": aerialway_type,
                    "name": name,
                    "duration": tags.get("duration"),
                    "capacity": tags.get("aerialway:capacity"),
                    "occupancy": tags.get("aerialway:occupancy"),
                    "coords": coords,  # store coords directly
                    "length_m": element_length,
                })

    @dlt.resource(write_disposition="merge", table_name="ski_runs", primary_key=["osm_id"])
    def ski_runs():
        for run in ski_runs_data:
            tags = run.get("tags", {})
            coords = run.get("coords", [])
            if len(coords) < 3:
                continue

            turniness_score = compute_turniness(coords)

            # Get elevations for top/bottom calculation
            elevations = get_elevations_batch(coords)
            top_lat, top_lon, top_elev, bottom_lat, bottom_lon, bottom_elev = get_top_bottom_coordinates(coords, elevations)

            yield {
                "osm_id": run["osm_id"],
                "resort": run["resort"],
                "country_code": run["country_code"],
                "region": run["region"],
                "run_name": tags.get("name", ""),
                "difficulty": tags.get("piste:difficulty"),
                "piste_type": tags.get("piste:type"),
                "grooming": tags.get("piste:grooming"),
                "lit": tags.get("piste:lit"),
                "run_length_m": run.get("length_m", 0),
                "n_points": len(coords),
                "turniness_score": turniness_score,
                "top_lat": top_lat,
                "top_lon": top_lon,
                "top_elevation_m": top_elev,
                "bottom_lat": bottom_lat,
                "bottom_lon": bottom_lon,
                "bottom_elevation_m": bottom_elev,
            }

    @dlt.resource(write_disposition="merge", table_name="ski_run_points", primary_key=["osm_id", "point_index"])
    def ski_run_points():
        for run in ski_runs_data:
            tags = run.get("tags", {})
            coords = run.get("coords", [])
            if len(coords) < 4:
                continue
            elevations = get_elevations_batch(coords)
            coords, elevations = trim_to_downhill(coords, elevations)
            # --- Solution 1: Filter out if not enough points after trimming ---
            if len(coords) < 4:
                logger.warning(f"Skipping run {tags.get('name', '')} ({run['osm_id']}) after trim_to_downhill: only {len(coords)} points left.")
                continue
            # Ensure top-to-bottom order
            if elevations[0] < elevations[-1]:
                coords = coords[::-1]
                elevations = elevations[::-1]

            cum_distances = [0.0]
            steep_count = 0
            for i in range(1, len(coords)):
                dist = geodesic(coords[i - 1], coords[i]).meters
                cum_distances.append(cum_distances[-1] + dist)
                if dist > 0:
                    gradient = abs((elevations[i] - elevations[i - 1]) / dist)
                    if gradient > 2:  # 200% gradient threshold
                        steep_count += 1
            if steep_count > 3:
                logger.warning(f"Skipping run {tags.get('name', '')} ({run['osm_id']}) due to steep segments: count={steep_count}")
                continue

            if len(elevations) >= 3:
                elevations_smooth, gradients_smooth = smooth_steep_gradients(
                    elevations, cum_distances, threshold=0.80, window=7
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
                    "elevation_smoothed_m": float(elev_sm) if elev_sm is not None else None,
                    "gradient_smoothed": float(grad_sm) if grad_sm is not None else None
                }

    @dlt.resource(write_disposition="merge", table_name="ski_lifts", primary_key=["osm_id"])
    def ski_lifts_resource():
        for lift in ski_lifts:
            coords = lift.get("coords", [])
            if len(coords) < 2:
                continue

            lift_length = lift.get("length_m", 0)
            if lift_length < 100:
                logger.info(f"Skipping {lift['name']} - length < 100m")
                continue

            lift_type = lift.get("lift_type")
            average_speed = AVERAGE_LIFT_SPEEDS.get(lift_type)

            if lift_length and average_speed:
                duration_sec = lift_length / average_speed
            else:
                duration_sec = None

            lift_speed = lift_length / duration_sec if lift_length and duration_sec and duration_sec > 0 else None

            # Get top/bottom coordinates (for lifts, first point is typically bottom, last is top)
            bottom_lat, bottom_lon, bottom_elev, top_lat, top_lon, top_elev = get_top_bottom_coordinates(coords)

            yield {
                "osm_id": lift["osm_id"],
                "resort": lift["resort"],
                "country_code": lift["country_code"],
                "region": lift["region"],
                "lift_type": lift_type,
                "name": lift.get("name"),
                "duration": duration_sec, 
                "capacity": lift.get("capacity"),
                "occupancy": lift.get("occupancy"),
                "lift_length_m": lift_length,
                "lift_speed_mps": lift_speed,
                "top_lat": top_lat,
                "top_lon": top_lon,
                "top_elevation_m": top_elev,
                "bottom_lat": bottom_lat,
                "bottom_lon": bottom_lon,
                "bottom_elevation_m": bottom_elev,
            }

    return [ski_runs, ski_run_points, ski_lifts_resource]

def run_pipeline(logger):
    logger.info("Starting DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="ski_run_pipeline",
        destination=os.getenv("DLT_DESTINATION"),
        dataset_name="ski_runs",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )
    dataset = None
    known_locations = set()

    try:
        logger.info("Trying to access table: ski_runs")
        dataset = pipeline.dataset()["ski_runs"].df()
        if dataset is not None and not dataset.empty:
            known_locations = set(dataset["resort"].unique())
            logger.info(f"Found {len(known_locations)} known locations: {known_locations}")
    except (ValueError, KeyError, DatabaseUndefinedRelation) as e:
        logger.info(f"Table ski_runs not found: {e}")
    except Exception as e:
        logger.warning(f"Error accessing table ski_runs: {e}")

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
