import dlt
from dlt.sources.helpers import requests
import os
import logging
from time import perf_counter
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
    # Canadian Ski Resorts
    {"name": "Whistler Blackcomb", "country": "CA", "region": "British Columbia"},
    # {"name": "Banff Sunshine", "country": "CA", "region": "Alberta"},
    # {"name": "Lake Louise Ski Resort", "country": "CA", "region": "Alberta"},
    # {"name": "Nakiska", "country": "CA", "region": "Alberta"},
    # {"name": "Fernie Alpine", "country": "CA", "region": "British Columbia"},
    # {"name": "Kicking Horse", "country": "CA", "region": "British Columbia"},
    # {"name": "Red Mountain", "country": "CA", "region": "British Columbia"},
    # {"name": "Silver Star", "country": "CA", "region": "British Columbia"},
    # {"name": "Sun Peaks", "country": "CA", "region": "British Columbia"},
    # {"name": "Cypress Mountain", "country": "CA", "region": "British Columbia"},
    {"name": "Grouse Mountain", "country": "CA", "region": "British Columbia"},
    {"name": "Marmot Basin", "country": "CA", "region": "Alberta"},
    {"name": "Fortress Mountain Resort", "country": "CA", "region": "Alberta"},
    # New Zealand Ski Resorts
    {"name": "Broken River Ski Area", "country": "NZ", "region": "Canterbury"},
    {"name": "Cardrona Alpine Resort", "country": "NZ", "region": "Otago"},
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
    {"name": "Porters Ski Area", "country": "NZ", "region": "Canterbury"},
    {"name": "Rainbow Ski Area", "country": "NZ", "region": "Tasman"},
    {"name": "Roundhill Ski Field", "country": "NZ", "region": "Canterbury"},
    {"name": "Temple Basin Ski Area", "country": "NZ", "region": "Canterbury"},
    {"name": "The Remarkables Ski Area", "country": "NZ", "region": "Otago"},
    {"name": "Treble Cone Ski Area", "country": "NZ", "region": "Otago"},
    {"name": "Tukino Skifield", "country": "NZ", "region": "Manawatu-Wanganui"},
    {"name": "Tūroa Ski Area", "country": "NZ", "region": "Manawatu-Wanganui"},
    {"name": "Whakapapa Ski Area", "country": "NZ", "region": "Manawatu-Wanganui"},
    {"name": "Ōhau Snow Fields", "country": "NZ", "region": "Canterbury"},
    # Australian Ski Resorts
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
DESTINATION_DATABASE = "camonairflow"
MIN_LIFT_LENGTH_M = 100
PISTE_TYPE_REGEX = "^(downhill|nordic|skitour|snow_park|sled|connection)$"
IGNORED_AERIALWAY_REGEX = "^(station|goods)$"


def _post_overpass_query(query, field_name, timeout=90, stats=None, time_key=None):
    started_at = perf_counter()
    response = requests.post(BASE_URL, data={"data": query}, timeout=timeout)
    if stats is not None and time_key is not None:
        stats[time_key] += perf_counter() - started_at
    if response.status_code != 200:
        raise RuntimeError(
            f"Overpass API error for {field_name}: {response.status_code} {response.text}"
        )
    return response.json().get("elements", [])



def _fetch_all_resorts_candidate_ids(stats=None):
    area_clauses = "\n".join(
        f'      area["name"="{field["name"]}"]["landuse"="winter_sports"];'
        for field in SKI_FIELDS
    )
    query = f'''
    [out:json][timeout:90];
    (
{area_clauses}
    )->.all_areas;
    (
      way["piste:type"~"{PISTE_TYPE_REGEX}"](area.all_areas);
      way["aerialway"]
        ["aerialway"!~"{IGNORED_AERIALWAY_REGEX}"]
        (area.all_areas)
        (if: length() >= {MIN_LIFT_LENGTH_M});
    );
    out tags;
    '''
    if stats is not None:
        stats["overpass_id_requests_sent"] += 1

    run_ids = set()
    lift_ids = set()
    raw_elements = _post_overpass_query(
        query,
        "all configured ski resorts",
        timeout=120,
        stats=stats,
        time_key="overpass_id_time_s",
    )

    for element in raw_elements:
        element_id = element.get("id")
        tags = element.get("tags", {})
        if element_id is None:
            continue
        if "piste:type" in tags:
            run_ids.add(element_id)
        elif "aerialway" in tags:
            lift_ids.add(element_id)

    logger.info(
        "Global ID preflight result: candidate_run_ids=%s | candidate_lift_ids=%s",
        len(run_ids),
        len(lift_ids),
    )

    return run_ids, lift_ids


def _fetch_resort_elements(field, stats=None):
    query = f'''
    [out:json][timeout:60];
    area["name"="{field['name']}"]["landuse"="winter_sports"]->.a;
    (
      way["piste:type"~"{PISTE_TYPE_REGEX}"](area.a);
      way["aerialway"](area.a);
    );
    out body geom;
    '''
    if stats is not None:
        stats["overpass_requests_sent"] += 1
    return _post_overpass_query(
        query,
        field["name"],
        stats=stats,
        time_key="overpass_geom_time_s",
    )


def get_elevations_batch(coords_list, stats=None):
    if not coords_list:
        return []
    BATCH_SIZE = 200
    results = []
    for i in range(0, len(coords_list), BATCH_SIZE):
        batch = coords_list[i:i+BATCH_SIZE]
        locations = "|".join(f"{lat},{lon}" for lat, lon in batch)
        try:
            if stats is not None:
                stats["elevation_requests_sent"] += 1
                stats["elevation_points_requested"] += len(batch)
            started_at = perf_counter()
            r = requests.get(
                "https://maps.googleapis.com/maps/api/elevation/json",
                params={"locations": locations, "key": GOOGLE_API_KEY},
                timeout=10,
            )
            if stats is not None:
                stats["elevation_time_s"] += perf_counter() - started_at
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

def get_top_bottom_coordinates(coords, elevations=None, is_lift=False):
    """
    Get top and bottom coordinates for a run or lift.
    For ski runs: First point is top, last point is bottom (flow top-to-bottom)
    For ski lifts: First point is bottom, last point (where skiers get off)
    """
    if not coords or len(coords) < 2:
        return None, None, None, None, None, None

    if is_lift:
        # For lifts: First point is bottom, last point is top
        bottom_coords = coords[0]  # First point (where skiers get on)
        top_coords = coords[-1]    # Last point (where skiers get off)
        
        # Get elevations if available
        if elevations and len(elevations) == len(coords):
            bottom_elevation = elevations[0]
            top_elevation = elevations[-1]
        else:
            bottom_elevation = None
            top_elevation = None
    else:
        # For ski runs: First point is top, last point is bottom
        top_coords = coords[0]     # First point (where skiing begins)
        bottom_coords = coords[-1]  # Last point (where skiing ends)
        
        # Get elevations if available
        if elevations and len(elevations) == len(coords):
            top_elevation = elevations[0]
            bottom_elevation = elevations[-1]
        else:
            top_elevation = None
            bottom_elevation = None

    return (top_coords[0], top_coords[1], top_elevation, 
            bottom_coords[0], bottom_coords[1], bottom_elevation)

@dlt.source
def ski_source(known_run_osm: set, known_lift_osm: set):
    ski_runs_data = []
    ski_lifts = []
    should_preflight_ids = bool(known_run_osm or known_lift_osm)
    stats = {
        "overpass_requests_sent": 0,
        "overpass_id_requests_sent": 0,
        "overpass_id_time_s": 0.0,
        "overpass_geom_time_s": 0.0,
        "elevation_requests_sent": 0,
        "elevation_points_requested": 0,
        "elevation_time_s": 0.0,
        "elements_seen": 0,
        "elements_sent_to_elevation": 0,
        "preflight_run_ids_seen": 0,
        "preflight_lift_ids_seen": 0,
        "preflight_new_run_ids": 0,
        "preflight_new_lift_ids": 0,
        "resorts_skipped_geometry": 0,
        "resorts_with_new_ids": 0,
        "skipped_known_run": 0,
        "skipped_known_lift": 0,
        "skipped_unknown_type": 0,
        "skipped_insufficient_coords": 0,
        "skipped_filtered_lift_type": 0,
        "skipped_short_lift": 0,
    }
    if should_preflight_ids:
        try:
            logger.info("Checking OSM ids across all configured resorts before geometry fetch ...")
            run_ids, lift_ids = _fetch_all_resorts_candidate_ids(stats=stats)
            new_run_ids = run_ids - known_run_osm
            new_lift_ids = lift_ids - known_lift_osm

            stats["preflight_run_ids_seen"] = len(run_ids)
            stats["preflight_lift_ids_seen"] = len(lift_ids)
            stats["preflight_new_run_ids"] = len(new_run_ids)
            stats["preflight_new_lift_ids"] = len(new_lift_ids)

            if not new_run_ids and not new_lift_ids:
                stats["resorts_skipped_geometry"] = len(SKI_FIELDS)
                stats["resorts_with_new_ids"] = 0
                logger.info("No new OSM ids across configured resorts, skipping all geometry fetches")
            else:
                logger.info(
                    "New OSM ids across configured resorts | runs=%s | lifts=%s",
                    len(new_run_ids),
                    len(new_lift_ids),
                )
                stats["resorts_with_new_ids"] = len(SKI_FIELDS)
                stats["resorts_skipped_geometry"] = 0
                logger.info("New IDs found globally, fetching geometry for all configured resorts")
        except Exception as e:
            logger.warning(
                f"Global ID preflight failed, falling back to geometry fetch: {e}"
            )
            should_preflight_ids = False

    for field in SKI_FIELDS:
        if should_preflight_ids and stats["preflight_new_run_ids"] == 0 and stats["preflight_new_lift_ids"] == 0:
            continue 

        logger.info(f"Fetching ski runs for {field['name']} ...")
        try:
            elements = _fetch_resort_elements(field, stats=stats)
        except Exception as e:
            logger.error(str(e))
            continue

        for run in elements:
            stats["elements_seen"] += 1
            run_id = run.get("id")
            tags = run.get("tags", {})
            geometry = run.get("geometry", [])
            nodes = run.get("nodes", [])
            coords = [(pt["lat"], pt["lon"]) for pt in geometry] if geometry else []

            # Add these checks for debugging node/coord issues
            if len(coords) != len(nodes):
                logger.warning(f"coords and nodes length mismatch for OSM ID {run_id}: coords={len(coords)}, nodes={len(nodes)}")
            if not nodes:
                logger.warning(f"No node_ids for OSM ID {run_id} ({tags.get('name', '')})")

            is_run = "piste:type" in tags
            is_lift = "aerialway" in tags
            if not is_run and not is_lift:
                stats["skipped_unknown_type"] += 1
                continue

            if is_run and run_id in known_run_osm:
                stats["skipped_known_run"] += 1
                continue

            if is_lift and run_id in known_lift_osm:
                stats["skipped_known_lift"] += 1
                continue

            if len(coords) < 2:
                stats["skipped_insufficient_coords"] += 1
                # logger.info(f"Skipping OSM ID {run_id}: insufficient coordinates ({len(coords)})")
                continue

            element_length = sum(
                geodesic(coords[i], coords[i + 1]).meters
                for i in range(len(coords) - 1)
            )

            aerialway_type = None
            name = tags.get("name", "unnamed").lower()
            if is_lift:
                aerialway_type = tags.get("aerialway", "").lower()
                if aerialway_type in ["station", "goods"]:
                    stats["skipped_filtered_lift_type"] += 1
                    # logger.info(f"Filtering out {aerialway_type} lift: {name} at {field['name']}")
                    continue
                if element_length < MIN_LIFT_LENGTH_M:
                    stats["skipped_short_lift"] += 1
                    # logger.info(f"Skipping {name} - length < {MIN_LIFT_LENGTH_M}m")
                    continue

            # Calculate elevations ONCE for all coords
            stats["elements_sent_to_elevation"] += 1
            elevations = get_elevations_batch(coords, stats=stats)

            if is_run:
                # Calculate cumulative distances once
                cum_distances = [0.0]
                for i in range(1, len(coords)):
                    dist = geodesic(coords[i - 1], coords[i]).meters
                    cum_distances.append(cum_distances[-1] + dist)

                # Smooth elevations if needed
                if len(elevations) >= 3:
                    elevations_smooth, gradients_smooth = smooth_steep_gradients(
                        elevations, cum_distances, threshold=0.80, window=7
                    )
                else:
                    elevations_smooth = elevations
                    gradients_smooth = [0.0] * len(elevations)

                # Calculate top/bottom coordinates ONCE
                top_lat, top_lon, top_elev, bottom_lat, bottom_lon, bottom_elev = get_top_bottom_coordinates(coords, elevations)
                
                # Calculate turniness once
                turniness_score = compute_turniness(coords)

                ski_runs_data.append({
                    "osm_id": run_id,
                    "resort": field["name"],
                    "country_code": field["country"],
                    "region": field["region"],
                    "tags": tags,
                    "coords": coords,  # store coords directly
                    "node_ids": nodes,  # store node ids
                    "length_m": element_length,
                    "elevations": elevations,  # store all elevations
                    "elevations_smooth": elevations_smooth,  # store smoothed elevations
                    "gradients_smooth": gradients_smooth,  # store gradients
                    "cum_distances": cum_distances,  # store distances
                    "top_lat": top_lat,  # store top coordinates
                    "top_lon": top_lon,
                    "top_elev": top_elev,
                    "bottom_lat": bottom_lat,  # store bottom coordinates
                    "bottom_lon": bottom_lon,
                    "bottom_elev": bottom_elev,
                    "turniness_score": turniness_score  # store turniness
                })
            elif is_lift:
                # Calculate top/bottom coordinates ONCE with is_lift=True
                top_lat, top_lon, top_elev, bottom_lat, bottom_lon, bottom_elev = get_top_bottom_coordinates(coords, elevations, is_lift=True)

                ski_lifts.append({
                    "osm_id": run_id,
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
                    "elevations": elevations,  # store elevations
                    "top_lat": top_lat,  # store top coordinates
                    "top_lon": top_lon,
                    "top_elev": top_elev,
                    "bottom_lat": bottom_lat,  # store bottom coordinates
                    "bottom_lon": bottom_lon,
                    "bottom_elev": bottom_elev
                })

    skipped_before_elevation = (
        stats["elements_seen"] - stats["elements_sent_to_elevation"]
    )
    logger.info(
        "API summary | overpass_id_sent=%s | overpass_geom_sent=%s | elevation_sent=%s | elevation_points=%s",
        stats["overpass_id_requests_sent"],
        stats["overpass_requests_sent"],
        stats["elevation_requests_sent"],
        stats["elevation_points_requested"],
    )
    logger.info(
        "Timing summary | overpass_id_s=%.2f | overpass_geom_s=%.2f | elevation_s=%.2f",
        stats["overpass_id_time_s"],
        stats["overpass_geom_time_s"],
        stats["elevation_time_s"],
    )
    logger.info(
        "Prefetch summary | run_ids_seen=%s | lift_ids_seen=%s | new_run_ids=%s | new_lift_ids=%s | resorts_with_new_ids=%s | resorts_skipped_geometry=%s",
        stats["preflight_run_ids_seen"],
        stats["preflight_lift_ids_seen"],
        stats["preflight_new_run_ids"],
        stats["preflight_new_lift_ids"],
        stats["resorts_with_new_ids"],
        stats["resorts_skipped_geometry"],
    )
    logger.info(
        "Element summary | seen=%s | sent_to_elevation=%s | skipped_before_elevation=%s",
        stats["elements_seen"],
        stats["elements_sent_to_elevation"],
        skipped_before_elevation,
    )
    logger.info(
        "Skip breakdown | known_run=%s | known_lift=%s | unknown_type=%s | insufficient_coords=%s | filtered_lift_type=%s | short_lift=%s",
        stats["skipped_known_run"],
        stats["skipped_known_lift"],
        stats["skipped_unknown_type"],
        stats["skipped_insufficient_coords"],
        stats["skipped_filtered_lift_type"],
        stats["skipped_short_lift"],
    )

    @dlt.resource(write_disposition="merge", table_name="ski_runs", primary_key=["osm_id"])
    def ski_runs():
        for run in ski_runs_data:
            tags = run.get("tags", {})
            
            # Use pre-calculated values
            yield {
                "osm_id": run["osm_id"],
                "resort": run["resort"],
                "country_code": run["country_code"],
                "region": run["region"],
                "run_name": tags.get("name", ""),
                "difficulty": tags.get("piste:difficulty"),
                "piste_type": tags.get("piste:type"),
                "run_length_m": run.get("length_m", 0),
                "n_points": len(run.get("coords", [])),
                "turniness_score": run.get("turniness_score", 0),
                "top_lat": run["top_lat"],
                "top_lon": run["top_lon"],
                "top_elevation_m": run["top_elev"],
                "bottom_lat": run["bottom_lat"],
                "bottom_lon": run["bottom_lon"],
                "bottom_elevation_m": run["bottom_elev"]
            }

    @dlt.resource(write_disposition="merge", table_name="ski_run_points", primary_key=["osm_id", "point_index"])
    def ski_run_points():
        for run in ski_runs_data:
            tags = run.get("tags", {})
            coords = run.get("coords", [])
            node_ids = run.get("node_ids", [])
            
            # Use pre-calculated values
            elevations = run.get("elevations", [])
            elevations_smooth = run.get("elevations_smooth", [])
            gradients_smooth = run.get("gradients_smooth", [])
            cum_distances = run.get("cum_distances", [])
            
            if len(coords) < 2:
                logger.warning(f"Skipping run {tags.get('name', '')} ({run['osm_id']}): only {len(coords)} points left.")
                continue

            # Zip node_ids and coords to ensure correct mapping
            for idx, ((lat, lon), dist, elev, elev_sm, grad_sm) in enumerate(
                zip(coords, cum_distances, elevations, elevations_smooth, gradients_smooth)
            ):
                node_id = node_ids[idx] if idx < len(node_ids) else None
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
                    "gradient_smoothed": float(grad_sm) if grad_sm is not None else None,
                    "node_id": str(node_id) if node_id is not None else None
                }

    @dlt.resource(write_disposition="merge", table_name="ski_lifts", primary_key=["osm_id"])
    def ski_lifts_resource():
        for lift in ski_lifts:
            lift_length = lift.get("length_m", 0)
            if lift_length < MIN_LIFT_LENGTH_M:
                # logger.info(f"Skipping {lift['name']} - length < {MIN_LIFT_LENGTH_M}m")
                continue

            lift_type = lift.get("lift_type")
            average_speed = AVERAGE_LIFT_SPEEDS.get(lift_type)

            if lift_length and average_speed:
                duration_sec = lift_length / average_speed
            else:
                duration_sec = None

            lift_speed = lift_length / duration_sec if lift_length and duration_sec and duration_sec > 0 else None

            # Use pre-calculated values
            yield {
                "osm_id": lift["osm_id"],
                "resort": lift["resort"],
                "country_code": lift["country_code"],
                "region": lift["region"],
                "lift_type": lift_type,
                "name": lift.get("name"),
                "duration": duration_sec, 
                "lift_length_m": lift_length,
                "lift_speed_mps": lift_speed,
                "top_lat": lift["top_lat"],
                "top_lon": lift["top_lon"],
                "top_elevation_m": lift["top_elev"],
                "bottom_lat": lift["bottom_lat"],
                "bottom_lon": lift["bottom_lon"],
                "bottom_elevation_m": lift["bottom_elev"]
            }

    @dlt.resource(write_disposition="merge", table_name="ski_run_segments", primary_key=["run_osm_id", "segment_index"])
    def ski_run_segments():
        for run in ski_runs_data:
            coords = run.get("coords", [])
            node_ids = run.get("node_ids", [])
            tags = run.get("tags", {})
            elevations = run.get("elevations", [])  # Use pre-calculated elevations
            
            for idx in range(len(coords) - 1):
                from_lat, from_lon = coords[idx]
                to_lat, to_lon = coords[idx + 1]
                from_node_id = node_ids[idx] if idx < len(node_ids) else None
                to_node_id = node_ids[idx + 1] if idx + 1 < len(node_ids) else None
                length_m = geodesic((from_lat, from_lon), (to_lat, to_lon)).meters
                from_elev = elevations[idx] if idx < len(elevations) else None
                to_elev = elevations[idx + 1] if idx + 1 < len(elevations) else None
                if from_elev is not None and to_elev is not None and length_m > 0:
                    gradient = (to_elev - from_elev) / length_m
                else:
                    gradient = None
                yield {
                    "run_osm_id": run["osm_id"],
                    "segment_index": idx,
                    "from_node_id": str(from_node_id) if from_node_id is not None else None,
                    "to_node_id": str(to_node_id) if to_node_id is not None else None,
                    "from_lat": from_lat,
                    "from_lon": from_lon,
                    "to_lat": to_lat,
                    "to_lon": to_lon,
                    "length_m": length_m,
                    "gradient": float(gradient) if gradient is not None else None,
                    "resort": run["resort"],
                    "run_name": tags.get("name", ""),
                    "difficulty": tags.get("piste:difficulty"),
                }

    return [ski_runs, ski_run_points, ski_lifts_resource, ski_run_segments]


def _load_known_osm_ids(pipeline, table_name):
    sql = f"SELECT osm_id FROM {DESTINATION_DATABASE}.ski_runs.{table_name}"
    with pipeline.sql_client() as client:
        result = client.execute_sql(sql)
    if result is None:
        return set()
    return {row[0] for row in result if row and row[0] is not None}

def run_pipeline(logger):
    logger.info("Starting DLT pipeline...")
    bootstrap_started_at = perf_counter()
    pipeline = dlt.pipeline(
        pipeline_name="ski_run_pipeline",
        destination=os.getenv("DLT_DESTINATION"),
        dataset_name="ski_runs",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )
    known_run_osm = set()
    known_lift_osm = set()

    try:
        logger.info("Trying to access table: ski_runs")
        started_at = perf_counter()
        known_run_osm = _load_known_osm_ids(pipeline, "ski_runs")
        logger.info(
            "Found %s known ski runs in %.2fs",
            len(known_run_osm),
            perf_counter() - started_at,
        )
    except (ValueError, KeyError, DatabaseUndefinedRelation) as e:
        logger.info(f"Table ski_runs not found: {e}")
    except Exception as e:
        logger.warning(f"Error accessing table ski_runs: {e}")

    try:
        logger.info("Trying to access table: ski_lifts")
        started_at = perf_counter()
        known_lift_osm = _load_known_osm_ids(pipeline, "ski_lifts")
        logger.info(
            "Found %s known ski lifts in %.2fs",
            len(known_lift_osm),
            perf_counter() - started_at,
        )
    except (ValueError, KeyError, DatabaseUndefinedRelation) as e:
        logger.info(f"Table ski_lifts not found: {e}")
    except Exception as e:
        logger.warning(f"Error accessing table ski_lifts: {e}")

    logger.info("Bootstrap summary | known_id_load_s=%.2f", perf_counter() - bootstrap_started_at)

    if not known_run_osm and not known_lift_osm:
        logger.info("No existing ski runs/lifts data found, treating as first run")

    try:
        pipeline.run(ski_source(known_run_osm, known_lift_osm))
    except Exception as e:
        logger.error(f"❌ Pipeline run failed: {e}")
        return False

if __name__ == "__main__":
    run_pipeline(logger=logger)
    logger.info("Pipeline run completed successfully.")
