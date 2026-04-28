import dlt
from dlt.sources.helpers import requests
import os
import logging
import time
from datetime import date
from time import perf_counter
from collections import defaultdict
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
    # Chinese Ski Resorts
    {"name": "雪如意滑雪场", "country": "CN", "region": "Hebei"},
    {"name": "太舞滑雪场", "country": "CN", "region": "Hebei"},
    {"name": "Yunding Resort Secret Garden", "country": "CN", "region": "Hebei"},
    {"name": "Wanlong Paradise Resort", "country": "CN", "region": "Hebei"},
    {"name": "富龙滑雪场", "country": "CN", "region": "Hebei"},
    {"name": "National Alpine Skiing Centre", "country": "CN", "region": "Hebei"},
    {"name": "禾木（吉克普林）国际滑雪度假区", "country": "CN", "region": "Xinjiang"},
    {"name": "将军山滑雪场", "country": "CN", "region": "Xinjiang"},
    {"name": "Beidahu Ski Resort", "country": "CN", "region": "Jilin"},
    {"name": "Songhua Lake Ski Resort", "country": "CN", "region": "Jilin"},

    # Japanese Ski Resorts
    {"name": "Kiroro Resort", "country": "JP", "region": "Hokkaido"},
    {"name": "Rusutsu Resort Ski Area", "country": "JP", "region": "Hokkaido"},
    {"name": "Mount Racey", "country": "JP", "region": "Hokkaido"},
    {"name": "Niseko United", "country": "JP", "region": "Hokkaido"},
    {"name": "Furano Ski Resort", "country": "JP", "region": "Hokkaido"},
    {"name": "Tomamu Ski Resort", "country": "JP", "region": "Hokkaido"},
    {"name": "Hakodate Nanae Snowpark", "country": "JP", "region": "Hokkaido"},
    {"name": "Sahoro", "country": "JP", "region": "Hokkaido"},
    {"name": "Appi Kogen Ski Resort", "country": "JP", "region": "Honshu"},
    {"name": "Shizukuishi Ski Resort", "country": "JP", "region": "Honshu"},
    {"name": "Takasu Snow Park", "country": "JP", "region": "Honshu"},
    {"name": "Washigatake Ski Area", "country": "JP", "region": "Honshu"},
    {"name": "WINGHILLS Shiratori Resort", "country": "JP", "region": "Honshu"},
    {"name": "Zao Onsen Ski Resort", "country": "JP", "region": "Honshu"},
    {"name": "Miyagi Zao Eboshi Resort", "country": "JP", "region": "Honshu"},
    {"name": "Miyagi Zao Shiroishi Ski Resort", "country": "JP", "region": "Honshu"},

    # Canadian Ski Resorts
    {"name": "Nakiska", "country": "CA", "region": "Alberta"},
    {"name": "Banff Sunshine Village", "country": "CA", "region": "Alberta"},
    {"name": "Lake Louise Ski Area", "country": "CA", "region": "Alberta"},
    {"name": "Whistler Blackcomb", "country": "CA", "region": "British Columbia"},
    {"name": "Cypress Mountain", "country": "CA", "region": "British Columbia"},
    {"name": "Grouse Mountain", "country": "CA", "region": "British Columbia"},
    {"name": "Kicking Horse Resort", "country": "CA", "region": "British Columbia"},
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
    {"name": "Thredbo Resort", "country": "AU", "region": "New South Wales"},

    # Chilean Ski Resorts
    {"name": "Corralco", "country": "CL", "region": "Chile"},
    {"name": "Estación Esquí Antillanca", "country": "CL", "region": "Chile"},
    {"name": "El Colorado", "country": "CL", "region": "Chile"}, 
    {"name": "La Parva", "country": "CL", "region": "Chile"},
    {"name": "Portillo", "country": "CL", "region": "Chile"},
    # {"name": "Centro Ski Pucón", "country": "CL", "region": "Chile"},
    # {"name": "Centro de Ski Chapa Verde", "country": "CL", "region": "Chile"},
    # {"name": "Valle Nevado", "country": "CL", "region": "Chile"},

    # Argentinian Ski Resorts
    {"name": "Cerro Perito Moreno", "country": "AR", "region": "Argentina"},
    {"name": "Cerro Catedral", "country": "AR", "region": "Argentina"},
    {"name": "Cerro Bayo", "country": "AR", "region": "Argentina"},
    # {"name": "Chapelco Ski Resort", "country": "AR", "region": "Argentina"},
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
BASE_URL = "https://overpass-api.de/api/interpreter"
DESTINATION_DATABASE = "camonairflow"
MIN_LIFT_LENGTH_M = 100
PISTE_TYPE_REGEX = "^(downhill|nordic|skitour|snow_park|sled|connection)$"
IGNORED_AERIALWAY_REGEX = "^(station|goods)$"
SMOOTHING_GRADE_THRESHOLD = math.tan(math.radians(50)) 
MANUAL_QUARANTINED_OSM_IDS_BY_RESORT = {
    "Wanlong Paradise Resort": {
        "run_ids": [878706034, 1349252976],
        "lift_ids": [],
    },
}


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



def _escape_overpass_string(value):
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _format_count_dict(counts):
    if not counts:
        return {}
    return {
        str(key): counts[key]
        for key in sorted(counts, key=lambda value: str(value))
    }


def _get_quarantined_ids(quarantine_by_resort, resort_name):
    manual_entry = MANUAL_QUARANTINED_OSM_IDS_BY_RESORT.get(resort_name, {})
    state_entry = quarantine_by_resort.get(resort_name, {})
    run_ids = set(manual_entry.get("run_ids", [])) | set(state_entry.get("run_ids", []))
    lift_ids = set(manual_entry.get("lift_ids", [])) | set(state_entry.get("lift_ids", []))
    return run_ids, lift_ids


def _quarantine_resort_ids(quarantine_by_resort, resort_name, run_ids=None, lift_ids=None):
    entry = quarantine_by_resort.setdefault(resort_name, {"run_ids": [], "lift_ids": []})
    entry["run_ids"] = sorted(set(entry.get("run_ids", [])) | set(run_ids or []))
    entry["lift_ids"] = sorted(set(entry.get("lift_ids", [])) | set(lift_ids or []))
    return entry


def _build_resort_match_clauses(field):
    area_seed_clauses = []
    relation_seed_clauses = []
    escaped_name = _escape_overpass_string(field["name"])
    area_seed_clauses.extend([
        f'  area["landuse"="winter_sports"]["name"="{escaped_name}"];',
        f'  area["landuse"="winter_sports"]["name:en"="{escaped_name}"];',
        f'  way["landuse"="winter_sports"]["name"="{escaped_name}"];',
        f'  way["landuse"="winter_sports"]["name:en"="{escaped_name}"];',
        f'  rel["landuse"="winter_sports"]["name"="{escaped_name}"];',
        f'  rel["landuse"="winter_sports"]["name:en"="{escaped_name}"];',
    ])
    relation_seed_clauses.extend([
        f'  rel["site"="piste"]["name"="{escaped_name}"];',
        f'  rel["site"="piste"]["name:en"="{escaped_name}"];',
    ])
    return area_seed_clauses, relation_seed_clauses


def _build_resort_preflight_query(field, area_seed_name, relation_seed_name):
    area_seed_clauses, relation_seed_clauses = _build_resort_match_clauses(field)
    escaped_resort_name = _escape_overpass_string(field["name"])
    area_seed_block = "\n".join(area_seed_clauses) or "  // no area-capable selectors configured"
    relation_seed_block = "\n".join(relation_seed_clauses) or "  // no relation selectors configured"
    return f'''
(
{area_seed_block}
)->.{area_seed_name};
.{area_seed_name} map_to_area -> .{area_seed_name}_areas;
(
  way["piste:type"~"{PISTE_TYPE_REGEX}"](area.{area_seed_name}_areas);
  way["aerialway"]
    ["aerialway"!~"{IGNORED_AERIALWAY_REGEX}"]
    (area.{area_seed_name}_areas)
    (if: length() >= {MIN_LIFT_LENGTH_M});
);
convert item
  ::id = id(),
  resort = "{escaped_resort_name}",
  element_kind = is_tag("piste:type") ? "run" : "lift";
out tags;

(
{relation_seed_block}
)->.{relation_seed_name};
(
  way(r.{relation_seed_name})["piste:type"~"{PISTE_TYPE_REGEX}"];
  way(r.{relation_seed_name})["aerialway"]
    ["aerialway"!~"{IGNORED_AERIALWAY_REGEX}"]
    (if: length() >= {MIN_LIFT_LENGTH_M});
);
convert item
  ::id = id(),
  resort = "{escaped_resort_name}",
  element_kind = is_tag("piste:type") ? "run" : "lift";
out tags;
'''


def _build_resort_geometry_query(field):
    area_seed_clauses, relation_seed_clauses = _build_resort_match_clauses(field)
    area_seed_block = "\n".join(area_seed_clauses) or "  // no area-capable selectors configured"
    relation_seed_block = "\n".join(relation_seed_clauses) or "  // no relation selectors configured"
    return f'''
    [out:json][timeout:60];
    (
{area_seed_block}
    )->.resort_seed;
    .resort_seed map_to_area -> .resort_areas;
    (
      way["piste:type"~"{PISTE_TYPE_REGEX}"](area.resort_areas);
      way["aerialway"](area.resort_areas);
    )->.area_results;

    (
{relation_seed_block}
    )->.resort_relations;
    (
      way(r.resort_relations)["piste:type"~"{PISTE_TYPE_REGEX}"];
      way(r.resort_relations)["aerialway"];
    )->.relation_results;

    (
      .area_results;
      .relation_results;
    );
    out body geom;
    '''



def _fetch_all_resorts_candidate_ids(stats=None):
    query_parts = ["[out:json][timeout:120];"]
    for idx, field in enumerate(SKI_FIELDS):
        query_parts.append(
            _build_resort_preflight_query(
                field,
                area_seed_name=f"resort_seed_{idx}",
                relation_seed_name=f"resort_relation_{idx}",
            )
        )
    query = "\n".join(query_parts)
    if stats is not None:
        stats["overpass_id_requests_sent"] += 1

    candidate_ids_by_resort = defaultdict(
        lambda: {"run_ids": set(), "lift_ids": set()}
    )
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
        resort_name = tags.get("resort")
        element_kind = tags.get("element_kind")
        if element_id is None:
            continue
        if not resort_name or not element_kind:
            continue
        if element_kind == "run":
            candidate_ids_by_resort[resort_name]["run_ids"].add(element_id)
        elif element_kind == "lift":
            candidate_ids_by_resort[resort_name]["lift_ids"].add(element_id)

    run_ids = set()
    lift_ids = set()
    for candidate_ids in candidate_ids_by_resort.values():
        run_ids.update(candidate_ids["run_ids"])
        lift_ids.update(candidate_ids["lift_ids"])

    logger.info(
        "Global ID preflight result: candidate_run_ids=%s | candidate_lift_ids=%s",
        len(run_ids),
        len(lift_ids),
    )

    return candidate_ids_by_resort


def _fetch_resort_elements(field, stats=None):
    query = _build_resort_geometry_query(field)
    if stats is not None:
        stats["overpass_requests_sent"] += 1
    return _post_overpass_query(
        query,
        field["name"],
        stats=stats,
        time_key="overpass_geom_time_s",
    )


# If a single-point Google resolution probe returns worse than this, fall back to OTD srtm30m.
RESOLUTION_OTD_THRESHOLD_M = 30.0


def _probe_google_resolution(coord, stats=None):
    """Single-point Google Elevation query to read the resolution at a representative location.
    Returns resolution in metres, or None on failure. Use a single point for accurate resolution
    (batching multiple points degrades Google's reported resolution value)."""
    lat, lon = coord
    try:
        if stats is not None:
            stats["elevation_requests_sent"] += 1
            stats["elevation_points_requested"] += 1
        started_at = perf_counter()
        r = requests.get(
            "https://maps.googleapis.com/maps/api/elevation/json",
            params={"locations": f"{lat},{lon}", "key": GOOGLE_API_KEY},
            timeout=10,
        )
        if stats is not None:
            stats["elevation_time_s"] += perf_counter() - started_at
        r.raise_for_status()
        results = r.json().get("results", [])
        if results:
            return results[0].get("resolution")
    except Exception as e:
        logger.warning(f"Google resolution probe failed: {e}")
    return None


def _get_elevations_otd(coords_list, dataset, stats=None):
    """Fetch elevations from OpenTopoData. Free tier: 1000 req/day, 100 pts/batch, 1 req/s.
    Returns (elevations, resolutions) where resolutions are all None (OTD doesn't expose resolution)."""
    if not coords_list:
        return [], []
    BATCH_SIZE = 100
    all_elevations = []
    for i in range(0, len(coords_list), BATCH_SIZE):
        batch = coords_list[i:i + BATCH_SIZE]
        locations = "|".join(f"{lat},{lon}" for lat, lon in batch)
        try:
            if stats is not None:
                stats["otd_requests_sent"] += 1
                stats["otd_points_requested"] += len(batch)
            started_at = perf_counter()
            r = requests.get(
                f"https://api.opentopodata.org/v1/{dataset}",
                params={"locations": locations},
                timeout=15,
            )
            if stats is not None:
                stats["elevation_time_s"] += perf_counter() - started_at
            r.raise_for_status()
            results = r.json().get("results", [])
            elevations = [res.get("elevation") for res in results]
            while len(elevations) < len(batch):
                elevations.append(None)
        except Exception as e:
            logger.error(f"Error fetching OTD elevations: {e}")
            elevations = [None] * len(batch)
        all_elevations.extend(elevations)
        if i + BATCH_SIZE < len(coords_list):
            time.sleep(1)  # OTD public API: 1 req/s
    return all_elevations, [None] * len(all_elevations)


def get_elevations_batch(coords_list, stats=None, use_otd=False):
    """Fetch elevations. If use_otd=True, routes to OpenTopoData srtm30m (30m free DEM).
    use_otd is set per-resort based on a resolution probe against Google Elevation."""
    if use_otd:
        return _get_elevations_otd(coords_list, "srtm30m", stats)
    if not coords_list:
        return [], []
    BATCH_SIZE = 200
    all_elevations = []
    all_resolutions = []
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
            batch_results = r.json().get("results", [])
            elevations = [res.get("elevation") for res in batch_results]
            resolutions = [res.get("resolution") for res in batch_results]
            while len(elevations) < len(batch):
                elevations.append(None)
                resolutions.append(None)
        except Exception as e:
            logger.error(f"Error fetching elevations: {e}")
            elevations = [None] * len(batch)
            resolutions = [None] * len(batch)
        all_elevations.extend(elevations)
        all_resolutions.extend(resolutions)
    return all_elevations, all_resolutions

def smooth_steep_gradients(elevs, dists, threshold=SMOOTHING_GRADE_THRESHOLD, window=7):
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
def ski_source(known_run_osm_by_resort: dict, known_lift_osm_by_resort: dict):
    ski_runs_data = []
    ski_lifts = []
    known_run_osm = set().union(*known_run_osm_by_resort.values()) if known_run_osm_by_resort else set()
    known_lift_osm = set().union(*known_lift_osm_by_resort.values()) if known_lift_osm_by_resort else set()
    should_preflight_ids = bool(known_run_osm or known_lift_osm)
    resorts_to_fetch = {field["name"] for field in SKI_FIELDS}
    stats = {
        "overpass_requests_sent": 0,
        "overpass_id_requests_sent": 0,
        "overpass_id_time_s": 0.0,
        "overpass_geom_time_s": 0.0,
        "elevation_requests_sent": 0,
        "elevation_points_requested": 0,
        "otd_requests_sent": 0,
        "otd_points_requested": 0,
        "elevation_time_s": 0.0,
        "elements_seen": 0,
        "elements_sent_to_elevation": 0,
        "preflight_run_ids_seen": 0,
        "preflight_lift_ids_seen": 0,
        "preflight_new_run_ids": 0,
        "preflight_new_lift_ids": 0,
        "preflight_quarantined_run_ids": 0,
        "preflight_quarantined_lift_ids": 0,
        "resorts_skipped_geometry": 0,
        "resorts_with_new_ids": 0,
        "new_ids_by_resort": {},
        "accepted_run_count": 0,
        "accepted_lift_count": 0,
        "accepted_run_by_difficulty": defaultdict(int),
        "accepted_run_by_piste_type": defaultdict(int),
        "accepted_lift_by_type": defaultdict(int),
        "accepted_by_resort": defaultdict(lambda: {"runs": 0, "lifts": 0}),
        "skipped_known_run": 0,
        "skipped_known_lift": 0,
        "skipped_unknown_type": 0,
        "skipped_insufficient_coords": 0,
        "skipped_filtered_lift_type": 0,
        "skipped_short_lift": 0,
    }
    quarantine_state = dlt.current.source_state().setdefault("osm_id_quarantine", {"by_resort": {}})
    quarantine_by_resort = quarantine_state.setdefault("by_resort", {})

    today_str = str(date.today())
    api_state = dlt.current.source_state().setdefault("api_requests", {})
    _api_keys = (
        "overpass_id_sent",
        "overpass_geom_sent",
        "google_elevation_sent",
        "google_elevation_points",
        "otd_sent",
        "otd_points",
    )
    for _key in _api_keys:
        api_state.setdefault(_key, {})
    # Prune to only today — carry forward today's tally, start fresh each new day
    for _key in _api_keys:
        api_state[_key] = {today_str: api_state[_key].get(today_str, 0)}
    if should_preflight_ids:
        try:
            logger.info("Checking OSM ids across all configured resorts before geometry fetch ...")
            candidate_ids_by_resort = _fetch_all_resorts_candidate_ids(stats=stats)
            run_ids = set()
            lift_ids = set()
            new_run_ids = set()
            new_lift_ids = set()
            resorts_to_fetch = set()

            for field in SKI_FIELDS:
                resort_name = field["name"]
                resort_candidates = candidate_ids_by_resort.get(
                    resort_name,
                    {"run_ids": set(), "lift_ids": set()},
                )
                quarantined_run_ids, quarantined_lift_ids = _get_quarantined_ids(
                    quarantine_by_resort,
                    resort_name,
                )
                resort_run_ids = resort_candidates["run_ids"] - quarantined_run_ids
                resort_lift_ids = resort_candidates["lift_ids"] - quarantined_lift_ids
                run_ids.update(resort_run_ids)
                lift_ids.update(resort_lift_ids)
                stats["preflight_quarantined_run_ids"] += len(resort_candidates["run_ids"] & quarantined_run_ids)
                stats["preflight_quarantined_lift_ids"] += len(resort_candidates["lift_ids"] & quarantined_lift_ids)

                resort_new_run_ids = resort_run_ids - known_run_osm_by_resort.get(resort_name, set())
                resort_new_lift_ids = resort_lift_ids - known_lift_osm_by_resort.get(resort_name, set())
                if resort_new_run_ids or resort_new_lift_ids:
                    stats["new_ids_by_resort"][resort_name] = {
                        "run_ids": len(resort_new_run_ids),
                        "lift_ids": len(resort_new_lift_ids),
                        "run_id_values": sorted(resort_new_run_ids),
                        "lift_id_values": sorted(resort_new_lift_ids),
                    }
                    resorts_to_fetch.add(resort_name)
                    new_run_ids.update(resort_new_run_ids)
                    new_lift_ids.update(resort_new_lift_ids)

            stats["preflight_run_ids_seen"] = len(run_ids)
            stats["preflight_lift_ids_seen"] = len(lift_ids)
            stats["preflight_new_run_ids"] = len(new_run_ids)
            stats["preflight_new_lift_ids"] = len(new_lift_ids)

            if not new_run_ids and not new_lift_ids:
                stats["resorts_skipped_geometry"] = len(SKI_FIELDS)
                stats["resorts_with_new_ids"] = 0
                resorts_to_fetch = set()
                logger.info("No new OSM ids across configured resorts, skipping all geometry fetches")
            else:
                logger.info(
                    "New OSM ids across configured resorts | runs=%s | lifts=%s",
                    len(new_run_ids),
                    len(new_lift_ids),
                )
                stats["resorts_with_new_ids"] = len(resorts_to_fetch)
                stats["resorts_skipped_geometry"] = len(SKI_FIELDS) - len(resorts_to_fetch)
                logger.info(
                    "New IDs found for %s resorts, fetching geometry only for those resorts",
                    len(resorts_to_fetch),
                )
                for resort_name, resort_counts in sorted(stats["new_ids_by_resort"].items()):
                    logger.info(
                        "Resort new-id summary | resort=%s | new_run_ids=%s | new_lift_ids=%s",
                        resort_name,
                        resort_counts["run_ids"],
                        resort_counts["lift_ids"],
                    )
                    # logger.info(
                    #     "Resort new-id details | resort=%s | run_ids=%s | lift_ids=%s",
                    #     resort_name,
                    #     resort_counts.get("run_id_values", []),
                    #     resort_counts.get("lift_id_values", []),
                    # )
        except Exception as e:
            logger.warning(
                f"Global ID preflight failed, falling back to geometry fetch: {e}"
            )
            should_preflight_ids = False
            resorts_to_fetch = {field["name"] for field in SKI_FIELDS}

    for field in SKI_FIELDS:
        if should_preflight_ids and field["name"] not in resorts_to_fetch:
            continue 

        logger.info(f"Fetching ski runs for {field['name']} ...")
        try:
            elements = _fetch_resort_elements(field, stats=stats)
        except Exception as e:
            resort_new_ids = stats["new_ids_by_resort"].get(field["name"], {})
            if resort_new_ids:
                quarantine_entry = _quarantine_resort_ids(
                    quarantine_by_resort,
                    field["name"],
                    run_ids=resort_new_ids.get("run_id_values", []),
                    lift_ids=resort_new_ids.get("lift_id_values", []),
                )
                logger.warning(
                    "Added resort IDs to quarantine after geometry fetch failure | resort=%s | run_ids=%s | lift_ids=%s",
                    field["name"],
                    quarantine_entry.get("run_ids", []),
                    quarantine_entry.get("lift_ids", []),
                )
            logger.error(str(e))
            continue

        # Probe Google resolution with one representative point from this resort.
        # Single-point queries return accurate resolution values (batching degrades them).
        # If Google is coarser than the threshold, use OTD srtm30m for the whole resort.
        use_otd = False
        probe_coord = next(
            (
                (pt["lat"], pt["lon"])
                for elem in elements
                for pt in elem.get("geometry", [])
            ),
            None,
        )
        if probe_coord is not None:
            google_res = _probe_google_resolution(probe_coord, stats=stats)
            if google_res is not None:
                if google_res > RESOLUTION_OTD_THRESHOLD_M:
                    use_otd = True
                    logger.info(
                        "Resolution probe | resort=%s | google_res=%.1fm > %.0fm threshold → OTD srtm30m",
                        field["name"], google_res, RESOLUTION_OTD_THRESHOLD_M,
                    )
                else:
                    logger.info(
                        "Resolution probe | resort=%s | google_res=%.1fm ≤ %.0fm threshold → Google",
                        field["name"], google_res, RESOLUTION_OTD_THRESHOLD_M,
                    )
            else:
                logger.warning(
                    "Resolution probe failed for %s, defaulting to Google", field["name"]
                )

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
            elevations, point_resolutions = get_elevations_batch(coords, stats=stats, use_otd=use_otd)
            # OTD doesn't return per-point resolution; fill with the known srtm30m grid resolution.
            if use_otd:
                point_resolutions = [30.0] * len(coords)

            if is_run:
                # Calculate cumulative distances once
                cum_distances = [0.0]
                for i in range(1, len(coords)):
                    dist = geodesic(coords[i - 1], coords[i]).meters
                    cum_distances.append(cum_distances[-1] + dist)

                # Smooth elevations if needed
                if len(elevations) >= 3:
                    elevations_smooth, gradients_smooth = smooth_steep_gradients(
                        elevations, cum_distances, threshold=SMOOTHING_GRADE_THRESHOLD, window=7
                    )
                else:
                    elevations_smooth = elevations
                    gradients_smooth = [0.0] * len(elevations)

                # Calculate top/bottom coordinates ONCE
                top_lat, top_lon, top_elev, bottom_lat, bottom_lon, bottom_elev = get_top_bottom_coordinates(coords, elevations)
                
                # Calculate turniness once
                turniness_score = compute_turniness(coords)
                run_difficulty = tags.get("piste:difficulty") or "unknown"
                run_piste_type = tags.get("piste:type") or "unknown"
                stats["accepted_run_count"] += 1
                stats["accepted_run_by_difficulty"][run_difficulty] += 1
                stats["accepted_run_by_piste_type"][run_piste_type] += 1
                stats["accepted_by_resort"][field["name"]]["runs"] += 1

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
                    "point_resolutions": point_resolutions,  # resolution per point (metres)
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
                stats["accepted_lift_count"] += 1
                stats["accepted_lift_by_type"][aerialway_type or "unknown"] += 1
                stats["accepted_by_resort"][field["name"]]["lifts"] += 1

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
        "API summary | overpass_id_sent=%s | overpass_geom_sent=%s | google_elevation_sent=%s | google_elevation_points=%s | otd_sent=%s | otd_points=%s",
        stats["overpass_id_requests_sent"],
        stats["overpass_requests_sent"],
        stats["elevation_requests_sent"],
        stats["elevation_points_requested"],
        stats["otd_requests_sent"],
        stats["otd_points_requested"],
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
        "Quarantine summary | blocked_run_ids=%s | blocked_lift_ids=%s | quarantined_resorts=%s",
        stats["preflight_quarantined_run_ids"],
        stats["preflight_quarantined_lift_ids"],
        len(quarantine_by_resort),
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
    logger.info(
        "Accepted summary | runs=%s | lifts=%s",
        stats["accepted_run_count"],
        stats["accepted_lift_count"],
    )
    logger.info(
        "Accepted run categories | difficulty=%s | piste_type=%s",
        _format_count_dict(stats["accepted_run_by_difficulty"]),
        _format_count_dict(stats["accepted_run_by_piste_type"]),
    )
    logger.info(
        "Accepted lift categories | lift_type=%s",
        _format_count_dict(stats["accepted_lift_by_type"]),
    )
    for resort_name in sorted(stats["accepted_by_resort"]):
        resort_counts = stats["accepted_by_resort"][resort_name]
        logger.info(
            "Accepted per resort | resort=%s | runs=%s | lifts=%s",
            resort_name,
            resort_counts["runs"],
            resort_counts["lifts"],
        )

    # Persist daily API request counts to pipeline state
    api_state["overpass_id_sent"][today_str] = api_state["overpass_id_sent"].get(today_str, 0) + stats["overpass_id_requests_sent"]
    api_state["overpass_geom_sent"][today_str] = api_state["overpass_geom_sent"].get(today_str, 0) + stats["overpass_requests_sent"]
    api_state["google_elevation_sent"][today_str] = api_state["google_elevation_sent"].get(today_str, 0) + stats["elevation_requests_sent"]
    api_state["google_elevation_points"][today_str] = api_state["google_elevation_points"].get(today_str, 0) + stats["elevation_points_requested"]
    api_state["otd_sent"][today_str] = api_state["otd_sent"].get(today_str, 0) + stats["otd_requests_sent"]
    api_state["otd_points"][today_str] = api_state["otd_points"].get(today_str, 0) + stats["otd_points_requested"]
    logger.info(
        "Daily API totals (state) | date=%s | overpass_id_sent=%s | overpass_geom_sent=%s | google_elevation_sent=%s | google_elevation_points=%s | otd_sent=%s | otd_points=%s",
        today_str,
        api_state["overpass_id_sent"][today_str],
        api_state["overpass_geom_sent"][today_str],
        api_state["google_elevation_sent"][today_str],
        api_state["google_elevation_points"][today_str],
        api_state["otd_sent"][today_str],
        api_state["otd_points"][today_str],
    )

    @dlt.resource(write_disposition="merge", table_name="ski_runs", primary_key=["osm_id"])
    def ski_runs():
        for run in ski_runs_data:
            tags = run.get("tags", {})
            
            # Use pre-calculated values
            point_resolutions = run.get("point_resolutions", [])
            valid_resolutions = [r for r in point_resolutions if r is not None]
            avg_point_resolution_m = sum(valid_resolutions) / len(valid_resolutions) if valid_resolutions else None

            yield {
                "osm_id": run["osm_id"],
                "resort": run["resort"],
                "country_code": run["country_code"],
                "region": run["region"],
                "run_name": tags.get("name", ""),
                "area": tags.get("area", ""),
                "difficulty": tags.get("piste:difficulty"),
                "piste_type": tags.get("piste:type"),
                "run_length_m": run.get("length_m", 0),
                "n_points": len(run.get("coords", [])),
                "avg_point_resolution_m": avg_point_resolution_m,
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
            point_resolutions = run.get("point_resolutions", [])
            
            if len(coords) < 2:
                logger.warning(f"Skipping run {tags.get('name', '')} ({run['osm_id']}): only {len(coords)} points left.")
                continue

            # Zip node_ids and coords to ensure correct mapping
            for idx, ((lat, lon), dist, elev, elev_sm, grad_sm) in enumerate(
                zip(coords, cum_distances, elevations, elevations_smooth, gradients_smooth)
            ):
                node_id = node_ids[idx] if idx < len(node_ids) else None
                resolution = point_resolutions[idx] if idx < len(point_resolutions) else None
                yield {
                    "osm_id": run["osm_id"],
                    "resort": run["resort"],
                    "country_code": run["country_code"],
                    "run_name": tags.get("name", ""),
                    "area": tags.get("area", ""),
                    "point_index": idx,
                    "lat": lat,
                    "lon": lon,
                    "distance_along_run_m": dist,
                    "elevation_m": elev,
                    "elevation_smoothed_m": float(elev_sm) if elev_sm is not None else None,
                    "gradient_smoothed": float(grad_sm) if grad_sm is not None else None,
                    "node_id": str(node_id) if node_id is not None else None,
                    "resolution_m": float(resolution) if resolution is not None else None,
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
                    "area": tags.get("area", ""),
                }

    return [ski_runs, ski_run_points, ski_lifts_resource, ski_run_segments]


def _load_known_osm_ids_by_resort(pipeline, table_name):
    sql = f"SELECT resort, osm_id FROM {DESTINATION_DATABASE}.ski_runs.{table_name}"
    with pipeline.sql_client() as client:
        result = client.execute_sql(sql)
    if result is None:
        return {}
    known_ids_by_resort = defaultdict(set)
    for row in result:
        if not row or len(row) < 2:
            continue
        resort_name, osm_id = row[0], row[1]
        if resort_name is None or osm_id is None:
            continue
        known_ids_by_resort[resort_name].add(osm_id)
    return dict(known_ids_by_resort)

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
    known_run_osm_by_resort = {}
    known_lift_osm_by_resort = {}

    try:
        logger.info("Trying to access table: ski_runs")
        started_at = perf_counter()
        known_run_osm_by_resort = _load_known_osm_ids_by_resort(pipeline, "ski_runs")
        known_run_count = sum(len(osm_ids) for osm_ids in known_run_osm_by_resort.values())
        logger.info(
            "Found %s known ski runs across %s resorts in %.2fs",
            known_run_count,
            len(known_run_osm_by_resort),
            perf_counter() - started_at,
        )
    except (ValueError, KeyError, DatabaseUndefinedRelation) as e:
        logger.info(f"Table ski_runs not found: {e}")
    except Exception as e:
        logger.warning(f"Error accessing table ski_runs: {e}")

    try:
        logger.info("Trying to access table: ski_lifts")
        started_at = perf_counter()
        known_lift_osm_by_resort = _load_known_osm_ids_by_resort(pipeline, "ski_lifts")
        known_lift_count = sum(len(osm_ids) for osm_ids in known_lift_osm_by_resort.values())
        logger.info(
            "Found %s known ski lifts across %s resorts in %.2fs",
            known_lift_count,
            len(known_lift_osm_by_resort),
            perf_counter() - started_at,
        )
    except (ValueError, KeyError, DatabaseUndefinedRelation) as e:
        logger.info(f"Table ski_lifts not found: {e}")
    except Exception as e:
        logger.warning(f"Error accessing table ski_lifts: {e}")

    logger.info("Bootstrap summary | known_id_load_s=%.2f", perf_counter() - bootstrap_started_at)

    if not known_run_osm_by_resort and not known_lift_osm_by_resort:
        logger.info("No existing ski runs/lifts data found, treating as first run")

    try:
        pipeline.run(ski_source(known_run_osm_by_resort, known_lift_osm_by_resort))
    except Exception as e:
        logger.error(f"❌ Pipeline run failed: {e}")
        return False

if __name__ == "__main__":
    run_pipeline(logger=logger)
    logger.info("Pipeline run completed successfully.")
