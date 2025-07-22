import dlt
from dlt.sources.helpers import requests
import os
import json
import logging
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from geopy.distance import geodesic
from typing import List, Dict

paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
# DBT_DIR = paths["DBT_DIR"]

# Load environment variables
load_dotenv(dotenv_path=ENV_FILE)


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


BASE_URL = "https://overpass-api.de/api/interpreter"


def get_elevations_batch(coords_list):
    """
    Given a list of (lat, lon) tuples, return a list of elevations in meters
    using the Open-Elevation API. Will return None for any failed lookups.
    """
    if not coords_list:
        return []

    # Open-Elevation allows up to 200 coordinates per request
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
            # Pad if response count doesn't match input count
            while len(elevations) < len(batch):
                elevations.append(None)
        except Exception as e:
            elevations = [None] * len(batch)

        results.extend(elevations)

    return results


# @dlt.source
# def ski_runs_source(logger):

@dlt.resource(name="ski_runs", write_disposition="replace")
def fetch_run():
    # state = dlt.current.source_state().setdefault("ski_runs", {
    #     "processed_records": {},
    #     "country_status": {}
    # })
    query = """
    [out:json][timeout:100];
    (
        area["name"="New Zealand"]->.nz;
        way(area.nz)["piste:type"];
        area["name"="Australia"]->.au;
        way(area.au)["piste:type"];
    );
    out geom tags;
    """

    response = requests.post(BASE_URL, data={"data": query}, timeout=60)
    elements = response.json()["elements"]
    for run in elements:
        logger.info(f"run: {run}")
        if "geometry" not in run or not run["geometry"]:
            continue
        yield {
            "osm_id": run["id"],
            "tags": run.get("tags", {}),
            "geometry": run["geometry"],
        }




@dlt.transformer(data_from=fetch_run, table_name="ski_runs_final")
def process_runs(run: Dict):
    coords = [(pt["lat"], pt["lon"]) for pt in run["geometry"]]
    if len(coords) < 2:
        return

    # Accurate length: sum segment distances
    run_length = sum(
        geodesic(coords[i], coords[i + 1]).meters
        for i in range(len(coords) - 1)
    )

    # Start/end
    start, end = coords[0], coords[-1]

    # Fetch start/end elevation (you can batch these for all runs if preferred)
    elevations = get_elevations_batch([start, end])
    elev_start, elev_end = elevations if len(elevations) == 2 else (None, None)

    # Compute metrics
    elev_diff = (elev_start - elev_end) if elev_start is not None and elev_end is not None else None
    avg_gradient = (100 * elev_diff / run_length) if elev_diff is not None and run_length else None

    tags = run.get("tags", {})
    yield {
        "osm_id": run["osm_id"],
        "resort": tags.get("ski", tags.get("name", "")),
        "run_name": tags.get("name", ""),
        "difficulty": tags.get("piste:difficulty"),
        "piste_type": tags.get("piste:type"),
        "run_length_m": run_length,
        "elev_start": elev_start,
        "elev_end": elev_end,
        "elev_diff": elev_diff,
        "avg_gradient_pct": avg_gradient,
        "coords_start": start,
        "coords_end": end,
    }


def run_pipeline(logger):
    logger.info("Starting DLT pipeline...")

    pipeline = dlt.pipeline(
        pipeline_name="ski_run_pipeline",
        destination=os.getenv("DLT_DESTINATION"),
        dataset_name="main",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )
    # row_counts_dict = {}
    # try:
    #     dataset = pipeline.dataset()["geo_cities"].df()
    #     if dataset is not None:
    #         row_counts = dataset.groupby(
    #             "country_code").size().reset_index(name="count")
    #         row_counts_dict = dict(
    #             zip(row_counts["country_code"], row_counts["count"]))
    #         logger.info(f"Grouped Row Counts:\n{row_counts}")
    # except PipelineNeverRan:
    #     logger.warning(
    #         "⚠️ No previous runs found for this pipeline. Assuming first run.")
    #     row_counts_dict = {}
    # except DatabaseUndefinedRelation:
    #     logger.warning(
    #         "⚠️ Table Doesn't Exist. Assuming truncation.")
    #     row_counts_dict = {}


    try:
        load_info = pipeline.run(process_runs())


    except Exception as e:
        logger.error(f"❌ Pipeline run failed: {e}")
        return False




if __name__ == "__main__":
    should_run_dbt = run_pipeline(logger=logger)
    logger.info("Pipeline and DBT run completed successfully.")