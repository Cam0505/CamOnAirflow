import dlt
from dlt.sources.helpers import requests
import os
import json
import logging
from dotenv import load_dotenv
# from time import sleep
# import subprocess
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



@dlt.source
def ski_runs_source(logger):

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

        response = requests.post(url, data={"data": query}, timeout=60)
        elements = response.json()["elements"]
        for run in elements:
            if "geometry" not in run or not run["geometry"]:
                continue
            yield {
                "osm_id": run["id"],
                "tags": run.get("tags", {}),
                "geometry": run["geometry"],
            }

       
    return fetch_run


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
 

    source = ski_runs_source(logger)

    try:
        load_info = pipeline.run(source)


    except Exception as e:
        logger.error(f"❌ Pipeline run failed: {e}")
        return False
        



if __name__ == "__main__":
    should_run_dbt = run_pipeline(logger=logger)
    logger.info("Pipeline and DBT run completed successfully.")