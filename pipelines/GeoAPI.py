import dlt
from dlt.sources.helpers import requests
import os
import json
import logging
from dotenv import load_dotenv
from time import sleep
import subprocess
from project_path import get_project_paths, set_dlt_env_vars
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation


paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
DBT_DIR = paths["DBT_DIR"]

# Load environment variables
load_dotenv(dotenv_path=ENV_FILE)

bboxes = {
    "AU": {"north": "-10.0", "south": "-44.0", "east": "155.0", "west": "112.0"},
    "NZ": {"north": "-33.0", "south": "-47.0", "east": "180.0", "west": "166.0"},
    "GB": {"north": "60.0", "south": "49.0", "east": "1.0", "west": "-8.0"},
    "CA": {"north": "83.0", "south": "42.0", "east": "-52.0", "west": "-140.0"}
}


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# API credentials and URL for GeoNames
USERNAME = os.getenv("GEONAMES_USERNAME")
if not USERNAME:
    raise ValueError("Missing GEONAMES_USERNAME in environment.")

BASE_URL = "http://api.geonames.org/citiesJSON"
DETAILS_URL = "http://api.geonames.org/getJSON"

def make_request_with_retries(url, params, max_retries=5, backoff_factor=2):
    for attempt in range(max_retries):
        try:
            prepared = requests.Request("GET", url, params=params).prepare()
            print("DEBUG URL:", prepared.url)

            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            wait_time = backoff_factor ** attempt
            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
            sleep(wait_time)
    logger.error(f"All retries failed for params: {params}")
    return {}

def fetch_city_details(geoname_id):
    params = {
        "geonameId": geoname_id,
        "username": USERNAME
    }
    return requests.get(DETAILS_URL, params=params).json()


@dlt.source
def geo_source(logger, row_counts_dict: dict):

    @dlt.resource(name="geo_cities", write_disposition="merge", primary_key="city_id")
    def cities():
        state = dlt.current.source_state().setdefault("geo_cities", {
            "processed_records": {},
            "country_status": {}
        })

        def fetch_cities(country_code):
            max_rows = 100
            total_fetched = 0

            logger.info(f"Starting fetch for country: {country_code}")

            params = {
                "formatted": "true",
                "lat": "0",
                "lng": "0",
                "maxRows": max_rows,
                "lang": "en",
                "username": USERNAME
            }

            if country_code in bboxes:
                params.update(bboxes[country_code])
            try:
                # logger.info(f"Fetching cities for {country_code} with params: {params}")
                cities_data = make_request_with_retries(BASE_URL, params=params).get("geonames", [])
                # logger.info(
                #     f"Fetched {cities_data} cities for {country_code}")
            except Exception as e:
                logger.error(
                    f"Failed to fetch cities for {country_code}: {e}")
                state["country_status"][country_code] = "failed"
                raise
            database_rowcount = row_counts_dict.get(country_code, 0)

            current_count = len(cities_data)

            previous_count = state["processed_records"].get(country_code, 0)

            if database_rowcount < previous_count or database_rowcount == 0:
                logger.info(
                    f"⚠️ GeoAPI data for `{country_code}` row count dropped from {previous_count} to {database_rowcount}. Forcing reload.")
                state["country_status"][country_code] = "database_row_count"
            elif (current_count == previous_count):
                logger.info(f"\n🔁 SKIPPED LOAD:\n"
                            f"📅 Previous Run for {country_code}: {previous_count}\n"
                            f"📦 API Cities for {country_code}: {current_count}\n"
                            f"⏳ No new data for {country_code}. Skipping... \n"
                            f"{'-'*45}")
                state["country_status"][country_code] = "skipped_no_new_data"
                return

            for city in cities_data:
                total_fetched += 1
                details = fetch_city_details(city.get("geonameId")) or {}

                yield {
                    "city_id": city.get("geonameId"),
                    "city": city.get("name"),
                    "latitude": city.get("lat"),
                    "longitude": city.get("lng"),
                    "country": details.get("countryName") or city.get("countryName"),
                    "country_code": country_code,
                    "region": details.get("adminName1"),
                    "region_code": details.get("adminCode1"),
                    "continent": details.get("continentCode")
                }

            # Update the state with the number of records processed in this run
            state["processed_records"][country_code] = total_fetched
            state["country_status"][country_code] = "success"
            logger.info(
                f"Total cities fetched for {country_code}: {total_fetched}")

        try:
            for country in bboxes.keys():
                try:
                    yield from fetch_cities(country)
                except Exception as e:
                    logger.error(
                        f"Error while processing country {country}: {e}")
                    raise
            logger.info(f"Current state after successful run: {state}")
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            raise
    return cities


def run_pipeline(logger) -> bool:
    logger.info("Starting DLT pipeline...")

    pipeline = dlt.pipeline(
        pipeline_name="geo_cities_pipeline",
        destination=os.getenv("DLT_DESTINATION"),
        dataset_name="geo_data",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )
    row_counts = None
    try:
        dataset = pipeline.dataset()["geo_cities"].df()
        if dataset is not None:
            row_counts = dataset.groupby(
                "country_code").size().reset_index(name="count")
            logger.info(f"Grouped Row Counts:\n{row_counts}")
    except PipelineNeverRan:
        logger.warning(
            "⚠️ No previous runs found for this pipeline. Assuming first run.")
        row_counts = None
    except DatabaseUndefinedRelation:
        logger.warning(
            "⚠️ Table Doesn't Exist. Assuming truncation.")
        row_counts = None
 
    if row_counts is not None:
        row_counts_dict = dict(
            zip(row_counts["country_code"], row_counts["count"]))
    else:
        logger.warning(
            "⚠️ No tables found yet in dataset — assuming first run.")
        row_counts_dict = {}

    source = geo_source(logger, row_counts_dict)

    try:
        load_info = pipeline.run(source)

        outcome_data = source.state.get(
            'geo_cities', {}).get("country_status", {})

        logger.info("Country Status:\n" +
                    json.dumps(outcome_data, indent=2))

        statuses = [outcome_data.get(resource, 0) for resource in bboxes.keys()]

        if any(s == "success" for s in statuses):
            logger.info(f"Pipeline Load Info: {load_info}")
            return True
        elif all(s == "skipped_no_new_data" for s in statuses):
            logger.info("No new data to load, skipping pipeline run.")
            return False
        else:
            logger.error(
                "💥  Pipeline Failures — check Logic, API or network.")
            return False

    except Exception as e:
        logger.error(f"❌ Pipeline run failed: {e}")
        return False
        


def run_dbt(should_run=True):
    if not should_run:
        logger.info("Skipping DBT run as per configuration.")
        return
    try:
        result = subprocess.run(
            [
                "dbt", "build",
                "--select", "source:geo+"
            ],
            shell=True,
            cwd=DBT_DIR,
            capture_output=True,
            text=True,
            check=True
        )
        logger.info("DBT run succeeded:\n" + result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error("DBT run failed:\n" + e.stderr)



if __name__ == "__main__":
    should_run_dbt = run_pipeline(logger=logger)
    run_dbt(should_run=should_run_dbt)
    logger.info("Pipeline and DBT run completed successfully.")