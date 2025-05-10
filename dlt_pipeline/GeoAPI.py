import dlt
import requests
import os
import logging
from dotenv import load_dotenv
from time import sleep
import subprocess

# Load environment variables
load_dotenv(dotenv_path="../.env")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# API credentials and URL for GeoNames
USERNAME = os.getenv("GEONAMES_USERNAME")
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
    return make_request_with_retries(DETAILS_URL, params)

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

    if country_code == "AU":
        params.update({"north": "-10.0", "south": "-44.0", "east": "155.0", "west": "112.0"})
    elif country_code == "NZ":
        params.update({"north": "-33.0", "south": "-47.0", "east": "180.0", "west": "166.0"})
    elif country_code == "GB":
        params.update({"north": "60.0", "south": "49.0", "east": "1.0", "west": "-8.0"})
    elif country_code == "CA":
        params.update({"north": "83.0", "south": "42.0", "east": "-52.0", "west": "-140.0"})

    cities_data = make_request_with_retries(BASE_URL, params).get("geonames", [])

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

    logger.info(f"Total cities fetched for {country_code}: {total_fetched}")

@dlt.source
def geo_source():

    @dlt.resource(name="geo_cities", write_disposition="merge", primary_key="city_id")
    def cities():
        for country in ["AU", "NZ", "GB", "CA"]:
            try:
                yield from fetch_cities(country)
            except Exception as e:
                logger.error(f"Error while processing country {country}: {e}")

    return cities()

if __name__ == "__main__":
    logger.info("Starting DLT pipeline...")

    pipeline = dlt.pipeline(
        pipeline_name="geo_cities_pipeline",
        destination="postgres",
        dataset_name="geo_data",
        dev_mode=False
    )

    try:
        logger.info("Pipeline finished started.")
        load_info = pipeline.run(geo_source())
        logger.info(f"Pipeline finished. Load info: {load_info}")

        logger.info("Running dbt transformation for open-meteo...")
        try:
            result = subprocess.run(
                [
                    "dbt", "run",
                    "--select", "source:geo+",
                    "--profiles-dir", "/usr/local/airflow/dbt",
                    "--project-dir", "/usr/local/airflow/dbt"
                ],
                check=True,
                capture_output=True,
                text=True
            )
            logger.info("DBT run succeeded:\n" + result.stdout)
        except subprocess.CalledProcessError as e:
            logger.error("DBT run failed:\n" + e.stderr)

    except Exception as e:
            logger.exception("Unhandled exception in Open-Meteo pipeline execution.")