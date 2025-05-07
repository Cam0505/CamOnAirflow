import dlt
import requests
import os
import logging
from dotenv import load_dotenv
from time import sleep

# Load environment variables
load_dotenv(dotenv_path="../.env")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# API credentials and URL for GeoNames
USERNAME = os.getenv("GEONAMES_USERNAME")
# print(f"Using GeoNames username: {USERNAME}")
BASE_URL = "http://api.geonames.org/citiesJSON"

def make_request_with_retries(params, max_retries=5, backoff_factor=2):
    for attempt in range(max_retries):
        try:
            # Debug: Show full request
            prepared = requests.Request("GET", BASE_URL, params=params).prepare()
            print("DEBUG URL:", prepared.url)

            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            return response.json().get("geonames", [])
        except requests.RequestException as e:
            wait_time = backoff_factor ** attempt
            logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
            sleep(wait_time)
    logging.error(f"All retries failed for params: {params}")
    return []

def fetch_cities(country_code):
    # Default values for GeoNames query
    max_rows = 100  # Max cities per request (GeoNames limit per query)
    total_fetched = 0

    logging.info(f"Starting fetch for country: {country_code}")

    # Fetch cities from GeoNames (you can adjust the latitude/longitude box as needed)
    params = {
        "formatted": "true",
        "lat": "0",  # Adjust as needed for bounding box around the country
        "lng": "0",  # Adjust as needed for bounding box around the country
        "maxRows": max_rows,
        "lang": "en",
        "username": USERNAME
    }

    # Adjust the parameters for each country (NZ and AU)
    if country_code == "AU":
        params["north"] = "-10.0"  # Adjust latitudes for Australia
        params["south"] = "-44.0"
        params["east"] = "155.0"
        params["west"] = "112.0"
    elif country_code == "NZ":
        params["north"] = "-33.0"  # Adjust latitudes for New Zealand
        params["south"] = "-47.0"
        params["east"] = "180.0"
        params["west"] = "166.0"
    elif country_code == "GB":
        params["north"] = "60.0"  # Adjust latitudes for the United Kingdom
        params["south"] = "49.0"
        params["east"] = "1.0"
        params["west"] = "-8.0"
    elif country_code == "CA":
        params["north"] = "83.0"  # Adjust latitudes for Canada
        params["south"] = "42.0"
        params["east"] = "-52.0"
        params["west"] = "-140.0"

    # Request data from GeoNames
    data = make_request_with_retries(params)

    for city in data:
        total_fetched += 1
        yield {
            "city_id": city.get("geonameId"),
            "city": city.get("name"),
            "latitude": city.get("lat"),
            "longitude": city.get("lng"),
            "country": city.get("countryName"),
            "country_code": country_code  # Add country_code to the result
        }

    logging.info(f"Total cities fetched for {country_code}: {total_fetched}")

@dlt.source
def geo_source():

    @dlt.resource(name="geo_cities", write_disposition="merge", primary_key="city_id")
    def cities():
        for country in ["AU", "NZ", "GB", "CA"]:
        # for country in ["GB", "CA"]:
            try:
                yield from fetch_cities(country)
            except Exception as e:
                logging.error(f"Error while processing country {country}: {e}")

    return cities()

if __name__ == "__main__":
    logging.info("Starting DLT pipeline...")

    pipeline = dlt.pipeline(
        pipeline_name="geo_cities_pipeline",
        destination="postgres",
        dataset_name="geo_data",
        dev_mode=False
    )

    load_info = pipeline.run(geo_source())
    logging.info(f"Pipeline finished. Load info: {load_info}")