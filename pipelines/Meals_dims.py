import dlt
# import pandas as pd
import requests
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
from os import getenv
from dotenv import load_dotenv
import logging

load_dotenv(dotenv_path="../.env")
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Map table names to API parameters
TABLE_PARAMS = {
    "category": "c=list",
    "country": "a=list",
    "ingredients": "i=list"
}

def make_resource(table_name, param):
    @dlt.resource(name=table_name, write_disposition="replace")
    def resource_func():
        url = f"https://www.themealdb.com/api/json/v1/1/list.php?{param}"
        logging.info(f"Fetching data for {table_name} from {url}")
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            if "meals" not in data or data["meals"] is None:
                logging.warning(f"No 'meals' found in API response for {table_name}")
                return
            yield data["meals"]
            logging.info(f"Fetched and yielded data for {table_name}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {table_name}: {e}", exc_info=True)
            raise
        except ValueError as e:
            logging.error(f"JSON decode failed for {table_name}: {e}", exc_info=True)
            raise
    return resource_func

@dlt.source
def meals():
    resources = []
    for table, param in TABLE_PARAMS.items():
        try:
            resource = make_resource(table, param)
            resources.append(resource())
        except Exception as e:
            logging.error(f"Failed to create resource for {table}: {e}", exc_info=True)
    return resources

# pipeline
pipeline = dlt.pipeline(
    pipeline_name="meals_pipeline",
    destination="postgres",
    dataset_name="meals_data",
    dev_mode=False
)
# Run the pipeline
if __name__ == "__main__":
    try:
        load_info = pipeline.run(meals())
        logging.info(f"Load successful: {load_info}")
    except Exception as e:
        logging.error(f"Pipeline run failed: {e}", exc_info=True)
