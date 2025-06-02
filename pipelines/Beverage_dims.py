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
logging.basicConfig(level=logging.INFO)

# Map table names to API parameters
TABLE_PARAMS = {
    "beverages": "c=list",
    "glasses": "g=list",
    "ingredients": "i=list",
    "alcoholic": "a=list"
}

def make_resource(table_name, param):
    @dlt.resource(name=table_name, write_disposition="replace")
    def resource_func():
        url = f"https://www.thecocktaildb.com/api/json/v2/{getenv("BEVERAGE_API_KEY")}/list.php?{param}"
        response = requests.get(url)
        response.raise_for_status()  # Raise exception on error
        drinks = response.json()["drinks"]
        yield drinks
    return resource_func

@dlt.source
def alcoholic_cocktails():
    return [make_resource(table, param)() for table, param in TABLE_PARAMS.items()]

# pipeline
pipeline = dlt.pipeline(
    pipeline_name="cocktails_pipeline",
    destination="postgres",
    dataset_name="cocktail_data",
    dev_mode=False
)
# Run the pipeline
if __name__ == "__main__":
    try:
        load_info = pipeline.run(alcoholic_cocktails())
        logging.info(f"Load successful: {load_info}")
    except Exception as e:
        logging.error(f"Pipeline run failed: {e}", exc_info=True)
