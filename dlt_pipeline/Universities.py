import dlt
# import pandas as pd
import requests
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
from os import environ
from dotenv import load_dotenv

load_dotenv(dotenv_path="../.env")

@dlt.resource(name="universities", write_disposition="replace")
def universities_resource():
    url = "https://raw.githubusercontent.com/Hipo/university-domains-list/master/world_universities_and_domains.json"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    yield data


@dlt.source
def universities_source():
    return universities_resource()


# Define the pipeline to load into PostgreSQL
pipeline = dlt.pipeline(
    pipeline_name="universities_pipeline",
    destination="postgres",
    dataset_name="universities_data",
    dev_mode=False
)

# Run the pipeline
if __name__ == "__main__":
    load_info = pipeline.run(universities_source())
