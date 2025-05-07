import dlt
# import pandas as pd
import requests
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
import os
import psycopg2
import logging
from dotenv import load_dotenv

load_dotenv(dotenv_path="../.env")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

DIMENSION_CONFIG = {
    "ingredients": {
        "sql_column": "str_ingredient",
        "query_param": "i",
        "source_key": "source_ingredient",
        "resource_name": "ingredient_table"
    },
    "country": {
        "sql_column": "str_area",
        "query_param": "a",
        "source_key": "source_country",
        "resource_name": "country_table"
    },
    "category": {
        "sql_column": "str_category",
        "query_param": "c",
        "source_key": "source_category",
        "resource_name": "category_table"
    }
}

def get_dim(table_name: str, column_name: str):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("CREDENTIALS__DATABASE"),
            user=os.getenv("CREDENTIALS__USERNAME"),
            password=os.getenv("CREDENTIALS__PASSWORD"),
            host=os.getenv("CREDENTIALS__HOST"),
            port=os.getenv("CREDENTIALS__PORT")
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT {column_name} FROM meals_data.{table_name}")
        rows = cursor.fetchall()
        logging.info(f"Fetched {len(rows)} distinct values from {table_name}.{column_name}")
        return [row[0] for row in rows]
    except Exception as e:
        logging.error(f"Error fetching data from DB: {e}")
        return []
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def make_resource(dimension: str):
    config = DIMENSION_CONFIG[dimension]

    @dlt.resource(name=config["resource_name"], write_disposition="replace")
    def resource_func():
        values = get_dim(dimension, config["sql_column"])

        for value in values:
            if not value:
                continue  # skip null or empty values
            url = f"https://www.themealdb.com/api/json/v1/1/filter.php?{config['query_param']}={value}"
            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status()  # Raise exception on error
                meals = response.json().get("meals")
                if meals is None:
                    logging.warning(f"No meals found for {dimension}='{value}'")
                    continue
                for meal in meals:
                    if isinstance(meal, dict):  # Ensure it's a dictionary
                        meal[config["source_key"]] = value
                        yield meal
                    else:
                        logging.warning(f"Invalid meal format for {value}: {meal}")
            except requests.RequestException as e:
                logging.error(f"HTTP error for {value} ({url}): {e}")
            except Exception as e:
                logging.error(f"Unexpected error for {value}: {e}")

    return resource_func()

# DIMENSION_CONFIG.keys()
@dlt.source
def meals_data():
    return [make_resource(dim) for dim in DIMENSION_CONFIG.keys()]

# pipeline
pipeline = dlt.pipeline(
    pipeline_name="meals_pipeline",
    destination="postgres",
    dataset_name="meals_data",
    dev_mode=False
)
# Run the pipeline
if __name__ == "__main__":
    logging.info("Starting pipeline run...")
    try:
        load_info = pipeline.run(meals_data())
        logging.info(f"Pipeline completed successfully: {load_info}")
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
