import dlt
# import pandas as pd
import requests
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(dotenv_path="../.env")

DIMENSION_CONFIG = {
    "ingredients": {
        "sql_column": "str_ingredient1",
        "query_param": "i",
        "source_key": "source_ingredient",
        "resource_name": "ingredients_table"
    },
    "alcoholic": {
        "sql_column": "str_alcoholic",
        "query_param": "a",
        "source_key": "source_alcohol_type",
        "resource_name": "alcoholic_table"
    },
    "beverages": {
        "sql_column": "str_category",
        "query_param": "c",
        "source_key": "source_beverage_type",
        "resource_name": "beverages_table"
    },
    "glasses": {
        "sql_column": "str_glass",
        "query_param": "g",
        "source_key": "source_glass",
        "resource_name": "glass_table"
    }
}

def get_dim(table_name: str, column_name: str):
    conn = psycopg2.connect(
        dbname=os.getenv("CREDENTIALS__DATABASE"),
        user=os.getenv("CREDENTIALS__USERNAME"),
        password=os.getenv("CREDENTIALS__PASSWORD"),
        host=os.getenv("CREDENTIALS__HOST"),
        port=os.getenv("CREDENTIALS__PORT")
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT {column_name} FROM cocktail_data.{table_name}")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # Extract the values from tuples
    return [row[0] for row in rows]

def make_resource(dimension: str):
    config = DIMENSION_CONFIG[dimension]
    @dlt.resource(name=config["resource_name"], write_disposition="replace")
    def resource_func():
        values = get_dim(dimension, config["sql_column"])

        for value in values:
            url = f"https://www.thecocktaildb.com/api/json/v2/{os.getenv("BEVERAGE_API_KEY")}/filter.php?{config['query_param']}={value}"
            response = requests.get(url)
            response.raise_for_status()  # Raise exception on error
            drinks = response.json()["drinks"]
            for drink in drinks:
                if isinstance(drink, dict):  # Ensure it's a dictionary
                    drink[config["source_key"]] = value
            yield drinks

    return resource_func()

# DIMENSION_CONFIG.keys()
@dlt.source
def cocktail_filter_data():
    return [make_resource(dim) for dim in DIMENSION_CONFIG.keys()]

# pipeline
pipeline = dlt.pipeline(
    pipeline_name="cocktails_pipeline",
    destination="postgres",
    dataset_name="cocktail_data",
    dev_mode=False
)
# Run the pipeline
if __name__ == "__main__":
    load_info = pipeline.run(cocktail_filter_data())
    print(load_info)
