import dlt
# import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
from dotenv import load_dotenv
import logging

load_dotenv(dotenv_path="../.env")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)


@dlt.resource(name="consumption", write_disposition="append")
def meals_api():
    url = f"https://www.themealdb.com/api/json/v1/1/random.php"
    all_meals = []

    for i in range(1):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            meals = response.json().get("meals")
            if not meals:
                logging.warning(f"No meals returned in iteration {i+1}")
                continue
            all_meals.extend(meals)
        except requests.RequestException as e:
            logging.error(f"Request failed on iteration {i+1}: {e}", exc_info=True)
        except Exception as e:
            logging.error(f"Unexpected error in meals_api on iteration {i+1}: {e}", exc_info=True)

    if not all_meals:
        logging.warning("No meals collected in total; this may result in no data being loaded.")
    else:
        logging.info(f"Collected {len(all_meals)} meals.")

    yield all_meals


@dlt.source
def meals():
    return meals_api()

def run_dlt_pipeline():
# pipeline
    try:
        pipeline = dlt.pipeline(
            pipeline_name="meals_pipeline",
            destination="postgres",
            dataset_name="meals_data",
            dev_mode=False
        )
        load_info = pipeline.run(meals())
        logging.info(f"DLT pipeline run complete: {load_info}")
    except Exception as e:
        logging.error("DLT pipeline run failed", exc_info=True)
        raise  # Reraise so Airflow marks task as failed


with DAG(
    dag_id='meals_postgres_dbt',
    start_date=datetime(2025, 5, 1),
    schedule_interval= None, #'0 */12 * * *', Every 12 hrs
    catchup=False,
) as dag:

    run_pipeline = PythonOperator(
        task_id='run_dlt_pipeline',
        python_callable=run_dlt_pipeline,
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="""
            dbt build \
                --project-dir /usr/local/airflow/dbt \
                --profiles-dir /usr/local/airflow/dbt \
                --log-path /usr/local/airflow/dbt/dbt.log \
                --select source:meals+
        """,
        env={
            'DB_USER': os.getenv("CREDENTIALS__USERNAME"),
            'DB_PASSWORD': os.getenv("CREDENTIALS__PASSWORD"),
        }
    )

    run_pipeline >> dbt_run

