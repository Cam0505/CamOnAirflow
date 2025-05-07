import dlt
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="../.env")


GOOGLE_SHEET_NAME = "Basic_Financial_Data"

def gsheet_source(sheet_name: str) -> pd.DataFrame:
    # sheet_id = "1q3OGkbdLP57xr58hqgn7S71nD9LYOMKgMWgwNRdGkBY"
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name("/usr/local/airflow/dlt_pipeline/credentials.json", scope)
    client = gspread.authorize(credentials)
    sheet = client.open(sheet_name).sheet1
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    # Return the dataframe to DLT
    return df


def run_dlt_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="gsheets_to_pg",
        destination="postgres",
        dataset_name="google_sheets",
        dev_mode=False
    )
    df = gsheet_source(GOOGLE_SHEET_NAME)
    # Wrap DataFrame into a named DLT resource
    data_resource = dlt.resource(df, name="gsheetFinance")
    pipeline.run(data_resource, write_disposition="merge", primary_key="id")

with DAG(
    dag_id='gfinance_dbt',
    start_date=datetime(2025, 5, 1),
    schedule_interval= None, #'*/45 * * * *',
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
                --select base_gsheets_finance
        """,
        env={
            'DB_USER': os.getenv("CREDENTIALS__USERNAME"),
            'DB_PASSWORD': os.getenv("CREDENTIALS__PASSWORD"),
        }
    )

    run_pipeline >> dbt_run