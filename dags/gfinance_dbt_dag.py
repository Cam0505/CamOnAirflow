import dlt
import gspread
import pandas as pd
import logging
from google.oauth2.service_account import Credentials
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, time
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import os
from path_config import ENV_FILE, DLT_PIPELINE_DIR, DBT_DIR, CREDENTIALS

load_dotenv(dotenv_path=ENV_FILE)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@dlt.source
def gsheet_finance_source(logger=None):
    @dlt.resource(write_disposition="merge", primary_key='id', name="gsheets_finance")
    def gsheet_finance_resource():
        # Initialize state within the source context
        state = dlt.current.source_state().setdefault("gsheet_finance", {
            "latest_ts": None,
            "last_run": None,
            "processed_records": 0,
            "last_run_status": None
        })
        if logger:
            logger.info(f"Current state: {state}")

        try:
            # Load data from Google Sheets
            creds = Credentials.from_service_account_file(
                CREDENTIALS,
                scopes=[
                    'https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive'
                ]
            )
            client = gspread.authorize(creds)
            SHEETNAME = os.getenv("GOOGLE_SHEET_NAME", "Basic_Financial_Data")
            sheet = client.open(SHEETNAME).sheet1
            data = sheet.get_all_records()

            if not data:
                state["last_run_status"] = "skipped_empty_data"
                if logger:
                    logger.warning("No data found in sheet")
                return

            if "DateTime" not in data[0]:
                state["last_run_status"] = "skipped_missing_datetime"
                if logger:
                    logger.warning("DateTime column missing")
                return

            # Process data and track timestamps
            df = pd.DataFrame(data)
            df['DateTime'] = pd.to_datetime(
                df['DateTime']).dt.tz_localize('UTC')
            latest_gsheet_ts = df['DateTime'].max()
            if logger:
                logger.info(f"Latest data timestamp: {latest_gsheet_ts}")

            # Check for new data
            if state["latest_ts"]:
                latest_state_ts = pd.to_datetime(state["latest_ts"])
                buffered_ts = latest_state_ts + pd.Timedelta(minutes=30)

                if latest_gsheet_ts <= buffered_ts:
                    state.update({
                        "last_run": datetime.now(ZoneInfo("UTC")).isoformat(),
                        "last_run_status": "skipped_no_new_data"
                    })
                    if logger:
                        logger.info(
                            f"\n🔁 SKIPPED LOAD:\n"
                            f"📅 GSheet timestamp: {latest_gsheet_ts}\n"
                            f"📦 Buffered DLT state timestamp: {buffered_ts}\n"
                            f"⏳ Reason: No new data within 30-minute window.\n"
                            f"{'-'*45}"
                        )
                    return

            # Update state
            state.update({
                "latest_ts": latest_gsheet_ts.isoformat(),
                "last_run": datetime.now(ZoneInfo("UTC")).isoformat(),
                "processed_records": len(df),
                "last_run_status": "success"
            })

            if logger:
                logger.info(f"Loading {len(df)} new records")
            yield df.to_dict('records')

        except Exception as e:
            state["last_run_status"] = f"failed: {str(e)}"
            if logger:
                logger.error(f"Processing failed: {e}")
            raise

    return gsheet_finance_resource


def run_dlt_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="gsheets_pipeline",
        destination=os.environ.get(
            "DLT_DESTINATION") or os.getenv("DLT_DESTINATION"),
        dataset_name="google_sheets_data", dev_mode=False,
        pipelines_dir=str(DLT_PIPELINE_DIR)
    )

    # Get the source
    source = gsheet_finance_source(logger=logger)
    try:
        load_info = pipeline.run(source)

        status = source.state.get(
            'gsheet_finance', {}).get('last_run_status', '')

        if status == 'skipped_no_new_data':
            logger.info(f"\n⏭️ resource skipped — no data loaded.")
            return False
        elif status == 'success':
            logger.info(f"\n✅ Resource loaded: {load_info}")
            return True
        else:
            logger.error(
                f"\n💥 All resources failed to load: {status}")
            return False
    except Exception as e:
        logger.error(f"\n❌ Pipeline run failed: {e}")
        return False

with DAG(
    dag_id='gfinance_dbt',
    start_date=datetime(2025, 5, 1),
    schedule= None, #'*/45 * * * *',
    catchup=False,
) as dag:

    run_pipeline = PythonOperator(
        task_id='run_dlt_pipeline',
        python_callable=run_dlt_pipeline,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"""
            cd {DBT_DIR} &&
            dbt build --select source:gsheets+ --profiles-dir . --project-dir .
        """,
    )

    _ = run_pipeline >> dbt_build


if __name__ == "__main__":
    # Run the DLT pipeline step
    print("Running DLT pipeline locally...")
    result = run_dlt_pipeline()
    print(f"DLT pipeline result: {result}")

    result = True  # Simulate success for local testing

    # Optionally run dbt build if DLT succeeded
    if result:
        print("Running dbt build locally...")
        os.system(f"cd {DBT_DIR} && dbt build --select source:gsheets+ --profiles-dir . --project-dir .")