import dlt
import requests
import logging
import subprocess
import psycopg2
from os import getenv
from dotenv import load_dotenv
from datetime import datetime
from zoneinfo import ZoneInfo

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path="../.env")

BASE_URL = "https://api.openuv.io/api/v1/uv"


def get_dates():
    try:
        with psycopg2.connect(
            dbname=getenv("CREDENTIALS__DATABASE"),
            user=getenv("CREDENTIALS__USERNAME"),
            password=getenv("CREDENTIALS__PASSWORD"),
            host=getenv("CREDENTIALS__HOST"),
            port=getenv("CREDENTIALS__PORT")
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT date_col FROM public_staging.uv_data_dates")
                dates = [row[0] for row in cur.fetchall()]
                logger.info(f"Retrieved {len(dates)} missing date(s).")
                return dates
    except Exception as e:
        logger.exception("Failed to retrieve missing dates from the database.")
        return []


def get_uv_data(lat: float, lng: float, dt: datetime):
    dt_local = datetime.combine(dt, datetime.min.time(), tzinfo=ZoneInfo("Australia/Sydney")).replace(hour=12)
    headers = {"x-access-token": getenv("UV_API_KEY")}
    params = {
        "lat": lat,
        "lng": lng,
        "alt": 100,
        "dt": dt_local.astimezone(ZoneInfo("UTC")).strftime('%Y-%m-%dT%H:%M:%SZ')
    }
    try:
        response = requests.get(BASE_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        return [response.json()]
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch UV data for ({lat}, {lng}, {dt.date()}): {e}")
        return []


@dlt.source
def openuv_source(cities: list[dict], dates: list[datetime]):

    @dlt.resource(name="uv_index", write_disposition="merge", primary_key=["uv_time", "City"])
    def uv_resource():
        for dt in dates:
            for city_info in cities:
                uv_data = get_uv_data(city_info["lat"], city_info["lng"], dt)
                for entry in uv_data:
                    yield {
                        "uv": entry["result"]["uv"],
                        "uv_max": entry["result"]["uv_max"],
                        "uv_time": entry["result"]["uv_time"],
                        "ozone": entry["result"]["ozone"],
                        "City": city_info["city"],
                        "location": {
                            "lat": city_info["lat"],
                            "lng": city_info["lng"]
                        },
                        "timestamp": datetime.now(ZoneInfo("Australia/Sydney")).isoformat()
                    }

    return uv_resource()


pipeline = dlt.pipeline(
    pipeline_name="openuv_pipeline",
    destination="postgres",
    dataset_name="uv_data",
    dev_mode=False
)


if __name__ == "__main__":
    try:
        cities = [
            {"city": "Sydney", "lat": -33.8688, "lng": 151.2093},
            {"city": "Melbourne", "lat": -37.8136, "lng": 144.9631},
            {"city": "Brisbane", "lat": -27.4698, "lng": 153.0251},
            {"city": "Perth", "lat": -31.9505, "lng": 115.8605},
            {"city": "Adelaide", "lat": -34.9285, "lng": 138.6007},
            {"city": "Canberra", "lat": -35.2809, "lng": 149.1300},
            {"city": "Hobart", "lat": -42.8821, "lng": 147.3272},
            {"city": "Darwin", "lat": -12.4634, "lng": 130.8456}
        ]

        missing_dates = get_dates()

        if not missing_dates:
            logger.info("No missing dates found — skipping pipeline and dbt.")
        else:
            logger.info("Running DLT pipeline...")
            pipeline.run(openuv_source(cities, missing_dates))
            logger.info("DLT pipeline run completed.")

            logger.info("Running dbt transformation...")
            try:
                result = subprocess.run(
                    [
                        "dbt", "run",
                        "--select", "source:uv+",
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
        logger.exception("Unhandled exception in main pipeline execution.")