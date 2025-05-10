import dlt
import requests
import logging
import subprocess
from os import getenv
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

def get_weather_data(lat: float, lng: float, start_date: datetime, end_date: datetime, timezone: str):
    params = {
        "latitude": lat,
        "longitude": lng,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "daily": ",".join([
            "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "precipitation_sum", "windspeed_10m_max", "windgusts_10m_max",
            "sunshine_duration", "uv_index_max"
        ]),
        "timezone": timezone
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch weather data for ({lat}, {lng}) from {start_date} to {end_date}: {e}")
        return None

def split_into_yearly_chunks(start_date: datetime, end_date: datetime):
    chunks = []
    current = start_date
    while current <= end_date:
        year_end = min(datetime(current.year + 1, 1, 1).date() - timedelta(days=1), end_date)
        chunks.append((current, year_end))
        current = year_end + timedelta(days=1)
    return chunks

@dlt.source
def openmeteo_source(cities: list[dict], start_date: datetime, end_date: datetime):

    @dlt.resource(name="daily_weather", write_disposition="merge", primary_key=["date", "City"])
    def weather_resource():
        for city_info in cities:
            for chunk_start, chunk_end in split_into_yearly_chunks(start_date, end_date):
                data = get_weather_data(
                    lat=city_info["lat"],
                    lng=city_info["lng"],
                    start_date=chunk_start,
                    end_date=chunk_end,
                    timezone=city_info["timezone"]
                )
                if not data or "daily" not in data:
                    continue
                daily_data = data["daily"]
                for i in range(len(daily_data["time"])):
                    yield {
                        "date": daily_data["time"][i],
                        "City": city_info["city"],
                        "temperature_max": daily_data["temperature_2m_max"][i],
                        "temperature_min": daily_data["temperature_2m_min"][i],
                        "temperature_mean": daily_data["temperature_2m_mean"][i],
                        "precipitation_sum": daily_data["precipitation_sum"][i],
                        "windspeed_max": daily_data["windspeed_10m_max"][i],
                        "windgusts_max": daily_data["windgusts_10m_max"][i],
                        "sunshine_duration": daily_data["sunshine_duration"][i],
                        "uv_index_max": daily_data["uv_index_max"][i],
                        "location": {
                            "lat": city_info["lat"],
                            "lng": city_info["lng"]
                        },
                        "timestamp": datetime.now(ZoneInfo(city_info["timezone"])).isoformat()
                    }

    return weather_resource()

pipeline = dlt.pipeline(
    pipeline_name="openmeteo_pipeline",
    destination="postgres",
    dataset_name="weather_data",
    dev_mode=False
)

if __name__ == "__main__":
    try:
        cities = [
            {"city": "Sydney", "lat": -33.8688, "lng": 151.2093, "timezone": "Australia/Sydney"},
            {"city": "Melbourne", "lat": -37.8136, "lng": 144.9631, "timezone": "Australia/Melbourne"},
            {"city": "Brisbane", "lat": -27.4698, "lng": 153.0251, "timezone": "Australia/Brisbane"},
            {"city": "Perth", "lat": -31.9505, "lng": 115.8605, "timezone": "Australia/Perth"},
            {"city": "Adelaide", "lat": -34.9285, "lng": 138.6007, "timezone": "Australia/Adelaide"},
            {"city": "Canberra", "lat": -35.2809, "lng": 149.1300, "timezone": "Australia/Sydney"},
            {"city": "Hobart", "lat": -42.8821, "lng": 147.3272, "timezone": "Australia/Hobart"},
            {"city": "Darwin", "lat": -12.4634, "lng": 130.8456, "timezone": "Australia/Darwin"}
        ]

        today = datetime.now(ZoneInfo("Australia/Sydney")).date()
        end_date = today - timedelta(days=2)
        # start_date = end_date - timedelta(days=3 * 365) Last 3 years worth of data, don't need this now
        start_date = end_date - timedelta(days=14) # Merge in 14 days worth of data

        logger.info(f"Fetching data from {start_date} to {end_date} for {len(cities)} cities.")
        pipeline.run(openmeteo_source(cities=cities, start_date=start_date, end_date=end_date))
        logger.info("DLT pipeline run completed.")

        logger.info("Running dbt transformation for open-meteo...")
        try:
            result = subprocess.run(
                [
                    "dbt", "run",
                    "--select", "source:weather+",
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
        logger.exception("Unhandled exception in Open-Meteo pipeline execution.")