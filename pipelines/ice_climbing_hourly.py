import logging
from dotenv import load_dotenv
from datetime import datetime, date, timedelta, timezone
import pandas as pd
import dlt
from dlt.sources.helpers import requests
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
import os
import time as tyme
from project_path import get_project_paths, set_dlt_env_vars
import duckdb

# Load environment variables and set DLT config
paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
DBT_DIR = paths["DBT_DIR"]

load_dotenv(dotenv_path=ENV_FILE)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Configurable time window at the top
END_DT_LAG_DAYS = 2  # Open-Meteo archive is usually 1-2 days behind
DATA_WINDOW_DAYS = 120  # 4 months

end_dt = datetime.now(timezone.utc) - timedelta(days=END_DT_LAG_DAYS)
start_dt = end_dt - timedelta(days=DATA_WINDOW_DAYS)

def get_ice_climbing_with_thresholds():
    now = datetime.now(timezone.utc)
    return [
        {
            "name": "Remarkables", "country": "NZ", "lat": -45.0716, "lon": 168.8030, "timezone": "Pacific/Auckland",
            "valid_from": now, "valid_to": None,
            "forming_temp": 0.5, "forming_hours": 8, "forming_days": 3, "formed_days": 14, "degrade_temp": 1.5, "degrade_hours": 4,
        },
        {
            "name": "Black Peak", "country": "NZ", "lat": -44.5841, "lon": 168.8309, "timezone": "Pacific/Auckland",
            "valid_from": now, "valid_to": None,
            "forming_temp": 0.0, "forming_hours": 10, "forming_days": 4, "formed_days": 16, "degrade_temp": 2.0, "degrade_hours": 5,
        },
        {
            "name": "Dasler Pinnacles", "country": "NZ", "lat": -43.9568, "lon": 169.8682, "timezone": "Pacific/Auckland",
            "valid_from": now, "valid_to": None,
            "forming_temp": 0.3, "forming_hours": 7, "forming_days": 2, "formed_days": 12, "degrade_temp": 1.2, "degrade_hours": 3,
        },
        {
            "name": "Milford Sound", "country": "NZ", "lat": -44.7726, "lon": 168.0389, "timezone": "Pacific/Auckland",
            "valid_from": now, "valid_to": None,
            "forming_temp": 0.5, "forming_hours": 8, "forming_days": 3, "formed_days": 14, "degrade_temp": 1.5, "degrade_hours": 4,
        }
    ]


ICE_CLIMBING = get_ice_climbing_with_thresholds()
BATCH_SIZE = 500



def get_missing_dates(logger, locations, start_dt, end_dt, dataset):
    """Returns missing dates for each location."""
    try:
        missing = {}
        all_dates = pd.date_range(start_dt.date(), end_dt.date(), freq="D")
        table_truncated = dataset is None or dataset.empty
        for loc in locations:
            name = loc["name"]
            if table_truncated or name not in dataset["location"].unique():
                missing[name] = set(all_dates)
            else:
                loc_df = dataset[dataset["location"] == name]
                existing = set(pd.to_datetime(loc_df["date"]).dt.date)
                missing[name] = set(all_dates.date) - existing
        return missing, table_truncated
    except Exception as e:
        logger.error(f"Failed to retrieve missing dates: {e}")
        missing = {loc["name"]: set(pd.date_range(start_dt.date(), end_dt.date(), freq='D')) for loc in locations}
        return missing, False


def fetch_hourly_data(location, start_dt, end_dt):
    logger.info(f"Fetching hourly data for {location['name']} from {start_dt} to {end_dt}")
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "start_date": start_dt.date().isoformat(),
        "end_date": end_dt.date().isoformat(),
        "hourly": ",".join([
            "temperature_2m", "precipitation", "snowfall", "cloudcover", "windspeed_10m",
            "dew_point_2m", "surface_pressure", "relative_humidity_2m",
            "shortwave_radiation", "sunshine_duration", "is_day"
        ]),
        "timezone": location["timezone"]
    }
    try:
        response = requests.get(url, params=params, timeout=60)
        logger.debug(f"API request URL: {response.url}")
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            logger.error(f"API error: {data.get('reason')}")
            return None
        return data
    except Exception as e:
        logger.error(f"Request failed for {location['name']}: {e}")
        return None

def count_freeze_thaw(series):
    # Count number of times temperature crosses 0°C
    return int(((series.shift(1) < 0) & (series >= 0)).sum() + ((series.shift(1) > 0) & (series <= 0)).sum())


def freeze_thaw_factor(cycles):
            # Bonus for 1–2 cycles, penalty for >2
            if cycles <= 2:
                return 1.05  # slight bonus
            elif cycles <= 4:
                return 1.0   # neutral
            else:
                return max(0.8, 1 - 0.2 * ((cycles - 4) / 4))  # penalty


def enrich_ice_conditions(df, thresholds):
    """Add all daily stats and 4 enrichment columns (0-1 scores) to the hourly DataFrame."""
    try:
        df = df.sort_values("datetime")
        forming_temp = thresholds["forming_temp"]
        forming_hours = thresholds["forming_hours"]
        forming_days = thresholds["forming_days"]
        formed_days = thresholds["formed_days"]
        degrade_temp = thresholds["degrade_temp"]
        degrade_hours = thresholds["degrade_hours"]

        df["date"] = df["datetime"].dt.date

        # Precompute daily stats
        daily_stats = df.groupby("date").agg(
            hours_below_freeze = ("temperature_2m", lambda x: (x < forming_temp).sum()),
            mean_cloud = ("cloudcover", "mean"),
            total_snow = ("snowfall", "sum"),
            mean_wind = ("windspeed_10m", "mean"),
            mean_rh = ("relative_humidity_2m", "mean"),
            mean_shortwave = ("shortwave_radiation", "mean"),
            hours_above_degrade = ("temperature_2m", lambda x: (x > degrade_temp).sum()),
            total_precip = ("precipitation", "sum"),
            freeze_thaw_cycles = ("temperature_2m", count_freeze_thaw),
        )

        # Forming day: at least forming_hours below freezing
        daily_stats["is_forming_day"] = (daily_stats["hours_below_freeze"] >= forming_hours).astype(int)

        # Fraction of forming days in the last forming_days window
        daily_stats["is_ice_forming"] = (
            daily_stats["is_forming_day"]
            .rolling(window=forming_days, min_periods=1)
            .mean()
            .clip(0, 1)
        )

        # Weighted score for forming (for quality/has_formed)
        forming_score = (
            0.4 * (daily_stats["hours_below_freeze"] / 24).clip(0, 1) +
            0.15 * (1 - daily_stats["mean_cloud"] / 100).clip(0, 1) +
            0.15 * (daily_stats["total_snow"] / 10).clip(0, 1) +
            0.1 * (1 - daily_stats["mean_wind"] / 15).clip(0, 1) +
            0.1 * (daily_stats["mean_rh"] / 100).clip(0, 1) +
            0.1 * (1 - daily_stats["mean_shortwave"] / 200).clip(0, 1)
        ).clip(0, 1)

        # Rolling mean for last N days for "has formed" and "ice_quality"
        daily_stats["ice_has_formed"] = (
            forming_score.rolling(window=formed_days, min_periods=1).mean().clip(0, 1)
        )

        # Degrading: combine several conditions, score is fraction of last 7 days with any degrade condition
        degrade_conditions = (
            (daily_stats["hours_above_degrade"] >= degrade_hours) |
            ((daily_stats["mean_shortwave"] > 150) & (daily_stats["mean_cloud"] < 30)) |
            ((daily_stats["total_precip"] > 0) & (daily_stats["hours_above_degrade"] > 0)) |
            ((daily_stats["mean_rh"] > 90) & (daily_stats["hours_above_degrade"] > 0)) |
            ((daily_stats["mean_wind"] > 10) & (daily_stats["hours_above_degrade"] > 0))
        )
        daily_stats["is_ice_degrading"] = (
            degrade_conditions.rolling(window=7, min_periods=1).mean().clip(0, 1)
        )

        # Ice quality: penalize for degrading and for excessive freeze/thaw cycles
        # Adjust penalty/bonus for freeze-thaw cycles

        daily_stats["ice_quality"] = (
            daily_stats["ice_has_formed"] *
            (1 - 0.7 * daily_stats["is_ice_degrading"]) *
            daily_stats["freeze_thaw_cycles"].apply(freeze_thaw_factor)
        ).clip(0, 1)

        # Merge all daily stats and enrichment columns back to hourly
        df = df.merge(
            daily_stats, left_on="date", right_index=True, how="left"
        )
        return df
    except Exception as e:
        logger.error(f"Error in enrich_ice_conditions: {e}")
        raise

def aggregate_to_daily(df):
    # Choose which columns to aggregate and how
    agg_dict = {
        "temperature_2m": "mean",
        "precipitation": "sum",
        "snowfall": "sum",
        "cloudcover": "mean",
        "windspeed_10m": "mean",
        "dew_point_2m": "mean",
        "surface_pressure": "mean",
        "relative_humidity_2m": "mean",
        "shortwave_radiation": "mean",
        "sunshine_duration": "sum",
        "is_day": "sum",
        # Enrichment columns
        "is_ice_forming": "mean",
        "ice_has_formed": "mean",
        "ice_quality": "mean",
        "is_ice_degrading": "mean",
        # All daily_stats columns: use "first" (they are constant per day)
        "hours_below_freeze": "first",
        "mean_cloud": "first",
        "total_snow": "first",
        "mean_wind": "first",
        "mean_rh": "first",
        "mean_shortwave": "first",
        "hours_above_degrade": "first",
        "total_precip": "first",
        "freeze_thaw_cycles": "first",
    }
    group_cols = ["location", "country", "date"]
    daily_df = df.groupby(group_cols).agg(agg_dict).reset_index()
    return daily_df


@dlt.source
def ice_climbing_hourly_source(logger: logging.Logger, dataset):
    @dlt.resource(write_disposition="merge", name="ice_climbing_hourly", primary_key=["location", "date"])
    def hourly_data():
        state = dlt.current.source_state().setdefault("hourly_ice", {
            "Processed_Ranges": {}
        })
        processed = 0

        # Use missing dates to avoid reprocessing
        missing_by_loc, table_truncated = get_missing_dates(logger, ICE_CLIMBING, start_dt, end_dt, dataset)

        for location in ICE_CLIMBING:
            location_name = location["name"]
            country = location["country"]

            missing_dates = missing_by_loc.get(location_name, set())
            if not table_truncated and not missing_dates:
                logger.info(f"No missing dates for {location_name}, skipping.")
                continue

            # If truncated, or missing dates, fetch the full range (DLT will deduplicate)
            fetch_start = start_dt
            fetch_end = end_dt

            try:
                data = fetch_hourly_data(location, fetch_start, fetch_end)
                if not data or "hourly" not in data or not data["hourly"].get("time"):
                    logger.warning(f"No data returned for {location_name}, skipping.")
                    continue
                h = data["hourly"]
                df = pd.DataFrame({"datetime": pd.to_datetime(h["time"])})
                for col in [
                    "temperature_2m", "precipitation", "snowfall", "cloudcover", "windspeed_10m",
                    "dew_point_2m", "surface_pressure", "relative_humidity_2m",
                    "shortwave_radiation", "sunshine_duration", "is_day"
                ]:
                    df[col] = h.get(col)
                df["location"] = location_name
                df["country"] = country
                df["date"] = df["datetime"].dt.date

                # Only keep rows for missing dates (unless truncated, then keep all)
                if not table_truncated:
                    df = df[df["date"].isin(missing_dates)]
                    if df.empty:
                        logger.info(f"No new data to process for {location_name}, skipping.")
                        continue

                # Enrich
                df = enrich_ice_conditions(df, location)
                daily_df = aggregate_to_daily(df)
                records = daily_df.to_dict("records")
                for i in range(0, len(records), BATCH_SIZE):
                    yield records[i:i+BATCH_SIZE]
                processed += 1
                logger.info(f"Processed and yielded data for {location_name}.")

                # Update processed range in state
                if not df.empty:
                    min_date = df["datetime"].min()
                    max_date = df["datetime"].max()
                    state["Processed_Ranges"][location_name] = {
                        "min": str(min_date),
                        "max": str(max_date)
                    }
                tyme.sleep(0.75)
            except Exception as e:
                logger.error(f"Failed to process {location_name}: {e}")
        logger.info(f"Hourly data resource finished. {processed} location(s) processed.")
    return hourly_data



FAR_FUTURE = datetime(9999, 12, 31, tzinfo=timezone.utc)

@dlt.resource(write_disposition="merge", name="ice_climbing_thresholds", primary_key=["name", "valid_from"])
def ice_climbing_thresholds_resource(logger, thresholds_dataset):
    now = datetime.now(timezone.utc)
    threshold_fields = ["forming_temp", "forming_hours", "forming_days", "formed_days", "degrade_temp", "degrade_hours"]
    changes = 0

    for loc in ICE_CLIMBING:
        if (
            thresholds_dataset is not None
            and not thresholds_dataset.empty
            and "valid_to" in thresholds_dataset.columns
        ):
            latest = (
                thresholds_dataset[
                    (thresholds_dataset["name"] == loc["name"]) &
                    (thresholds_dataset["valid_to"] == FAR_FUTURE)
                ]
                .sort_values("valid_from", ascending=False)
                .head(1)
            )
            latest = latest.iloc[0] if not latest.empty else None
        else:
            latest = None

        if latest is not None:
            changed = any(float(latest[field]) != float(loc[field]) for field in threshold_fields)
        else:
            changed = True  # No previous record

        if changed:
            changes += 1
            logger.info(f"Thresholds changed for {loc['name']}. Writing new version.")
            # If there is a previous record, yield a record to close it out
            if latest is not None:
                yield {
                    "name": latest["name"],
                    "country": latest["country"],
                    "valid_from": latest["valid_from"],
                    "valid_to": now,
                    "forming_temp": latest["forming_temp"],
                    "forming_hours": latest["forming_hours"],
                    "forming_days": latest["forming_days"],
                    "formed_days": latest["formed_days"],
                    "degrade_temp": latest["degrade_temp"],
                    "degrade_hours": latest["degrade_hours"],
                }
            # Yield the new record with far-future valid_to
            yield {
                "name": loc["name"],
                "country": loc["country"],
                "valid_from": now,
                "valid_to": FAR_FUTURE,
                "forming_temp": loc["forming_temp"],
                "forming_hours": loc["forming_hours"],
                "forming_days": loc["forming_days"],
                "formed_days": loc["formed_days"],
                "degrade_temp": loc["degrade_temp"],
                "degrade_hours": loc["degrade_hours"],
            }
        else:
            logger.info(f"No threshold change for {loc['name']}.")
    logger.info(f"Threshold resource finished. {changes} location(s) updated.")



# In your main pipeline run:
if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="ice_climbing_hourly_pipeline",
        destination=os.getenv("DLT_DESTINATION", "motherduck"),
        dataset_name="ice_climbing",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )
    ice_climbing_dataset = None
    thresholds_dataset = None
    try:
        ice_climbing_dataset = pipeline.dataset()["ice_climbing_hourly"].df()
        thresholds_dataset = pipeline.dataset()["ice_climbing_thresholds"].df()
    except (PipelineNeverRan, DatabaseUndefinedRelation, ValueError, KeyError):
        logger.warning("No previous runs or table found. Assuming first run or empty DB.")
        ice_climbing_dataset = None
        thresholds_dataset = None


    
    try:
        logger.info("Running ice climbing hourly pipeline...")
        source = ice_climbing_hourly_source(logger, ice_climbing_dataset)
        load_info = pipeline.run(source)
        logger.info(f"Pipeline run completed. Load Info: {load_info}")
        state = source.state.get('hourly_ice', {}).get('Processed_Ranges', {})
        if state:
            logger.info(f"Processed date ranges: {state}")
        else:
            logger.info("No processed date ranges found in state.")


        logger.info("Running thresholds resource...")
        thresholds_resource = pipeline.run(ice_climbing_thresholds_resource(logger, thresholds_dataset))
    except Exception as e:
        # Run the thresholds resource
        logger.info("Checking for threshold changes...")
        logger.error(f"Pipeline run failed: {e}")
        raise