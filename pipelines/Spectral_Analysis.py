import dlt
import logging
from dotenv import load_dotenv
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import json
import time
import pyarrow as pa
import numpy as np
import pandas as pd

# Microsoft Planetary Computer + STAC/xarray stack
import planetary_computer
import pystac_client
import stackstac
import xarray as xr

from project_path import get_project_paths, set_dlt_env_vars

# --------------------------- Bootstrap & Config ------------------------------

# Load environment variables and set DLT config
paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
DBT_DIR = paths["DBT_DIR"]

load_dotenv(dotenv_path=ENV_FILE)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("dlt").setLevel(logging.INFO)

# Locations (unchanged)
LOCATIONS = [
    {"name": "Wye Creek Ice Curtain", "lat": -45.0863, "lon": 168.8125},
    {"name": "Wye Creek", "lat": -45.087152, "lon": 168.811421},
    {"name": "Island Gully", "lat": -42.133076, "lon": 172.755765},
    # {"name": "Milford Sound", "lat": -44.770974, "lon": 168.036796},
    # {"name": "Bush Stream", "lat": -43.8487, "lon": 170.0439},
    # {"name": "Shrimpton Ice", "lat": -44.222395, "lon": 169.307676},
]
# ~12 m buffer around each point (in degrees)
EPSILON = 0.00018

START_DATE = date(2020, 1, 1)
BUFFER_DAYS = 7
TODAY = datetime.now(ZoneInfo("Pacific/Auckland")).date()
END_DATE = TODAY - timedelta(days=2)

MAX_RETRIES = 3
RETRY_DELAY = 6  # seconds

def json_converter(o):
    if isinstance(o, date):
        return o.isoformat()
    return str(o)

# --------------------------- MPC / STAC Helpers -----------------------------

# Open MPC STAC once
MPC_CATALOG = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

# SCL classes for masking (Sentinel-2 L2A)
# 0 NODATA, 1 SATURATED/DEFECTIVE, 2 DARK_SHADOWS, 3 CLOUD_SHADOWS, 4 VEGETATION,
# 5 BARE_SOIL, 6 WATER, 7 UNCLASSIFIED, 8 CLOUD_MEDIUM, 9 CLOUD_HIGH, 10 THIN_CIRRUS, 11 SNOW_ICE
SCL_MASK_VALUES = {0, 1, 2, 3, 8, 9, 10}  # mask nodata/defective/shadows/clouds/cirrus
# Keep SNOW_ICE (11) unmasked; keep veg/soil/water/unclassified.

def _rolling_avg_safe(arr: np.ndarray, window: int = 5) -> list[float | None]:
    out = []
    for i in range(len(arr)):
        start = max(0, i - window // 2)
        end = min(len(arr), i + window // 2 + 1)
        vals = arr[start:end]
        vals = vals[~np.isnan(vals)]
        out.append(float(np.mean(vals)) if len(vals) else None)
    return out

def _df_to_pa_table(df: pd.DataFrame, location: str) -> pa.Table | None:
    """Input df columns: date, ndsi, ndwi, ndii. Adds smoothed cols + location and returns a PyArrow table."""
    if df is None or df.empty:
        return None

    df = df.sort_values("date")
    ndsi = df["ndsi"].astype("float32").to_numpy()
    ndwi = df["ndwi"].astype("float32").to_numpy()
    ndii = df["ndii"].astype("float32").to_numpy()

    ndsi_smooth = np.array(_rolling_avg_safe(ndsi, 5), dtype="float32")
    ndwi_smooth = np.array(_rolling_avg_safe(ndwi, 5), dtype="float32")
    ndii_smooth = np.array(_rolling_avg_safe(ndii, 5), dtype="float32")

    # Ensure python date objects
    date_objs = [pd.to_datetime(d).date() for d in df["date"]]

    return pa.table(
        {
            "date": pa.array(date_objs, pa.date32()),
            "location": pa.array([location] * len(date_objs), pa.string()),
            "ndsi": pa.array(ndsi, pa.float32()),
            "ndsi_smooth": pa.array(ndsi_smooth, pa.float32()),
            "ndwi": pa.array(ndwi, pa.float32()),
            "ndwi_smooth": pa.array(ndwi_smooth, pa.float32()),
            "ndii": pa.array(ndii, pa.float32()),
            "ndii_smooth": pa.array(ndii_smooth, pa.float32()),
        }
    )

def _years(span_start: date, span_end: date) -> list[tuple[date, date]]:
    """Split [span_start, span_end] into year-bounded chunks for performance & memory control."""
    parts: list[tuple[date, date]] = []
    cur = span_start
    while cur <= span_end:
        year_end = date(cur.year, 12, 31)
        parts.append((cur, min(year_end, span_end)))
        cur = date(cur.year + 1, 1, 1)
    return parts

def fetch_stats_ndsi_mpc(
    bbox: list[float],
    start_date: date,
    end_date: date,
    logger: logging.Logger,
    *,
    epsg_out: int = 3857,
    resolution: int = 10,
    chunksize: int = 2048,
) -> pd.DataFrame | None:
    """
    Accuracy-focused version (Sentinel parity):
    - Process in yearly chunks to limit IO/compute.
    - For each day, select the single lowest-cloud item (mimics 'leastCC' mosaicking intent).
    - Load only the chosen items for that year and assets B03,B08,B11,SCL.
    - Apply SCL mask (mask clouds/cirrus/shadows/nodata) but KEEP SCL=11 (snow/ice).
    - Compute NDSI/NDWI/NDII per pixel, then spatial mean per scene.
    - Output daily means as DataFrame[date, ndsi, ndwi, ndii].
    """
    all_years = _years(start_date, end_date)
    out_parts: list[pd.DataFrame] = []

    for y_start, y_end in all_years:
        iso_range = f"{y_start.isoformat()}/{y_end.isoformat()}"
        logger.info(f"[MPC] STAC search for {iso_range}")

        # 1) Search items for the year
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                search = MPC_CATALOG.search(
                    collections=["sentinel-2-l2a"],
                    bbox=bbox,
                    datetime=iso_range,
                    query={"eo:cloud_cover": {"lt": 75}},
                )
                items = list(search.items())  # no deprecation warning
                break
            except Exception as e:
                logger.warning(f"[MPC] STAC search failed (attempt {attempt}/{MAX_RETRIES}): {e}")
                if attempt == MAX_RETRIES:
                    items = []
                else:
                    time.sleep(RETRY_DELAY * attempt)

        if not items:
            logger.info(f"[MPC] No items in {iso_range}")
            continue

        # 2) Per-day lowest-cloud selection
        by_day: dict[date, dict] = {}
        for it in items:
            dt = pd.to_datetime(it.properties.get("datetime")).date()
            cc = it.properties.get("eo:cloud_cover")
            if cc is None:
                cc = 1000.0  # deprioritize unknown cloud cover
            cc = float(cc)
            prev = by_day.get(dt)
            if prev is None or cc < prev["cc"]:
                by_day[dt] = {"item": it, "cc": cc}

        chosen_items = [v["item"] for v in sorted(by_day.values(), key=lambda x: x["item"].properties["datetime"])]
        if not chosen_items:
            logger.info(f"[MPC] No chosen items after per-day filtering in {iso_range}")
            continue

        signed = [planetary_computer.sign(i) for i in chosen_items]

        # 3) Stack ONLY chosen items; include SCL for masking
        try:
            da = stackstac.stack(
                signed,
                assets=["B03", "B08", "B11", "SCL"],
                resolution=resolution,   # meters
                epsg=epsg_out,           # enforce common CRS
                bounds_latlon=bbox,      # small crop then reproject
                chunksize=chunksize,
            )
        except Exception as e:
            logger.error(f"[MPC] stackstac failed (year {y_start.year}): {e}")
            continue

        if da.sizes.get("time", 0) == 0:
            continue

        # 4) Select bands. 'SCL' is categorical band; others are reflectance bands.
        try:
            B03 = da.sel(band="B03")
            B08 = da.sel(band="B08")
            B11 = da.sel(band="B11")
            SCL = da.sel(band="SCL")
        except Exception as e:
            logger.error(f"[MPC] Could not select bands B03/B08/B11/SCL for {y_start.year}: {e}")
            continue

        # 5) Build a mask from SCL codes (mask clouds/cirrus/shadows/nodata/defective)
        # Keep snow/ice (11) and valid land/water classes.
        # SCL values are integers; construct a boolean keep-mask:
        # True where pixel is valid (not in SCL_MASK_VALUES) and SCL != 0 (implicit in set).
        scl_mask = xr.ones_like(SCL, dtype=bool)
        for bad in SCL_MASK_VALUES:
            scl_mask = scl_mask & (SCL != bad)

        # 6) Indices with divide-by-zero guards; apply SCL mask BEFORE reduction
        def safe_div(n, d):
            return n / xr.where(d == 0, np.nan, d)

        ndsi = safe_div(B03 - B11, B03 + B11).where(scl_mask)
        ndwi = safe_div(B03 - B08, B03 + B08).where(scl_mask)
        ndii = safe_div(B08 - B11, B08 + B11).where(scl_mask)

        # 7) Spatial mean per scene (x,y) after masking
        ds_mean = xr.Dataset(
            {
                "ndsi": ndsi.mean(dim=["x", "y"], skipna=True),
                "ndwi": ndwi.mean(dim=["x", "y"], skipna=True),
                "ndii": ndii.mean(dim=["x", "y"], skipna=True),
            }
        )

        # 8) Convert to per-day records
        df = ds_mean.to_dataframe().reset_index()
        if df.empty:
            continue
        df["date"] = pd.to_datetime(df["time"]).dt.date
        out_parts.append(df[["date", "ndsi", "ndwi", "ndii"]])

    if not out_parts:
        return None

    out = pd.concat(out_parts, ignore_index=True).sort_values("date").drop_duplicates("date")
    # Clamp to requested bounds (since we chunk yearly)
    out = out[(out["date"] >= start_date) & (out["date"] <= end_date)]
    return out.reset_index(drop=True)

# --------------------------- Incremental Ranges ------------------------------

def compute_fetch_ranges(
    global_min: date,
    global_max: date,
    state_min: date | None,
    state_max: date | None,
    buffer_days: int = 7,
    truncation: bool = False,
) -> list[tuple[date, date]]:
    """
    Returns a list of (fetch_start, fetch_end) tuples for needed fetches.
    If no fetching needed, returns [].
    - If truncation, always fetch (global_min, global_max).
    - If no state (first run), fetch (global_min, global_max).
    - If state_min > global_min, fetch (global_min, state_min).
    - If state_max < global_max, fetch (max(global_min, state_max-buffer), global_max).
    - If fully up-to-date, return [].
    """
    ranges = []

    if truncation:
        return [(global_min, global_max)]

    if state_min is None or state_max is None:
        return [(global_min, global_max)]

    if state_min > global_min:
        ranges.append((global_min, state_min))

    if state_max < global_max:
        fetch_start = max(global_min, state_max - timedelta(days=buffer_days))
        if fetch_start < global_max:
            ranges.append((fetch_start, global_max))

    return ranges

# ------------------------------- DLT Source ---------------------------------

@dlt.source
def sentinel_source(logger: logging.Logger, locations_with_data: set):
    @dlt.resource(write_disposition="replace", name="ice_indices", primary_key=["location", "date"])
    def ice_indices_resource():
        state = dlt.current.source_state().setdefault(
            "ice_indices",
            {"location_dates": {}, "location_status": {}, "run_dates": {}},
        )

        for loc in LOCATIONS:
            loc_name = loc["name"]
            state["run_dates"][loc_name] = []

            truncation = loc_name not in locations_with_data
            state_dates = state["location_dates"].get(loc_name, {})
            state_min = state_dates.get("start_date")
            state_max = state_dates.get("end_date")

            BBOX = [
                loc["lon"] - EPSILON,
                loc["lat"] - EPSILON,
                loc["lon"] + EPSILON,
                loc["lat"] + EPSILON,
            ]

            ranges = compute_fetch_ranges(
                global_min=START_DATE,
                global_max=END_DATE,
                state_min=state_min,
                state_max=state_max,
                buffer_days=BUFFER_DAYS,
                truncation=truncation,
            )

            try:
                fetched_any = False

                if ranges:
                    state["run_dates"][loc_name] = ranges

                    run_start = min(fs for fs, _ in ranges)
                    run_end = max(fe for _, fe in ranges)

                    new_start = min([d for d in [state_min, run_start] if d is not None])
                    new_end = max([d for d in [state_max, run_end] if d is not None])

                    state["location_dates"][loc_name] = {"start_date": new_start, "end_date": new_end}

                    for fetch_start, fetch_end in ranges:
                        logger.info(f"Fetching {loc_name} from {fetch_start} to {fetch_end}")
                        try:
                            df = fetch_stats_ndsi_mpc(BBOX, fetch_start, fetch_end, logger)
                            if df is None or df.empty:
                                logger.warning(f"No data returned for {loc_name} range {fetch_start} to {fetch_end}")
                                continue

                            table = _df_to_pa_table(df, loc_name)
                            if table is None or table.num_rows == 0:
                                logger.warning(f"No rows after conversion for {loc_name} {fetch_start}..{fetch_end}")
                                continue

                            CHUNK_THRESHOLD = 50000
                            chunk_size = 10000 if table.num_rows > CHUNK_THRESHOLD else table.num_rows

                            for batch in table.to_batches(max_chunksize=chunk_size):
                                yield batch

                            fetched_any = True

                        except Exception as e:
                            logger.error(f"❌ Failed fetching {loc_name} range {fetch_start} to {fetch_end}: {e}")
                else:
                    logger.info(f"{loc_name} is up-to-date")
                    state["run_dates"][loc_name] = []

                state["location_status"][loc_name] = "success" if fetched_any else ("no_data" if ranges else "skipped")

            except Exception as e:
                logger.error(f"❌ Unexpected failure for {loc_name}: {e}")
                state["run_dates"][loc_name] = []
                state["location_status"][loc_name] = "failed"

    return ice_indices_resource

# --------------------------------- Main -------------------------------------

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    pipeline = dlt.pipeline(
        pipeline_name="ice_quality_ndsi",
        destination="motherduck",
        dataset_name="spectral",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False,
    )

    locations_with_data = set()
    try:
        dataset = pipeline.dataset()["ice_indices"].df()
        if dataset is not None:
            non_empty_locations = dataset["location"].unique()
            locations_with_data = set(str(loc) for loc in non_empty_locations)
    except PipelineNeverRan:
        logger.warning("⚠️ No previous runs found for this pipeline. Assuming first run.")
    except DatabaseUndefinedRelation:
        logger.warning("⚠️ Table Doesn't Exist. Assuming truncation.")
    except ValueError as ve:
        logger.warning(f"⚠️ ValueError: {ve}. Assuming first run or empty dataset.")

    source = sentinel_source(logger, locations_with_data)
    try:
        load_info = pipeline.run(source)
        outcome_data = source.state.get("ice_indices", {})
        logger.info("Weather State Metadata:\n" + json.dumps(outcome_data, indent=2, default=json_converter))
        logger.info(f"✅ Pipeline run completed: {load_info}")
    except Exception as e:
        logger.error(f"❌ Pipeline run failed: {e}")
