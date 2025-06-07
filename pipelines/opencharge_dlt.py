import dlt
from project_path import get_project_paths, set_dlt_env_vars
from dotenv import load_dotenv
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
import logging
import os
from typing import Iterator, Dict

# Load environment variables and set DLT config
paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
DBT_DIR = paths["DBT_DIR"]

load_dotenv(dotenv_path=ENV_FILE) 

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OPENCHARGE_API_KEY = os.getenv("OPENCHARGE_API_KEY")
BASE_URL = "https://api.openchargemap.io/v3/poi/"

@dlt.source
def opencharge_source(logger: logging.Logger, db_count: int = -1):
    """
    DLT source for Open Charge Map station data.
    Fetches all charging stations globally (or by country if specified).
    If dev_limit is set, only yields up to that many stations.
    """
    @dlt.resource(write_disposition="merge", primary_key="id", name="opencharge_stations")
    def stations() -> Iterator[list[Dict]]:
        state = dlt.current.source_state().setdefault("opencharge", {
            "seen_keys": [],
            'last_run_Status': None,
            "max_date_seen": None
        })

        seen_keys = set(state.setdefault("seen_keys", []))
        state["seen_keys"] = list(seen_keys)
        new_keys = set()

        # Use max_date_seen to filter API requests if this is not the first run
        params = {
            "output": "json",
            "countrycode": "AU",
            "key": OPENCHARGE_API_KEY,
        }
        if db_count != 0 and state.get("max_date_seen"):
            params["modifiedsince"] = state["max_date_seen"]
            logger.info(f"Using date filter: modifiedsince={state['max_date_seen']}")
        else:
            logger.info("No date filter applied (first run or no max_date_seen in state).")
        
        client = RESTClient(
            base_url=BASE_URL,
            paginator=OffsetPaginator(
                limit_param="maxresults",
                offset_param="offset",
                total_path=None,
                offset=0,
                limit=500,
                maximum_offset=5500,
                stop_after_empty_page=True
            ),
            headers={"Accept": "application/json"}
        )

        batch = []
        batch_size = 1500
        latest_date = state.get("max_date_seen")
        page_count = 0

        try:
            for page in client.paginate(params=params, method="GET"):
                if not page:
                    logger.info("No more results, stopping pagination.")
                    break
                page_count += 1

                logger.info(f"Fetched page {page_count} with {len(page)} records.")

                page_new_keys = 0
                for item in page:
                    address = item.get("AddressInfo", {}) or {}
                    operator = item.get("OperatorInfo", {}) or {}
                    usage_type = item.get("UsageType", {}) or {}
                    status = item.get("StatusType", {}) or {}

                    row = {
                        "id": item.get("ID"),
                        "name": address.get("Title"),
                        "country": address.get("Country", {}).get("Title"),
                        "countrycode": address.get("Country", {}).get("ISOCode"),
                        "region": address.get("StateOrProvince"),
                        "town": address.get("Town"),
                        "address": address.get("AddressLine1"),
                        "lat": address.get("Latitude"),
                        "lon": address.get("Longitude"),
                        "operator": operator.get("Title"),
                        "usage_type": usage_type.get("Title"),
                        "connection_types": [
                            {
                                "ID": item.get("ID"),
                                "ConnectionType": (c.get("ConnectionType") or {}).get("Title"),
                                "ConnectionTypeID": c.get("ConnectionTypeID"),
                                "Level": (c.get("Level") or {}).get("Title"),
                                "LevelID": c.get("LevelID"),
                                "PowerKW": c.get("PowerKW"),
                                "CurrentType": (c.get("CurrentType") or {}).get("Title"),
                                "CurrentTypeID": c.get("CurrentTypeID"),
                                "Quantity": c.get("Quantity"),
                                "StatusType": (c.get("StatusType") or {}).get("Title"),
                                "StatusTypeID": c.get("StatusTypeID"),
                                "Comments": c.get("Comments"),
                            }
                            for c in item.get("Connections", [])
                        ],
                        "status": status.get("Title"),
                        "last_updated": item.get("DateLastStatusUpdate"),
                        "NumberOfPoints": item.get("NumberOfPoints")
                    }
                    key = f"{row['id']}|{row.get('last_updated', '').lower()}"
                    if key in seen_keys and db_count != 0:
                        continue
                    page_new_keys += 1
                    logger.info(f"Processing new key: {key}")
                    new_keys.add(key)
                    batch.append(row)

                    # Track the latest date seen for incremental loads
                    last_updated = row.get("last_updated")
                    if last_updated and (not latest_date or last_updated > latest_date):
                        latest_date = last_updated

                    if len(batch) >= batch_size:
                        logger.info(f"Yielding batch of {len(batch)} stations.")
                        yield batch
                        batch = []

                if page_new_keys == 0:
                    logger.info(f"Page {page_count} contained only already-seen records (filtered by date or deduplication).")

            if batch:
                yield batch

            logger.info(f"Total pages fetched: {page_count}, Total new keys: {len(new_keys)}")

            # update persistent state with new keys and max_date_seen
            if new_keys:
                state["seen_keys"] = list(seen_keys.union(new_keys))
                state["last_run_Status"] = "success"
                if latest_date:
                    state["max_date_seen"] = latest_date
            else:
                state["last_run_Status"] = "skipped"
        except Exception as e:
            logger.error(f"Error during Open Charge Map extraction: {e}", exc_info=True)
            state["last_run_Status"] = "failed"
            raise

    return stations

# Usage in __main__:
if __name__ == "__main__":
    logger.info("Starting Open Charge Map DLT pipeline run.")
    pipeline = dlt.pipeline(
        pipeline_name="open_charge_pipeline",
        destination=os.getenv("DLT_DESTINATION"),
        dataset_name="open_charge",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )
    row_counts = None
    row_counts_dict = {}
    try:
        row_counts = pipeline.dataset().row_counts().df()
        if row_counts is not None:
            row_counts_dict = dict(
            zip(row_counts["table_name"], row_counts["row_count"]))
        logger.info(
            f"📊 Row counts for existing tables: {row_counts_dict}")
    except PipelineNeverRan:
        logger.warning(
            "⚠️ No previous runs found for this pipeline. Assuming first run.")
    except DatabaseUndefinedRelation:
        logger.warning(
            "⚠️ Table Doesn't Exist. Assuming truncation.")

    source = opencharge_source(logger, row_counts_dict.get("opencharge_stations", -1))
    
    try:
        pipeline.run(source)
        run_status = source.state.get('opencharge', {}).get('last_run_Status', [])
        if run_status == "skipped":
            logger.info(
                "⏭️ All resources skipped — no data loaded.")
            # return False
        elif run_status == "success":
            logger.info(
                f"✅ New data to merge — {len(source.state['opencharge']['seen_keys'])} new keys found.")
            # return True
        else:
            logger.info(
                f"❌ DLT pipeline run failed with status: {run_status}")
            # return False

    except Exception as e:
        logger.error(f"❌ Pipeline run failed: {e}")
        raise
        # return False
