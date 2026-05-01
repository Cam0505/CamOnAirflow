"""
NZ Elevation LiDAR Pipeline — ski_resort_elevation_1m
======================================================

Ingests 1 m LiDAR elevation data from the AWS open-data bucket
``s3://nz-elevation`` (Cloud Optimised GeoTIFFs, LERC-compressed) into
MotherDuck, flattening raster pixels into a tabular schema optimised for
fast GPS-coordinate lookups.

**Running the pipeline**
    python pipelines/nz_elevation_lidar_dlt.py

**Incremental loading**
    dlt source state tracks which ``(resort, source_file)`` pairs have
    already been written.  On every subsequent run those files are skipped
    completely, so the pipeline is safe to run on a schedule without
    duplicating data.

    To force a full reload (e.g. after refreshing the bounding-box view or
    adding new COG tiles to the catalogue):
        dlt pipeline nz_elevation_lidar_pipeline drop

**S3 access**
    The bucket is a public AWS Registry of Open Data dataset.
    ``AWS_NO_SIGN_REQUEST=YES`` is injected into the GDAL environment so
    no AWS credentials are required.

**CRS pipeline**
    Native pixel coordinates (EPSG:2193 / NZTM2000) are transformed to
    EPSG:4326 (WGS 84 lon/lat) *before* yielding to dlt, so the final
    MotherDuck table stores standard GPS coordinates.
"""

import os
import json
import logging
import threading
import concurrent.futures
import urllib.request

import duckdb
import numpy as np
import pyarrow as pa
import dlt
from dlt.destinations.exceptions import DatabaseUndefinedRelation
import rasterio
import rasterio.windows
from rasterio.windows import from_bounds
from rasterio.transform import xy as rasterio_xy
try:
    from rasterio.errors import WindowError as _RasterioWindowError
except ImportError:
    _RasterioWindowError = Exception  # type: ignore[assignment,misc]
from pyproj import Transformer
from dotenv import load_dotenv

from project_path import get_project_paths, set_dlt_env_vars


# ---------------------------------------------------------------------------
# Environment setup — mirrors the pattern used in Ski_runs.py / snowfall_dlt.py
# ---------------------------------------------------------------------------
paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
load_dotenv(dotenv_path=ENV_FILE)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline configuration constants
# ---------------------------------------------------------------------------

DESTINATION_DATABASE = "camonairflow"

# Fully-qualified name for the view that supplies resort bounding boxes.
# Columns: resort (str), min_x (float), min_y (float), max_x (float), max_y (float).
# Coordinates are EPSG:2193 Easting / Northing.
BBOX_VIEW = f"{DESTINATION_DATABASE}.main.nz_resort_lidar_bboxes"

# dlt dataset / table names
DATASET_NAME = "nz_lidar"
TARGET_TABLE = "ski_resort_elevation_1m"

# Source CRS (NZTM2000) → target CRS (WGS84 lon/lat).
CRS_SOURCE = "EPSG:2193"
CRS_TARGET = "EPSG:4326"
RESOLUTION_M = 1  # Source pixel resolution in metres

# Elevation values at or below this are treated as NoData and excluded.
NODATA_THRESHOLD = -100.0

# Number of pixels per side when reading the COG in sub-windows.
# 1024 × 1024 ≈ 1 M pixels per tile — adjust down to reduce peak RAM usage.
TILE_SIZE = 1024

# ---------------------------------------------------------------------------
# Rasterio's Window namedtuple has incorrect Pylance stubs in some installed
# versions — wrap construction in a helper so the ignore is isolated.
def _make_window(
    col_off: float, row_off: float, width: float, height: float
) -> rasterio.windows.Window:
    return rasterio.windows.Window(col_off, row_off, width, height)  # type: ignore[call-arg]


# Coordinate transformer — initialised once at module level for reuse.
# always_xy=True ensures (lon, lat) ordering rather than (lat, lon).
# ---------------------------------------------------------------------------
_TRANSFORMER = Transformer.from_crs(CRS_SOURCE, CRS_TARGET, always_xy=True)

# ---------------------------------------------------------------------------
# NZ elevation STAC catalog — root catalog covering all regional dem_1m surveys
# ---------------------------------------------------------------------------
_NZ_CATALOG_URL = "https://nz-elevation.s3.ap-southeast-2.amazonaws.com/catalog.json"
_NZ_ELEVATION_HTTP_BASE = "https://nz-elevation.s3.ap-southeast-2.amazonaws.com/"

# Tile index: lazily built on first call to get_s3_urls_for_bbox().
# Each entry is a tuple of (bbox_wgs84, s3_url) where
#   bbox_wgs84 = [lon_min, lat_min, lon_max, lat_max]
_TILE_INDEX: list[tuple[list, str]] | None = None
_TILE_INDEX_LOCK = threading.Lock()

# ---------------------------------------------------------------------------
# PyArrow output schema — used for all yielded record batches.
# ---------------------------------------------------------------------------
_ARROW_SCHEMA = pa.schema([
    pa.field("resort_name", pa.string()),
    pa.field("lon", pa.float64()),
    pa.field("lat", pa.float64()),
    pa.field("x_nztm", pa.int32()),
    pa.field("y_nztm", pa.int32()),
    pa.field("elevation_m", pa.float32()),
    pa.field("resolution_m", pa.int32()),
    pa.field("source_file", pa.string()),
])


# ---------------------------------------------------------------------------
# S3 URL discovery — NZ elevation STAC tile index
# ---------------------------------------------------------------------------

def _build_tile_index() -> list[tuple[list, str]]:
    """Fetch all NZ regional 1 m DEM STAC collections and build an in-memory
    spatial index.

    Fetches the root catalog to discover all ``dem_1m`` collection URLs across
    every regional survey (Canterbury, Otago, etc.), then fetches all
    collection and item JSONs in parallel.

    Returns:
        List of ``(bbox_wgs84, s3_url)`` tuples where
        ``bbox_wgs84 = [lon_min, lat_min, lon_max, lat_max]``.
    """
    logger.info("Building NZ elevation tile index from STAC catalog ...")

    # Step 1: fetch root catalog to get all dem_1m collection URLs.
    try:
        with urllib.request.urlopen(_NZ_CATALOG_URL, timeout=30) as resp:
            catalog = json.loads(resp.read())
    except Exception as e:
        logger.error("Failed to fetch NZ elevation catalog: %s", e)
        return []

    collection_urls = [
        _NZ_ELEVATION_HTTP_BASE + link["href"].lstrip("./")
        for link in catalog.get("links", [])
        if link.get("rel") == "child" and "/dem_1m/" in link.get("href", "")
    ]
    logger.info("Found %d dem_1m collections in catalog", len(collection_urls))

    # Step 2: fetch every collection JSON in parallel to get item ID lists.
    def fetch_collection_items(coll_url: str) -> list[tuple[str, str]]:
        # base_url = collection URL with "collection.json" stripped.
        base_url = coll_url[: coll_url.rfind("/") + 1]
        try:
            with urllib.request.urlopen(coll_url, timeout=30) as resp:
                collection = json.loads(resp.read())
            return [
                (base_url, link["href"].lstrip("./").replace(".json", ""))
                for link in collection.get("links", [])
                if link.get("rel") == "item"
            ]
        except Exception as exc:
            logger.warning("Failed to fetch collection %s: %s", coll_url, exc)
            return []

    all_items: list[tuple[str, str]] = []  # (base_url, item_id)
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        for result in executor.map(fetch_collection_items, collection_urls):
            all_items.extend(result)

    logger.info("Found %d total tiles across all collections", len(all_items))

    # Step 3: fetch every item JSON in parallel to get bbox + S3 URL.
    def fetch_tile(base_and_id: tuple[str, str]) -> tuple[list, str] | None:
        base_url, item_id = base_and_id
        url = f"{base_url}{item_id}.json"
        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=15) as resp:
                    item = json.loads(resp.read())
                bbox = item.get("bbox")
                if not bbox or len(bbox) != 4:
                    return None
                href = item.get("assets", {}).get("visual", {}).get("href", "")
                if not href.startswith("./"):
                    return None
                s3_base = base_url.replace(_NZ_ELEVATION_HTTP_BASE, "s3://nz-elevation/")
                s3_url = s3_base + href[2:]
                return (bbox, s3_url)
            except Exception as exc:
                if attempt < 2:
                    continue
                logger.warning("Failed to fetch tile metadata for %s: %s", item_id, exc)
                return None

    index: list[tuple[list, str]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        for result in executor.map(fetch_tile, all_items):
            if result is not None:
                index.append(result)

    logger.info("Tile index built: %d tiles indexed", len(index))
    return index


def get_s3_urls_for_bbox(
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float,
) -> list[str]:
    """Return S3 URLs for the 1 m LiDAR COG tiles that cover a bounding box.

    On the first call, fetches the NZ-wide STAC collection and all tile item
    JSONs in parallel to build an in-memory spatial index.  Subsequent calls
    (for other resorts in the same run) reuse the cached index.

    Args:
        min_x: Minimum Easting in EPSG:2193.
        min_y: Minimum Northing in EPSG:2193.
        max_x: Maximum Easting in EPSG:2193.
        max_y: Maximum Northing in EPSG:2193.

    Returns:
        List of ``s3://nz-elevation/...`` URLs for all intersecting tiles.
    """
    global _TILE_INDEX

    # Build the tile index on the first call (thread-safe double-checked locking).
    if _TILE_INDEX is None:
        with _TILE_INDEX_LOCK:
            if _TILE_INDEX is None:
                _TILE_INDEX = _build_tile_index()

    # Convert EPSG:2193 bbox to WGS84 lon/lat.
    lon_min, lat_min = _TRANSFORMER.transform(min_x, min_y)
    lon_max, lat_max = _TRANSFORMER.transform(max_x, max_y)
    # pyproj may return values out of min/max order depending on CRS axis direction.
    lon_min, lon_max = min(lon_min, lon_max), max(lon_min, lon_max)
    lat_min, lat_max = min(lat_min, lat_max), max(lat_min, lat_max)

    urls = []
    for tile_bbox, s3_url in _TILE_INDEX:
        t_lon_min, t_lat_min, t_lon_max, t_lat_max = tile_bbox
        if (lon_min <= t_lon_max and lon_max >= t_lon_min
                and lat_min <= t_lat_max and lat_max >= t_lat_min):
            urls.append(s3_url)

    # Prefer regional survey tiles over the national mosaic.
    # The national mosaic lives under s3://nz-elevation/new-zealand/new-zealand/
    # and duplicates pixels that are already covered by regional collections
    # (e.g. otago/cardrona_2025, canterbury/canterbury_2020-2023).
    # If any regional tile matches the bbox, drop the national mosaic tiles
    # to avoid loading the same point twice with different source_file values.
    _NATIONAL_MOSAIC = "s3://nz-elevation/new-zealand/new-zealand/"
    regional_urls = [u for u in urls if not u.startswith(_NATIONAL_MOSAIC)]
    if regional_urls:
        return regional_urls

    return urls


# ---------------------------------------------------------------------------
# Raster extraction — stream a COG from S3, clip to bbox, flatten to rows
# ---------------------------------------------------------------------------

def _iter_raster_chunks(
    s3_url: str,
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float,
    resort_name: str,
    tile_size: int = TILE_SIZE,
):
    """Open a COG remotely via GDAL VSI and stream valid pixels as PyArrow
    RecordBatches.

    The file is *never* downloaded in full.  GDAL performs HTTP range
    requests to read only the COG internal tiles that overlap the supplied
    bounding box.

    Args:
        s3_url:      ``s3://…`` URL of the LERC-compressed COG.
        min_x/y:     Clipping bbox minimum Easting / Northing (EPSG:2193).
        max_x/y:     Clipping bbox maximum Easting / Northing (EPSG:2193).
        resort_name: Resort label written into every output row.
        tile_size:   Side length (pixels) of the processing sub-windows.

    Yields:
        ``pa.RecordBatch`` conforming to ``_ARROW_SCHEMA`` for each
        sub-window that contains at least one valid pixel.
    """
    # GDAL VSI path — translates s3:// → /vsis3/ for GDAL's S3 virtual FS
    vsi_path = s3_url.replace("s3://", "/vsis3/")
    # Include the survey path in source_file so tiles with the same filename
    # from different surveys (e.g. 2016 vs 2021 editions) are distinct.
    # Format: "{region}/{survey}/dem_1m/2193/{filename}.tiff"
    _NZ_BUCKET = "s3://nz-elevation/"
    source_file = s3_url[len(_NZ_BUCKET):] if s3_url.startswith(_NZ_BUCKET) else s3_url.rsplit("/", 1)[-1]

    with rasterio.Env(
        # No AWS credentials needed — bucket is public open data.
        # aws_unsigned=True is the rasterio-native way to set AWS_NO_SIGN_REQUEST.
        # AWS_NO_SIGN_REQUEST is also set explicitly because some rasterio builds
        # do not propagate aws_unsigned to the underlying GDAL config.
        aws_unsigned=True,
        AWS_NO_SIGN_REQUEST="YES",
        # Prevent GDAL from scanning the S3 directory for sidecar files,
        # which would generate spurious and slow LIST requests.
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
        # Coalesce adjacent HTTP range requests for faster COG tile reads.
        GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES",
        # Restrict VSI curl to .tif files to avoid accidental full-file fetches.
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif,.tiff",
    ):
        try:
            with rasterio.open(vsi_path) as src:
                logger.info(
                    "Opened COG | file=%s | crs=%s | res=(%.2f, %.2f) | size=%dx%d",
                    source_file, src.crs, src.res[0], src.res[1],
                    src.width, src.height,
                )

                # Compute the window that corresponds to the clipping bbox,
                # then clamp it to the actual raster extent to avoid out-of-
                # bounds reads when a resort straddles multiple tiles.
                raw_window = from_bounds(min_x, min_y, max_x, max_y, src.transform)
                full_extent = _make_window(0, 0, src.width, src.height)
                try:
                    window = raw_window.intersection(full_extent)
                except _RasterioWindowError:
                    logger.warning(
                        "Bounding box does not intersect raster, skipping | file=%s", source_file
                    )
                    return

                win_height = int(np.ceil(window.height))
                win_width = int(np.ceil(window.width))
                if win_height <= 0 or win_width <= 0:
                    logger.warning(
                        "Empty intersection window, skipping | file=%s", source_file
                    )
                    return

                nodata_val = src.nodata

                # Iterate over sub-windows (tiles) to keep peak memory bounded.
                # Each tile yields one PyArrow RecordBatch.
                for row_off in range(0, win_height, tile_size):
                    row_count = min(tile_size, win_height - row_off)
                    for col_off in range(0, win_width, tile_size):
                        col_count = min(tile_size, win_width - col_off)

                        tile_window = _make_window(
                            window.col_off + col_off,
                            window.row_off + row_off,
                            col_count,
                            row_count,
                        )

                        # Read the elevation band for this sub-window.
                        # rasterio performs only the HTTP range requests
                        # needed for these pixels.
                        data = src.read(1, window=tile_window)
                        tile_transform = src.window_transform(tile_window)

                        # Generate pixel-centre (X, Y) coordinates in the
                        # source CRS (EPSG:2193).
                        rows_idx, cols_idx = np.indices(data.shape)
                        xs, ys = rasterio_xy(
                            tile_transform,
                            rows_idx.flatten(),
                            cols_idx.flatten(),
                        )
                        elevs = data.flatten().astype(np.float32)

                        # Build a boolean validity mask:
                        #   1. Elevation must be above the NoData threshold.
                        #   2. Exclude the explicit NoData value reported by
                        #      the file (e.g. NaN, -9999, etc.) if present.
                        valid_mask = elevs > NODATA_THRESHOLD
                        if nodata_val is not None:
                            valid_mask &= (elevs != nodata_val)
                        if not np.any(valid_mask):
                            continue  # All pixels in this tile are NoData

                        xs_valid = np.array(xs)[valid_mask]
                        ys_valid = np.array(ys)[valid_mask]
                        elevs_valid = elevs[valid_mask]

                        # Project NZTM2000 → WGS84 (lon, lat) for the
                        # valid pixels only — avoids wasted work on NoData.
                        lons, lats = _TRANSFORMER.transform(xs_valid, ys_valid)

                        # xs_valid/ys_valid are pixel-centre coordinates in
                        # EPSG:2193 (metres). Round to nearest integer to get
                        # the exact 1 m grid cell key — used for precise joins
                        # with OSM points without WGS84 floating-point drift.
                        xs_int = xs_valid.round().astype(np.int32)
                        ys_int = ys_valid.round().astype(np.int32)

                        n = len(lons)
                        batch = pa.RecordBatch.from_arrays(
                            [
                                pa.array([resort_name] * n, type=pa.string()),
                                pa.array(lons, type=pa.float64()),
                                pa.array(lats, type=pa.float64()),
                                pa.array(xs_int, type=pa.int32()),
                                pa.array(ys_int, type=pa.int32()),
                                pa.array(elevs_valid, type=pa.float32()),
                                pa.array([RESOLUTION_M] * n, type=pa.int32()),
                                pa.array([source_file] * n, type=pa.string()),
                            ],
                            schema=_ARROW_SCHEMA,
                        )
                        yield batch

        except Exception as e:
            logger.error("Failed to process COG | file=%s | error=%s", source_file, e)
            raise


# ---------------------------------------------------------------------------
# dlt source
# ---------------------------------------------------------------------------

@dlt.source(name="nz_elevation_lidar")
def nz_lidar_source(resort_bboxes: list[dict]):
    """dlt source that yields 1 m elevation points for each resort bbox.

    Args:
        resort_bboxes: List of dicts with keys:
            resort, min_x, min_y, max_x, max_y.
    """

    @dlt.resource(
        name=TARGET_TABLE,
        write_disposition="append",
        # Column types are inferred from the PyArrow schema embedded in each
        # yielded RecordBatch, so no explicit columns hint is needed here.
    )
    def elevation_points():
        """Stream 1 m LiDAR pixels for all resort bboxes.

        Incremental strategy
        --------------------
        dlt source state stores a list called ``loaded_file_keys``.  Each
        entry is a ``"{resort}::{source_file}"`` string.  Before processing
        a file, the key is looked up in this list; if found the file is
        skipped entirely.  The key is appended to the list *after* all
        batches for that file have been yielded successfully, so a failed
        or interrupted run will retry the file on the next execution.

        This gives per-file idempotency without requiring a merge primary
        key across billions of points.  The trade-off is that a partial
        failure on a very large file could lead to duplicate rows for that
        file; a post-load ``DISTINCT`` query or a scheduled ``DELETE``
        on the partially-loaded ``source_file`` can clean this up if needed.
        """
        # Load the persisted list from dlt state and keep a set for O(1) lookup.
        state_keys_list: list[str] = dlt.current.source_state().setdefault(
            "loaded_file_keys", []
        )
        loaded_keys: set[str] = set(state_keys_list)

        total_files = 0
        skipped_files = 0
        processed_files = 0
        total_points = 0

        for row in resort_bboxes:
            resort = row["resort"]
            min_x, min_y = row["min_x"], row["min_y"]
            max_x, max_y = row["max_x"], row["max_y"]

            s3_urls = get_s3_urls_for_bbox(min_x, min_y, max_x, max_y)
            if not s3_urls:
                logger.info("No S3 URLs found for resort: %s", resort)
                continue

            for s3_url in s3_urls:
                total_files += 1
                # Include the survey path so tiles with the same filename from
                # different surveys get distinct state keys (e.g. queenstown_2016
                # vs queenstown_2021 both have CB11_10000_0503.tiff).
                _NZ_BUCKET = "s3://nz-elevation/"
                source_file = s3_url[len(_NZ_BUCKET):] if s3_url.startswith(_NZ_BUCKET) else s3_url.rsplit("/", 1)[-1]
                state_key = f"{resort}::{source_file}"

                # --- Incremental check: skip files already loaded ---
                if state_key in loaded_keys:
                    logger.info(
                        "Skipping already-loaded file | resort=%s | file=%s",
                        resort, source_file,
                    )
                    skipped_files += 1
                    continue

                logger.info(
                    "Processing COG | resort=%s | file=%s", resort, source_file
                )

                try:
                    file_points = 0
                    file_chunks = 0
                    for batch in _iter_raster_chunks(
                        s3_url, min_x, min_y, max_x, max_y, resort
                    ):
                        yield batch
                        file_chunks += 1
                        file_points += len(batch)

                    # Mark as loaded only *after* all batches are yielded.
                    # If this run is interrupted mid-file the key won't be
                    # persisted and the file will be retried next run.
                    state_keys_list.append(state_key)
                    loaded_keys.add(state_key)
                    processed_files += 1
                    total_points += file_points

                    logger.info(
                        "Completed COG | resort=%s | file=%s | chunks=%d | points=%d",
                        resort, source_file, file_chunks, file_points,
                    )

                except Exception as e:
                    logger.error(
                        "Skipping failed COG | resort=%s | file=%s | error=%s",
                        resort, source_file, e,
                    )
                    # Do NOT add to loaded_keys — will be retried next run.

        logger.info(
            "Source summary | total_files=%d | skipped=%d | processed=%d | total_points=%d",
            total_files, skipped_files, processed_files, total_points,
        )

    return [elevation_points]


# ---------------------------------------------------------------------------
# Bbox loader — queries MotherDuck for resort bounding boxes
# ---------------------------------------------------------------------------

def _load_resort_bboxes(pipeline) -> list[dict]:
    """Query MotherDuck and return rows from the nz_resort_lidar_bboxes view.

    Uses dlt's sql_client() so the connection is shared with the pipeline and
    avoids duckdb global-state conflicts from opening a second connection.
    The spatial extension is loaded before the query because the view uses
    ST_Transform / ST_Y.
    Returns an empty list if the view does not yet exist (first-run safety).
    """
    sql = f"SELECT resort, min_x, min_y, max_x, max_y FROM {BBOX_VIEW} where resort = 'Ōhau Snow Fields'"
    try:
        with pipeline.sql_client() as client:
            try:
                client.execute_sql("LOAD spatial")
            except Exception:
                client.execute_sql("INSTALL spatial; LOAD spatial;")
            result = client.execute_sql(sql)
        if not result:
            return []
        rows = []
        for row in result:
            if not row or len(row) < 5:
                continue
            rows.append({
                "resort": row[0],
                "min_x": float(row[1]),
                "min_y": float(row[2]),
                "max_x": float(row[3]),
                "max_y": float(row[4]),
            })
        logger.info("Loaded %d resort bounding boxes from %s", len(rows), BBOX_VIEW)
        return rows
    except duckdb.CatalogException as e:
        logger.warning(
            "View %s not found — no bboxes loaded.  "
            "Create the view in MotherDuck before running the pipeline. (%s)",
            BBOX_VIEW, e,
        )
        return []
    except Exception as e:
        logger.error("Failed to load resort bboxes: %s", e)
        raise


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def _sync_state_from_table(pipeline) -> None:
    """Synchronise dlt source state with what is actually present in the
    destination table.

    Queries ``DISTINCT resort_name || '::' || source_file`` from the target
    table and overwrites ``loaded_file_keys`` in the pipeline state.  This
    means truncating (or otherwise modifying) the destination table will
    automatically cause the pipeline to re-process any missing files on the
    next run, without having to manually edit the state JSON.
    """
    qualified = f"{DESTINATION_DATABASE}.{DATASET_NAME}.{TARGET_TABLE}"
    try:
        with pipeline.sql_client() as client:
            rows = client.execute_sql(
                f"SELECT DISTINCT resort_name || '::' || source_file"
                f" FROM {qualified}"
            )
        keys = [r[0] for r in rows]
    except (duckdb.CatalogException, DatabaseUndefinedRelation):
        # Table doesn't exist yet (first run or table was dropped) — start with an empty state.
        keys = []

    with pipeline.managed_state() as state:
        src_state = state.setdefault("sources", {}).setdefault("nz_elevation_lidar", {})
        old_keys = src_state.get("loaded_file_keys", [])
        src_state["loaded_file_keys"] = keys

    added = set(keys) - set(old_keys)
    removed = set(old_keys) - set(keys)
    if added or removed:
        logger.info(
            "State synced from table | +%d new keys | -%d removed keys | total=%d",
            len(added), len(removed), len(keys),
        )
    else:
        logger.info("State synced from table | %d keys unchanged", len(keys))


def run_pipeline(logger: logging.Logger, full_refresh: bool = False) -> None:
    """Create the dlt pipeline, load resort bboxes, and run the source."""
    logger.info("Starting NZ Elevation LiDAR pipeline ...")

    pipeline = dlt.pipeline(
        pipeline_name="nz_elevation_lidar_pipeline",
        destination=os.getenv("DLT_DESTINATION", "motherduck"),
        dataset_name=DATASET_NAME,
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False,
    )

    if full_refresh:
        # Truncate the destination table so reprocessed data doesn't duplicate
        # rows that were loaded under the old (filename-only) source_file key.
        qualified = f"{DESTINATION_DATABASE}.{DATASET_NAME}.{TARGET_TABLE}"
        try:
            with pipeline.sql_client() as client:
                client.execute_sql(f"DELETE FROM {qualified} WHERE TRUE")
            logger.info("Full refresh — destination table truncated.")
        except Exception as e:
            logger.info("Full refresh — table not yet created or already empty: %s", e)
        # Wipe the local state entirely so every file is reprocessed.
        with pipeline.managed_state() as state:
            state.setdefault("sources", {}).setdefault(
                "nz_elevation_lidar", {}
            )["loaded_file_keys"] = []
        logger.info("Full refresh requested — state cleared.")
    else:
        # Sync state from the destination table so a truncated table
        # automatically triggers re-processing of its files.
        _sync_state_from_table(pipeline)

    resort_bboxes = _load_resort_bboxes(pipeline)
    if not resort_bboxes:
        logger.warning(
            "No resort bounding boxes available — nothing to ingest.  "
            "Ensure the view '%s' exists in MotherDuck.",
            BBOX_VIEW,
        )
        return

    try:
        load_info = pipeline.run(nz_lidar_source(resort_bboxes))
        logger.info("Pipeline completed | %s", load_info)
    except Exception as e:
        logger.error("Pipeline run failed: %s", e)
        raise


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="NZ Elevation LiDAR pipeline")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Clear all incremental state and reload every file from scratch.",
    )
    args = parser.parse_args()
    run_pipeline(logger=logger, full_refresh=args.full_refresh)
    logger.info("NZ Elevation LiDAR pipeline run completed.")
