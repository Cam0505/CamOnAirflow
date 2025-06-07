import logging
from pathlib import Path
from project_path import get_project_paths, set_dlt_env_vars
from dotenv import load_dotenv
from dlt.sources.helpers import requests
import dlt
import os
from pathlib import Path
import re
import json

# Load environment variables and set DLT config
paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
REQUEST_CACHE_DIR = Path(paths["REQUEST_CACHE_DIR"])
REQUIREMENTS_PATH = Path(paths["REQUIREMENTS_PATH"])

load_dotenv(dotenv_path=ENV_FILE) 

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MAX_DEPTH = 3

# Colour palette for up to 10 depths + root
COLOURS = [
    "#FF5733",  # roots (requirements.txt)
    "#33C1FF", "#33FF57", "#FF33A8", "#FFC733", "#8D33FF",
    "#33FFF6", "#FF3333", "#33FFB5", "#B5FF33", "#FF8D33"
]

# Ensure cache dir exists
REQUEST_CACHE_DIR.mkdir(parents=True, exist_ok=True)

def get_package_info(package):
    cache_file = REQUEST_CACHE_DIR / f"{package}.json"
    # Try to load from cache first
    if cache_file.exists():
        try:
            with cache_file.open("r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load cache for {package}: {e}")

    url = f"https://pypi.org/pypi/{package}/json"
    resp = requests.get(url)
    if resp.status_code != 200:
        logger.warning(f"Failed to fetch {package}")
        return None
    data = resp.json()
    info = data.get("info", {})
    releases = data.get("releases", {})
    # Get latest upload time
    upload_time = None
    if releases:
        latest = sorted(releases.keys(), reverse=True)[0]
        files = releases[latest]
        if files:
            upload_time = files[0].get("upload_time")
    # Get downloads (last month) from pepy.tech API
    downloads = None
    try:
        pepy = requests.get(f"https://pepy.tech/api/v2/projects/{package}").json()
        downloads = pepy.get("downloads", {}).get("last_month")
    except Exception:
        pass
    result = {
        "name": info.get("name"),
        "summary": info.get("summary"),
        "upload_time": upload_time,
        "downloads": downloads,
        "requires_dist": info.get("requires_dist") or []
    }
    # Save to cache
    try:
        with cache_file.open("w") as f:
            json.dump(result, f)
    except Exception as e:
        logger.warning(f"Failed to write cache for {package}: {e}")
    return result

def parse_requirement(line):
    # Remove comments and environment markers
    line = line.split("#")[0].split(";")[0].strip()
    if not line:
        return ""
    # Remove extras and version specifiers
    for sep in ["[", "(", "=", "<", ">", "~"]:
        line = line.split(sep)[0]
    # Remove any non-alphanumeric, underscore, or dash characters
    line = re.sub(r"[^A-Za-z0-9_\-\.]+", "", line)
    return line.strip()

def crawl(package, parent, depth, seen, nodes, flat_table, edge_set):
    if depth > MAX_DEPTH or package in seen:
        return
    seen.add(package)
    info = get_package_info(package)
    if not info:
        return
    node = {
        "pythonpackage_id": package,
        "name": info["name"],
        "summary": info["summary"],
        "upload_time": info["upload_time"],
        "downloads": info["downloads"],
        "depth": depth,
        "color": COLOURS[depth if depth < len(COLOURS) else -1]
    }
    nodes[package] = node
    for dep in info["requires_dist"]:
        dep_name = parse_requirement(dep)
        edge_key = (node["name"], dep_name)
        if dep_name and dep_name not in seen and edge_key not in edge_set:
            flat_table.append({
                "pythonpackage_id": package,
                "name": node["name"],
                "summary": node["summary"],
                "upload_time": node["upload_time"],
                "downloads": node["downloads"],
                "depth": node["depth"],
                "color": node["color"],
                "source_name": node["name"],
                "target_node": dep_name
            })
            edge_set.add(edge_key)
            crawl(dep_name, package, depth + 1, seen, nodes, flat_table, edge_set)
    if parent is None and not info["requires_dist"]:
        flat_table.append({
            "pythonpackage_id": package,
            "name": node["name"],
            "summary": node["summary"],
            "upload_time": node["upload_time"],
            "downloads": node["downloads"],
            "depth": node["depth"],
            "color": node["color"],
            "source_name": node["name"],
            "target_node": ""
        })

def main():
    import sys
    if len(sys.argv) > 1:
        # Allow user to specify a single package as a command-line argument
        requirements = [parse_requirement(sys.argv[1])]
        logger.info(f"Running for single package: {requirements[0]}")
    else:
        with REQUIREMENTS_PATH.open() as f:
            requirements = [parse_requirement(line) for line in f if line.strip() and not line.startswith("#")]
        logger.info(f"Running for all packages in requirements.txt")

    nodes = {}
    flat_table = []
    seen = set()
    edge_set = set()
    for pkg in requirements:
        crawl(pkg, None, 0, seen, nodes, flat_table, edge_set)

    # Write to MotherDuck using dlt
    pipeline = dlt.pipeline(
        pipeline_name="pypackage_graph",
        destination=os.getenv("DLT_DESTINATION"),
        dataset_name="main",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )
    pipeline.run(flat_table, table_name="pypackage_drawio", write_disposition="replace")
    logger.info(f"Draw.io compatible table loaded: {len(flat_table)} rows")

if __name__ == "__main__":
    main()