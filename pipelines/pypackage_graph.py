import logging
from pathlib import Path
from project_path import get_project_paths
from dotenv import load_dotenv
import requests
import re
import json

# Load environment variables and set DLT config
paths = get_project_paths()

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


COMMON_PACKAGES = {
    # Core packaging and distribution
    "setuptools", "pip", "wheel", "build", "pyproject", "pyproject-toml", "hatch", "flit", "meson", "ninja",
    "pybind11", "cython", "pyupgrade", "check-manifest", "twine", "conda-lock", "virtualenv",
    # Metadata, typing, and compatibility
    "six", "attrs", "packaging", "typing-extensions", "importlib-metadata", "importlib_metadata", "importlib-resources", "importlib_resources", "zipp",
    "certifi", "pluggy", "pyarrow", "fsspec", "grpcio",
    # Common utility
    "pathlib", "decorator", "orderly-set", "simplejson", "ujson", "jsonpickle", "idna", "chardet", "charset-normalizer",
    "colorama", "colorlog", "tabulate", "adbc-driver-postgresql", "adbc-driver-sqlite", "adbc-driver-manager",
    "bson", "ecdsa", "feedparser", "gmpy2", "cycler", "fonttools", "kiwisolver", "pyqt5", "qtpy", "zstandard", "dataframe-api-compat",
    "fastparquet", "bottleneck", "numba", "numexpr", "xarray", "s3fs", "gcsfs", "pandas-gbq", "odfpy", "openpyxl", "python-calamine",
    "pyxlsb", "xlrd", "xlsxwriter", "tables", "pyreadstat", "mashumaro", "msgpack", "pathspec",
    "brotli", "brotlicffi", "h2", "pysocks", "pyparsing", "pygments", "markdown", "markdown2", "markdown-it-py",
    "jinja2", "markupsafe", "agate", "babel", "leather", "parsedatetime", "python-slugify", "text-unidecode", "unidecode", "pytimeparse", "pyicu",
    "cssselect", "lxml", "html5lib", "beautifulsoup4", "lxml_html_clean", "backports.zoneinfo", "backports.cached-property", "cached-property", "typing",
    "jsonschema", "jsonschema-specifications", "referencing", "rpds-py", "fqdn", "isoduration", "arrow", "jsonpointer", "rfc3339-validator", "rfc3987",
    "uri-template", "webcolors", "types-pyyaml", "types-jinja2", "types-markupsafe", "types-jsonschema", "types-protobuf", "types-python-dateutil",
    "types-requests", "types-pywin32", "types-pandas", "types-setuptools", "types-urllib3", "types-certifi", "types-attrs", "types-pillow",
    "types-psutil", "types-requests", "types-pyyaml", "types-pyarrow", "types-pyparsing", "types-pyqt5", "types-tabulate", "types-toml",
    "types-tomli", "types-tomlkit", "types-ujson", "types-zipp", "types-xmltodict", "types-xmltodict", "types-pytest", "types-flask",
    "types-werkzeug", "types-itsdangerous", "types-click", "types-cryptography", "types-ecdsa", "types-requests-toolbelt", "types-psycopg2",
    "types-pymysql", "types-sqlalchemy", "types-boto3", "types-botocore", "types-awscrt", "types-bson", "types-feedparser", "types-gmpy2",
    "types-cython", "types-html5lib", "types-lxml", "types-beautifulsoup4",
    "types-pyarrow", "types-pyqt5", "types-pytest", "types-pywin32", "types-requests", "types-sqlalchemy", "types-toml", "types-tomli",
    "types-tomlkit", "types-ujson", "types-zipp", "types-xmltodict", "types-xmltodict"
}

TEST_DEV_DOC_PACKAGES = {
    # Test
    "pytest", "pytest-cov", "pytest-xdist", "pytest-mock", "pytest-asyncio", "pytest-instafail", "pytest-sugar", "pytest-console-scripts",
    "pytest-rerunfailures", "pytest-timeout", "tox", "hypothesis", "coverage", "ddt", "nox", "ipdb", "pyfakefs", "pretend",
    # Dev
    "black", "mypy", "pre-commit", "flake8", "isort", "ruff", "pycodestyle", "autoflake", "pyright", "pylance", "build", "pipdeptree",
    "bump2version", "towncrier", "bandit", "interrogate", "check-wheel-contents", "pipdeptree", "pyupgrade", "conda-lock", "virtualenv",
    # Lint/format
    "flake8-annotations", "flake8-bandit", "flake8-bugbear", "flake8-commas", "flake8-comprehensions", "flake8-continuation", "flake8-datetimez",
    "flake8-import-order", "flake8-literal", "flake8-modern-annotations", "flake8-noqa", "flake8-requirements", "flake8-typechecking-import",
    "flake8-use-fstring", "pep8-naming",
    # Doc
    "sphinx", "sphinx_rtd_theme", "myst-parser", "mkdocs", "mkdocs-jupyter", "mkdocs-material", "mkdocstrings", "pdoc", "furo", "numpydoc",
    "docutils", "readme-renderer", "rst.linker", "pydata-sphinx-theme", "sphinx-theme-builder", "sphinx-gallery", "sphinx-book-theme",
    "sphinx-design", "sphinx-copybutton", "sphinx-lint", "sphinx-prompt", "sphinx-remove-toctrees", "sphinx-togglebutton", "sphinx-favicon",
    "sphinx-issues", "sphinx-autoapi", "sphinxext-opengraph", "sphinxcontrib-youtube", "sphinxcontrib-bibtex", "sphinxcontrib-sass",
    # Jupyter/Notebook
    "jupyter", "jupyterlab", "jupyterlab-server", "jupyter-server", "jupyter-client", "jupyter-console", "jupyter-core", "nbconvert",
    "nbformat", "nbdime", "notebook", "ipywidgets", "ipykernel", "jupytext", "nbsphinx", "jupyter_sphinx", "jupyterlite-sphinx",
    # Misc dev/doc
    "ablog", "plotly", "memory-profiler", "joblib", "statsmodels", "seaborn", "sympy", "alabaster", "bokeh", "coconut", "altair",
    "beautifulsoup4", "pygments", "graphviz", "pytest", "pytest-cov", "pytest-xdist", "pytest-mock", "pytest-asyncio", "pytest-instafail",
    "pytest-sugar", "pytest-console-scripts", "pytest-rerunfailures", "pytest-timeout"
}

def is_test_dev_doc(dep_name):
    # Lowercase match for robustness
    name = dep_name.lower()
    if name in TEST_DEV_DOC_PACKAGES:
        return True
    # Heuristic: skip if name contains test/dev/doc keywords
    for kw in ["test", "doc", "dev", "coverage"]:
        if kw in name:
            return True
    return False

def crawl(package, parent, depth, seen, nodes, flat_table, edge_set):
    if depth > MAX_DEPTH or package in seen:
        return
    seen.add(package)
    info = get_package_info(package)
    if not info:
        return
    node = {
        "pythonpackage_id": package.lower(),
        "name": info["name"].lower() if info["name"] else package.lower(),
        "summary": info["summary"],
        "upload_time": info["upload_time"],
        "downloads": info["downloads"],
        "depth": depth,
        "color": COLOURS[depth if depth < len(COLOURS) else -1]
    }
    nodes[package] = node
    for dep in info["requires_dist"]:
        dep_name = parse_requirement(dep).lower()
        if dep_name in COMMON_PACKAGES or is_test_dev_doc(dep_name):
            continue  # Skip common/low-value packages and test/dev/doc packages
        edge_key = (node["name"], dep_name)
        if dep_name and dep_name not in seen and edge_key not in edge_set:
            flat_table.append({
                "pythonpackage_id": package.lower(),
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


# ...existing code...

def main():
    import sys
    if len(sys.argv) > 1:
        # Allow user to specify a single package as a command-line argument
        requirements = [parse_requirement(sys.argv[1])]
        logger.info(f"Running for single package: {requirements[0]}")
    else:
        with REQUIREMENTS_PATH.open() as f:
            requirements = [parse_requirement(line) for line in f if line.strip() and not line.startswith("#")]
        logger.info("Running for all packages in requirements.txt")

    nodes = {}
    flat_table = []
    seen = set()
    edge_set = set()
    for pkg in requirements:
        crawl(pkg, None, 0, seen, nodes, flat_table, edge_set)
        
        
    # Write Draw.io CSV config and data to a .txt file
    csv_path = Path("drawio_pypackage_graph.txt")
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("""##
## Configuration for Draw.io CSV Import
#
# label: %name%<br><i style="color:gray;">%summary%</i><br><small>Updated: %upload_time%<br>Depth: %depth%</small>
# style: shape=rectangle;html=1;whiteSpace=wrap;rounded=1;fillColor=%color%;strokeColor=#000000;strokeWidth=1;fontColor=#000000;
# connect: {"from": "target_node", "to": "source_name", "style":"endArrow=classic;endFill=1;html=1;fontSize=10;"}
# identity: pythonpackage_id
# namespace: csvimport-
# layout: auto
# nodespacing: 30
# edgespacing: 40
#
## ---- CSV starts below ----
pythonpackage_id,name,summary,upload_time,depth,color,source_name,target_node
""")
        for row in flat_table:
            summary = (row["summary"] or "").replace('"', '""')
            if ',' in summary or '"' in summary:
                summary = f'"{summary}"'
            f.write(f'{row["pythonpackage_id"]},{row["name"]},{summary},{row["upload_time"]},{row["depth"]},{row["color"]},{row["source_name"]},{row["target_node"]}\n')
    logger.info(f"Draw.io CSV config written to: {csv_path.resolve()}")


if __name__ == "__main__":
    main()