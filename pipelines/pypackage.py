import requests
import logging 

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

package = "requests"
resp = requests.get(f"https://pypi.org/pypi/{package}/json")
data = resp.json()
logger.info(f"Fetched data for package: {data}")
logger.info(f"Fetched data for package: {package}")
deps = data["info"].get("requires_dist", [])
logger.info(f"Dependencies for {package}: {deps}")