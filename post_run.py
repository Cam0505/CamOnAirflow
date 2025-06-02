import json
from datetime import datetime
import dlt
from dotenv import load_dotenv
import logging
from pathlib import Path
from path_config import DBT_RUN_RESULTS_DIR, DLT_PIPELINE_DIR, ENV_FILE
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def extract_model_type_and_node_name(result: dict) -> tuple:
    """
    Returns a tuple (model_type, node_name) according to:
    - If unique_id starts with "test", node_name is camonairflow.schema.table (parsed from unique_id)
    - If unique_id starts with "model", node_name is the relation_name (stripped of quotes)
    """
    unique_id = result.get("unique_id", "")
    if unique_id.startswith("test."):
        # Example: test.camonairflow.not_null_base_gsheets_finance_id.0d75c08608
        parts = unique_id.split(".")
        # Try to extract as camonairflow.<test_name>.<table>
        if len(parts) >= 4:
            project = parts[1]
            # Try to extract table from test name (e.g., not_null_base_gsheets_finance_id)
            test_name = parts[2]
            table = "_".join(parts[3:-1]) if len(parts) > 4 else parts[3]
            node_name = f"{project}.{test_name}.{table}"
        else:
            node_name = unique_id
        model_type = "test"
    elif unique_id.startswith("model."):
        relation = result.get("relation_name")
        node_name = relation.replace('"', '') if relation else unique_id
        model_type = "model"
    else:
        model_type = "unknown"
        node_name = unique_id
    return model_type, node_name


def validate_results(results: dict) -> bool:
    """Validate the structure of dbt run results"""
    required_keys = {"results", "metadata", "elapsed_time"}
    return all(key in results for key in required_keys)

def process_result(result: dict) -> dict:
    """Process individual dbt result into audit log format"""
    try:
        model_type, node_name = extract_model_type_and_node_name(result)
        return {
            "model_type": model_type,
            "run_at": datetime.now().isoformat(),
            "duration_seconds": result.get("execution_time"),
            "status": result["status"],
            "message": result.get("message", ""),
            "thread_id": result.get("thread_id"),
            "node_name": node_name
        }
    except KeyError as e:
        logger.warning(f"Skipping malformed result - missing key: {e}")
        return {}

def main():
    try:
        # Load environment
        load_dotenv(dotenv_path=ENV_FILE)
        
        # Verify results file exists
        if not DBT_RUN_RESULTS_DIR.exists():
            logger.error(f"DBT run results file not found at {DBT_RUN_RESULTS_DIR}")
            return 1

        # Load results
        with open(DBT_RUN_RESULTS_DIR) as f:
            results = json.load(f)

        # Validate results structure
        if not validate_results(results):
            logger.error("Invalid dbt run results structure")
            return 1

        # Process results
        audit_logs = [log for log in 
                     (process_result(r) for r in results["results"]) 
                     if log is not None]

        if not audit_logs:
            logger.info("No successful model runs to log")
            return 0

        # Load to destination
        pipeline = dlt.pipeline(
            pipeline_name="dbt_audit_log",
            destination=os.getenv("DLT_DESTINATION", "motherduck"),
            dataset_name="main",
            pipelines_dir=str(DLT_PIPELINE_DIR),
            dev_mode=False
        )

        load_info = pipeline.run(
            audit_logs,
            table_name="python_audit_log",
            write_disposition="append"
        )
        
        logger.info(f"Successfully loaded {len(audit_logs)} audit logs")
        logger.debug(f"Load info: {load_info}")
        return 0

    except Exception as e:
        logger.error(f"Failed to process audit logs: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())