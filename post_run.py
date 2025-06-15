import json
from datetime import datetime
import dlt
from dotenv import load_dotenv
import logging
from project_path import get_project_paths, set_dlt_env_vars
import sys
import os


paths = get_project_paths()
set_dlt_env_vars(paths)

DLT_PIPELINE_DIR = paths["DLT_PIPELINE_DIR"]
ENV_FILE = paths["ENV_FILE"]
DBT_RUN_RESULTS_DIR = paths["DBT_RUN_RESULTS_DIR"]

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

def summarize_results(results: list) -> dict:
    """Aggregate dbt results into a single summary row."""
    n_models = sum(1 for r in results if r.get("unique_id", "").startswith("model."))
    n_tests = sum(1 for r in results if r.get("unique_id", "").startswith("test."))
    tests = [r for r in results if r.get("unique_id", "").startswith("test.")]
    models = [r for r in results if r.get("unique_id", "").startswith("model.")]
    total_tests = len(tests)
    total_models = len(models)
    passed_tests = sum(1 for r in tests if r.get("status") == "pass")
    successful_models = sum(1 for r in models if r.get("status") == "success")
    thread_counts = {"Thread-1 (worker)": 0, "Thread-2 (worker)": 0, "Thread-3 (worker)": 0,
                     "Thread-4 (worker)": 0, "Thread-5 (worker)": 0, "Thread-6 (worker)": 0}
    for r in models:
        thread = r.get("thread_id")
        if thread in ("Thread-1 (worker)", "Thread-2 (worker)", "Thread-3 (worker)",
                      "Thread-4 (worker)", "Thread-5 (worker)", "Thread-6 (worker)"):
            thread_counts[thread] += 1
    total_model_threads = sum(thread_counts.values())
    percent_tests_passed = (passed_tests / total_tests * 100) if total_tests else 0
    percent_models_success = (successful_models / total_models * 100) if total_models else 0
    percent_thread_1 = (thread_counts["Thread-1 (worker)"] / total_model_threads * 100) if total_model_threads else 0
    percent_thread_2 = (thread_counts["Thread-2 (worker)"] / total_model_threads * 100) if total_model_threads else 0
    percent_thread_3 = (thread_counts["Thread-3 (worker)"] / total_model_threads * 100) if total_model_threads else 0
    percent_thread_4 = (thread_counts["Thread-4 (worker)"] / total_model_threads * 100) if total_model_threads else 0
    percent_thread_5 = (thread_counts["Thread-5 (worker)"] / total_model_threads * 100) if total_model_threads else 0
    percent_thread_6 = (thread_counts["Thread-6 (worker)"] / total_model_threads * 100) if total_model_threads else 0
    total_duration = sum(r.get("execution_time", 0) or 0 for r in results)
    return {
        "executed_at": datetime.now().isoformat(),
        "n_models": n_models,
        "n_tests": n_tests,
        "percent_tests_passed": percent_tests_passed,
        "percent_models_success": percent_models_success,
        "percent_thread_1": percent_thread_1,
        "percent_thread_2": percent_thread_2,
        "percent_thread_3": percent_thread_3,
        "percent_thread_4": percent_thread_4,
        "percent_thread_5": percent_thread_5,
        "percent_thread_6": percent_thread_6,
        "total_duration_seconds": total_duration
    }

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

        # Summarize results
        summary = summarize_results(results["results"])

        # Load to destination
        pipeline = dlt.pipeline(
            pipeline_name="dbt_audit_log",
            destination=os.getenv("DLT_DESTINATION", "motherduck"),
            dataset_name="main",
            pipelines_dir=str(DLT_PIPELINE_DIR),
            dev_mode=False
        )

        load_info = pipeline.run(
            [summary],
            table_name="summary_audit_log",
            write_disposition="append"
        )

        logger.info("Successfully loaded summary audit log")
        logger.debug(f"Load info: {load_info}")
        return 0

    except Exception as e:
        logger.error(f"Failed to process audit logs: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())