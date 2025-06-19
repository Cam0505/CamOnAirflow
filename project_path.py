# camonprefect/path_config.py
from pathlib import Path
import os
import sys


def get_project_paths():
    """Resolves and returns all standardized project paths as a dict, working from any directory."""
    # Start with specific paths to check
    search_paths = [
        Path("/workspaces/camonairflow"),  # Lower case devcontainer path
        Path("/workspaces/CamOnAirFlow"),  # Upper case devcontainer path
        Path(__file__).parent,              # Directory of this file
        Path.cwd()                          # Current working directory
    ]
    
    # Add parent directories of current directory (up to 3 levels)
    cwd = Path.cwd()
    for i in range(1, 4):  # Check up to 3 parent directories
        if i <= len(cwd.parents):
            search_paths.append(cwd.parents[i-1])
    
    # Remove None entries
    search_paths = [p for p in search_paths if p is not None]
    
    # Debug info
    # print(f"Search paths: {[str(p) for p in search_paths]}")
    
    for path in search_paths:
        try:
            # print(f"Checking {path}")
            # contents = list(path.iterdir()) if path.exists() else []
            # dir_names = [d.name for d in contents if d.is_dir()]
            # print(f"  Contents: {dir_names}")
            
            # Check if this path contains dbt and pipelines directories
            if (path / "dbt").exists() and (path / "pipelines").exists():
                project_root = path.resolve()
                print(f"✅ Found project root: {project_root}")
                break
        except (PermissionError, OSError) as e:
            print(f"Warning: Couldn't access {path} - {str(e)}", file=sys.stderr)
    else:
        cwd = Path.cwd()
        raise FileNotFoundError(
            "Project root not found! Checked:\n"
            f"- Search paths: {[str(p) for p in search_paths]}\n"
            f"- Current directory: {str(cwd)}\n"
            f"- Contents: {[f.name for f in cwd.iterdir() if f.is_dir()]}\n"
            "Required structure: must contain 'dbt/' and 'pipelines/' subdirectories"
        )

    paths = {
        "PROJECT_ROOT": project_root,
        "DBT_DIR": project_root / "dbt",
        "REQUIREMENTS_PATH": project_root / "requirements.txt",
        "DBT_TARGETS_DIR": project_root / "dbt" / "target",
        "DBT_RUN_RESULTS_DIR": project_root / "dbt" / "target" / "run_results.json",
        "PIPELINES_DIR": project_root / "pipelines",
        "CREDENTIALS": project_root / "pipelines" / "credentials.json",
        "ENV_FILE": project_root / ".env",
        "REQUEST_CACHE_DIR": project_root / "request_cache",
        "DLT_PIPELINE_DIR": project_root / "pipelines" / ".dlt"
    }
    return paths


def set_dlt_env_vars(paths):
    """Set DLT environment variables based on resolved paths."""
    os.environ["DLT_DATA_DIR"] = str(paths["DLT_PIPELINE_DIR"])
    os.environ["DLT_CONFIG_DIR"] = str(paths["DLT_PIPELINE_DIR"])
    os.environ["DLT_SECRETS_DIR"] = str(paths["DLT_PIPELINE_DIR"])
    os.environ["PROJECT_ROOT"] = str(paths["PROJECT_ROOT"])