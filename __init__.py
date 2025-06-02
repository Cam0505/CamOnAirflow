__version__ = "0.1.0" 

from .project_path import (
    get_project_paths,
    set_dlt_env_vars
)

# from .helper_functions import write_profiles_yml

__all__ = [
    "get_project_paths",
    "set_dlt_env_vars",
]