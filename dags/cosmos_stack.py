from airflow.decorators import dag
from pendulum import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
import os

# Set project paths
PROJECT_HOME = '/usr/local/airflow'
DBT_PROJECT_DIR = os.path.join(PROJECT_HOME, 'dbt')
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Optional: use global if not in venv

# Use a ProfileConfig that reads from your profiles.yml
profile_config = ProfileConfig(
    profile_name="default",         # must match your profile name in profiles.yml
    target_name="astro_dev",              # must match the target name
    profiles_yml_filepath="/usr/local/airflow/dbt/profiles.yml"  # set full path explicitly
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

# Define the Cosmos DbtDag
dbt_postgres_dag = DbtDag(
    dag_id="dbt_postgres_dag",
    operator_args={"dbt_command": "build", "install_deps": True},
    project_config=ProjectConfig(DBT_PROJECT_DIR),
    profile_config=profile_config,
    execution_config=execution_config,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 0}
)

dbt_postgres_dag