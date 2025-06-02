from airflow.decorators import dag
from pendulum import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from path_config import ENV_FILE, DLT_PIPELINE_DIR, DBT_DIR


# Use a ProfileConfig that reads from your profiles.yml
profile_config = ProfileConfig(
    profile_name="camonairflow",         # must match your profile name in profiles.yml
    target_name="dev",              # must match the target name
    profiles_yml_filepath=DBT_DIR  # set full path explicitly
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_DIR
)

# Define the Cosmos DbtDag
dbt_motherduck_dag = DbtDag(
    dag_id="dbt_motherduck_dag",
    operator_args={"dbt_command": "build", "install_deps": True},
    project_config=ProjectConfig(DBT_DIR),
    profile_config=profile_config,
    execution_config=execution_config,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 0}
)
