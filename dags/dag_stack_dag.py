from airflow import DAG
from pendulum import datetime
from airflow_dbt.operators.dbt_operator import (
    DbtDepsOperator,
    DbtSeedOperator,
    DbtRunOperator,
    DbtTestOperator
)

import os



# These could be set with environment variables if you want to run the DAG outside the Astro container
PROJECT_HOME = '/usr/local/airflow'
DBT_PROJECT_DIR = os.path.join(PROJECT_HOME, 'dbt')
DBT_TARGET = 'astro_dev'
DBT_TARGET_DIR = os.path.join(DBT_PROJECT_DIR, 'target')

dag = DAG(
    dag_id='dag_stack_dag',
    schedule_interval=None,
    start_date=datetime(2025,1,1)
)

dbt_deps = DbtDepsOperator(
    task_id='dbt_deps',
    dir=DBT_PROJECT_DIR,
    profiles_dir=PROJECT_HOME,
    target=DBT_TARGET,
    dag=dag
)


# The dbt seed command loads files in the data directory to the database
dbt_seed = DbtSeedOperator(
    task_id='dbt_seed',
    dir=DBT_PROJECT_DIR,
    profiles_dir=PROJECT_HOME,
    target=DBT_TARGET,
    dag=dag
)



# This runs the transformation steps in the dbt pipeline
dbt_run = DbtRunOperator(
    task_id='dbt_run',
    dir=DBT_PROJECT_DIR,
    profiles_dir=PROJECT_HOME,
    target=DBT_TARGET,
    dag=dag
)


dbt_test = DbtTestOperator(
    task_id='dbt_test',
    dir=DBT_PROJECT_DIR,
    profiles_dir=PROJECT_HOME,
    target=DBT_TARGET,
    retries=0,  # Failing tests would fail the task, and we don't want Airflow to try again
  )



# This task copies the Great Expectations docs to the include directory that's mapped to my local volume
# so I can open them locally. In production, I'd upload this to an S3 bucket!

dbt_deps >> dbt_seed >>  dbt_run >> dbt_test