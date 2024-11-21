import pendulum
from data_utils.dags_utils.orchestration_utils import create_orchestration_dag

default_args = {
    'owner': 'airflow',
    'retries': 1,
}
dag_id = 'general_orchestration'
description = 'General Orchestration DAG'
schedule_interval = "0 1 * * *"  # Every day at 1 AM
start_date = pendulum.datetime(2024, 11, 11, tz="UTC")
dags_to_orchestrate = ["meta_orchestration_matomo_dump", "meta_main_orchestration"]

dag = create_orchestration_dag(dag_id, description, schedule_interval, start_date, dags_to_orchestrate)
