import pendulum
from data_utils.dags_utils.orchestration_utils import create_orchestration_dag
from dags_list import dags_main_orchestration

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag_id = 'meta_main_orchestration'
description = 'Orchestrator DAG for managing client DAGs sequentially'
schedule_interval = None  # Trigger manually
start_date = pendulum.datetime(2024, 11, 11, tz="UTC")
client_dags = dags_main_orchestration

dag = create_orchestration_dag(dag_id, description, schedule_interval, start_date, client_dags)
