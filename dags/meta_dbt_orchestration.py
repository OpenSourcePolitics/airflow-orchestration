import pendulum

from client_list import clients
from data_utils.dags_utils.orchestration_utils import create_orchestration_dag

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag_id = 'meta_dbt_orchestration'
description = 'Orchestrator DAG for managing client GA DAGs sequentially'
schedule_interval = None  # Trigger manually
start_date = pendulum.datetime(2024, 11, 11, tz="UTC")

dbt_orchestration_dags = clients_prefixed = [f"dbt_orchestration_{client}" for client in clients]

dag = create_orchestration_dag(dag_id, description, schedule_interval, start_date, dbt_orchestration_dags)
