import pendulum

from clients import clients
from data_utils.dags_utils.orchestration_utils import create_orchestration_dag

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag_id = 'meta_orchestration_matomo_dump'
description = 'Orchestrator DAG for managing all matomo dump'
schedule_interval = None  # Trigger manually
start_date = pendulum.datetime(2024, 11, 11, tz="UTC")

main_orchestration_dags = [f"matomo_dump_{client}" for client in clients]

dag = create_orchestration_dag(dag_id, description, schedule_interval, start_date, main_orchestration_dags)