import pendulum

from client_list import clients
from data_utils.dags_utils.orchestration_utils import create_orchestration_dag

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag_id = 'meta_metabase_generic_dashboard'
description = 'Orchestrator DAG for Metabase Generic Dashboard update and creation'
schedule_interval = None  # Trigger manually
start_date = pendulum.datetime(2024, 11, 11, tz="UTC")

metabase_generic_dashboard_dags = [f"metabase_generic_dashboard_orchestration_{client}" for client in clients]

dag = create_orchestration_dag(dag_id, description, schedule_interval, start_date, metabase_generic_dashboard_dags)