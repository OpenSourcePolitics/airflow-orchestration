import pendulum

#from client_list import clients
from data_utils.dags_utils.orchestration_utils import create_orchestration_dag

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag_id = 'meta_metabase_questionnaire_dashboard'
description = 'Orchestrator DAG for Metabase questionnaire filters update and creation'
schedule_interval = None  # Trigger manually
start_date = pendulum.datetime(2024, 11, 11, tz="UTC")

clients_with_questionnaires = [
    'lyon',
    'marseille',
    'ps_belge'
    ]

metabase_questionnaire_filtering_dags = [f"metabase_metabase_questionnaire_dashboard_{client}" for client in clients_with_questionnaires]

dag = create_orchestration_dag(dag_id, description, schedule_interval, start_date, metabase_questionnaire_dashboard_dags)