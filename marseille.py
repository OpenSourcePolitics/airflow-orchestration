import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from connections.airbyte.airbyte_connection_id_retriever import get_airbyte_connection_id
from data_utils.alerting.alerting import task_failed
from data_utils.github_helper import get_github_token, trigger_workflow
# Get the GitHub token using the function
github_token = get_github_token()

env = Variable.get("environment")

DECIDIM_AIRBYTE_CONNECTION_ID = get_airbyte_connection_id(f"[Decidim]-[{env.upper()}] - Marseille")
MATOMO_AIRBYTE_CONNECTION_ID = get_airbyte_connection_id(f"[Matomo]-[{env.upper()}] - Marseille")
AIRBYTE_AIRFLOW_CONN_ID = 'airbyte_api'

# DAG Configuration
with DAG(
        dag_id='marseille',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        on_failure_callback=task_failed
) as dag:

    trigger_airbyte_decidim_sync = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_decidim_sync',
        airbyte_conn_id=AIRBYTE_AIRFLOW_CONN_ID,
        connection_id=DECIDIM_AIRBYTE_CONNECTION_ID,
        asynchronous=True
    )

    wait_for_decidim_sync_completion = AirbyteJobSensor(
        task_id='wait_for_decidim_sync_completion',
        airbyte_conn_id=AIRBYTE_AIRFLOW_CONN_ID,
        airbyte_job_id=trigger_airbyte_decidim_sync.output
    )

    trigger_airbyte_matomo_sync = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_matomo_sync',
        airbyte_conn_id=AIRBYTE_AIRFLOW_CONN_ID,
        connection_id=MATOMO_AIRBYTE_CONNECTION_ID,
        asynchronous=True
    )

    wait_for_matomo_sync_completion = AirbyteJobSensor(
        task_id='wait_for_matomo_sync_completion',
        airbyte_conn_id=AIRBYTE_AIRFLOW_CONN_ID,
        airbyte_job_id=trigger_airbyte_matomo_sync.output
    )

    #Task to trigger the GitHub Action
    run_dbt_github_action = PythonOperator(
        task_id="trigger_github_action",
        python_callable=trigger_workflow,
        op_args=[f'marseille_models_run.yml'],  # Pass the YAML file name as an argument for the proper env
        params={'token': f"{github_token}"},  # Pass the GitHub token as a parameter
        dag=dag
    )

    trigger_airbyte_decidim_sync >> wait_for_decidim_sync_completion >> run_dbt_github_action
    trigger_airbyte_matomo_sync >> wait_for_matomo_sync_completion >> run_dbt_github_action
