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

# Retrieve environment and city name from Airflow Variables
env = Variable.get("environment")
client_name = "toulouse"

# Airbyte Connection IDs
DECIDIM_AIRBYTE_CONNECTION_ID = get_airbyte_connection_id(f"[Decidim]-[{env.upper()}] - {client_name.capitalize()}")
MATOMO_AIRBYTE_CONNECTION_ID = get_airbyte_connection_id(f"[Matomo]-[{env.upper()}] - {client_name.capitalize()}")
AIRBYTE_AIRFLOW_CONN_ID = 'airbyte_api'

# DAG Configuration
with DAG(
        dag_id=f'{client_name}',  # Use city name for the DAG ID
        default_args={'owner': 'airflow'},
        schedule='30 4 * * *',
        start_date=pendulum.today('UTC').add(days=-1), 
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

    # Task to trigger the GitHub Action
    run_dbt_github_action = PythonOperator(
        task_id="trigger_github_action",
        python_callable=trigger_workflow,
        op_args=[f'{client_name}_models_run_{env}.yml'],  # Pass the YAML file name as an argument for the proper env
        params={'token': f"{github_token}"},  # Pass the GitHub token as a parameter
        dag=dag
    )

    trigger_airbyte_decidim_sync >> wait_for_decidim_sync_completion >> run_dbt_github_action
    trigger_airbyte_matomo_sync >> wait_for_matomo_sync_completion >> run_dbt_github_action
