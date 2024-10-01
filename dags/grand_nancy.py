import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from connections.airbyte.airbyte_connection_id_retriever import get_airbyte_connection_id
from data_utils.github_helper import get_github_token, trigger_workflow

# Get the GitHub token using the function
github_token = get_github_token()


AIRBYTE_CONNECTION_ID = get_airbyte_connection_id("[Decidim] - Grand Nancy")
AIRBYTE_AIRFLOW_CONN_ID = 'airbyte_local'

# DAG Configuration
with DAG(
        dag_id='grand_nancy_dag_v2',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
) as dag:

    # trigger_airbyte_sync = AirbyteTriggerSyncOperator(
    #     task_id='airbyte_trigger_sync',
    #     airbyte_conn_id=AIRBYTE_AIRFLOW_CONN_ID,
    #     connection_id=AIRBYTE_CONNECTION_ID,
    #     asynchronous=True
    # )
    #
    # wait_for_sync_completion = AirbyteJobSensor(
    #     task_id='airbyte_check_sync',
    #     airbyte_conn_id='airflow-call-to-airbyte-example',
    #     airbyte_job_id=trigger_airbyte_sync.output
    # )

    # Task to trigger the GitHub Action
    run_dbt_github_action = PythonOperator(
        task_id="trigger_github_action",
        python_callable=trigger_workflow,
        op_args=['grand_nancy_models_run.yml'],  # Pass the YAML file name as an argument
        params={'token': f"{github_token}"},  # Pass the GitHub token as a parameter
        dag=dag
    )

    # trigger_airbyte_sync >> wait_for_sync_completion >>
    run_dbt_github_action