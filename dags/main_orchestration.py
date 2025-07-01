from clients import clients
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from data_utils.postgres_helper.airbyte_cleanup import airbyte_cleanup
from data_utils.github_helper import get_github_token, trigger_workflow
from data_utils.airbyte_connection_id_retriever import get_airbyte_connection_id
from data_utils.alerting.alerting import task_failed
from data_utils.questionnaires.questionnaire_pivot import create_questionnaire_filters
import pendulum


def create_main_orchestration_dag(client_name):
    # Common variables
    github_token = get_github_token()
    airbyte_conn_id = 'airbyte_api'
    env = Variable.get("environment")
    default_args = {
        "owner": "airflow",
        "start_date": pendulum.datetime(2024, 11, 11, tz="UTC")
    }

    dag_args = {
        "dag_id": f"main_orchestration_{client_name}",
        "default_args": default_args,
        "schedule": None,
        "catchup": False,
    }

    if env != "preproduction":
        dag_args["on_failure_callback"] = task_failed

    @dag(**dag_args)
    def main_orchestration():
        def get_connection_id_task(sync_type):
            @task(task_id=f'get_connection_id_{sync_type.lower()}_{client_name}')
            def get_connection_id():
                """
                Get Airbyte connection ID for a given sync type and client.
                """
                connection_id = get_airbyte_connection_id(f"[{sync_type}]-[{env.upper()}] - {client_name.capitalize()}")
                return connection_id

            return get_connection_id

        def trigger_airbyte_sync_task(sync_type, connection_id):
            """
            Task to trigger Airbyte sync.
            """
            return AirbyteTriggerSyncOperator(
                task_id=f'trigger_airbyte_sync_{sync_type.lower()}_{client_name}',
                airbyte_conn_id=airbyte_conn_id,
                connection_id=connection_id,
                asynchronous=True,  # Run asynchronously
                wait_seconds=10,
                timeout=3600,
            )

            return trigger_airbyte_sync

        def wait_for_sync_completion_task(sync_type, job_id):
            """
            Task to wait for Airbyte job completion.
            """
            return AirbyteJobSensor(
                task_id=f'wait_for_sync_completion_{sync_type.lower()}_{client_name}',
                airbyte_conn_id=airbyte_conn_id,
                airbyte_job_id=job_id,
                timeout=3600,
                poke_interval=30,
            )

        # Define GitHub action trigger
        @task(task_id=f'trigger_github_action_{client_name}')
        def trigger_github_action():
            """
            Trigger a GitHub action for the client.
            """
            trigger_workflow(f'{client_name}_models_run_{env}.yml', token=github_token)


        @task(task_id=f'cleanup_airbyte_metadata_{client_name}')
        def cleanup_airbyte_metadata():
            """
            Cleanup Airbyte metadata by dropping unnecessary tables.
            """
            airbyte_cleanup(client_name)

        def trigger_questionnaire_pivot():
            """
            Create filters table for selected questionnaires.
            """
            return PythonOperator(
                task_id='create_questionnaire_filters',
                python_callable=create_questionnaire_filters,
                op_args=[client_name],
                on_failure_callback=task_failed,
                )

        decidim_connection_id = get_connection_id_task("Decidim")()
        decidim_sync = trigger_airbyte_sync_task("Decidim", decidim_connection_id)
        wait_for_decidim_sync = wait_for_sync_completion_task("Decidim", decidim_sync.output)

        # Retrieve the connection ID for Matomo
        matomo_connection_id = get_connection_id_task("Matomo")()
        matomo_sync = trigger_airbyte_sync_task("Matomo", matomo_connection_id)
        wait_for_matomo_sync = wait_for_sync_completion_task("Matomo", matomo_sync.output)

        cleanup_task = cleanup_airbyte_metadata()

        # Trigger GitHub action
        github_action = trigger_github_action()

        # Trigger questionnaire pivot
        questionnaire_pivot = trigger_questionnaire_pivot()

        # Define dependencies
        for task_action in [github_action, cleanup_task]:
            [wait_for_decidim_sync, wait_for_matomo_sync] >> task_action

        github_action >> questionnaire_pivot

    return main_orchestration()


# Dynamically generate DAGs for all clients
for client in clients:
    globals()[f"main_orchestration_{client}"] = create_main_orchestration_dag(client_name=client)
