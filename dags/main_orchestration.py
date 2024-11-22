from client_list import clients
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from data_utils.github_helper import get_github_token, trigger_workflow
from data_utils.airbyte_connection_id_retriever import get_airbyte_connection_id
from data_utils.alerting.alerting import task_failed
import pendulum

# Common variables
github_token = get_github_token()
airbyte_conn_id = 'airbyte_api'
env = Variable.get("environment")
default_args = {"owner": "airflow", "start_date": pendulum.datetime(2024, 11, 11, tz="UTC")}


def create_main_orchestration_dag(client_name):
    @dag(
        dag_id=f"main_orchestration_{client_name}",  # Ensure unique dag_id
        default_args=default_args,
        schedule=None,
        catchup=False,
        on_failure_callback=task_failed
    )
    def main_orchestration():
        @task()
        def get_connection_id(sync_type):
            """
            Get Airbyte connection ID for a given sync type and client.
            """
            connection_id = get_airbyte_connection_id(f"[{sync_type}]-[{env.upper()}] - {client_name.capitalize()}")
            return connection_id

        @task()
        def trigger_airbyte_sync(connection_id):
            """
            Trigger an Airbyte sync and return the job ID.
            """
            operator = AirbyteTriggerSyncOperator(
                task_id=f'trigger_airbyte_sync_{connection_id}',
                airbyte_conn_id=airbyte_conn_id,
                connection_id=connection_id,
                asynchronous=True,
            )
            # Execute the sync and return the job ID
            operator.execute(context={})  # Directly execute the operator
            return operator.job_id  # Assuming `job_id` is available after execution

        @task()
        def wait_for_sync_completion(job_id):
            """
            Wait for Airbyte job completion.
            """
            sensor = AirbyteJobSensor(
                task_id=f'wait_for_sync_completion_{job_id}',
                airbyte_conn_id=airbyte_conn_id,
                airbyte_job_id=job_id,
            )
            # Directly execute the sensor
            sensor.execute(context={})

        @task()
        def trigger_github_action():
            """
            Trigger a GitHub action for the client.
            """
            trigger_workflow(f'{client_name}_models_run_{env}.yml', token=github_token)

        # Define the workflow
        # Decidim sync
        decidim_connection_id = get_connection_id("Decidim")
        decidim_job_id = trigger_airbyte_sync(decidim_connection_id)
        decidim_sync_done = wait_for_sync_completion(decidim_job_id)

        # Matomo sync
        matomo_connection_id = get_connection_id("Matomo")
        matomo_job_id = trigger_airbyte_sync(matomo_connection_id)
        matomo_sync_done = wait_for_sync_completion(matomo_job_id)

        # Trigger GitHub action after both syncs are complete
        [decidim_sync_done, matomo_sync_done] >> trigger_github_action()

    return main_orchestration()


# Dynamically generate DAGs for all clients
for client in clients:
    globals()[f"main_orchestration_{client}"] = create_main_orchestration_dag(client_name=client)
