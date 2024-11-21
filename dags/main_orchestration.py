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
        def trigger_airbyte_sync(sync_type):
            connection_id = get_airbyte_connection_id(f"[{sync_type}]-[{env.upper()}] - {client_name.capitalize()}")

            return AirbyteTriggerSyncOperator(
                task_id=f'trigger_airbyte_{sync_type.lower()}_sync',
                airbyte_conn_id=airbyte_conn_id,
                connection_id=connection_id,
                asynchronous=True,
            )

        @task()
        def wait_for_sync_completion(sync_type, trigger_output):
            return AirbyteJobSensor(
                task_id=f'wait_for_{sync_type.lower()}_sync_completion',
                airbyte_conn_id=airbyte_conn_id,
                airbyte_job_id=trigger_output.output,
            )

        @task()
        def trigger_github_action():
            trigger_workflow(f'{client_name}_models_run_{env}.yml', token=github_token)

        # Define the workflow
        decidim_trigger = trigger_airbyte_sync("Decidim")
        decidim_wait_for_sync_completion = wait_for_sync_completion("Decidim", decidim_trigger)

        matomo_trigger = trigger_airbyte_sync("Matomo")
        matomo_wait_for_sync_completion = wait_for_sync_completion("Matomo", matomo_trigger)

        trigger_github_action = trigger_github_action()

        decidim_trigger >> decidim_wait_for_sync_completion >> trigger_github_action
        matomo_trigger >> matomo_wait_for_sync_completion >> trigger_github_action

    return main_orchestration()

for client in clients:
    globals()[f"main_orchestration_{client}"] = create_main_orchestration_dag(client_name=client)
