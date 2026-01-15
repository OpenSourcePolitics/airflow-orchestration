from client_list import clients
from airflow.decorators import dag, task
from airflow.models import Variable
from data_utils.github_helper import get_github_token, trigger_workflow
from data_utils.alerting.alerting import task_failed
import pendulum

# Common variables
github_token = get_github_token()
env = Variable.get("environment")
default_args = {"owner": "airflow", "start_date": pendulum.datetime(2024, 11, 11, tz="UTC")}


def create_dbt_orchestration_dag(client_name):
    @dag(
        dag_id=f"dbt_orchestration_{client_name}",  # Ensure unique dag_id
        default_args=default_args,
        schedule=None,
        catchup=False,
        on_failure_callback=task_failed
    )
    def dbt_orchestration():

        # Define GitHub action trigger
        @task(task_id=f'trigger_github_action_{client_name}')
        def trigger_github_action():
            """
            Trigger a GitHub action for the client.
            """
            trigger_workflow(f'{client_name}_models_run_{env}.yml', token=github_token)

 
        # Trigger GitHub action
        github_action = trigger_github_action()

        github_action

    return dbt_orchestration()


enabled = Variable.get("dbt_orchestration_enabled")

if enabled == "True":
    # Dynamically generate DAGs for all clients
    for client in clients:
        globals()[f"dbt_orchestration_{client}"] = create_dbt_orchestration_dag(client_name=client)
