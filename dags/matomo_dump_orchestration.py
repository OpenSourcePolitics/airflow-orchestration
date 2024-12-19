import pendulum
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from client_list import clients
from data_utils.matomo_pull.matomo_helper import fetch_and_dump_data
from data_utils.alerting.alerting import task_failed

default_args = {"owner": "airflow"}
default_start_date = pendulum.datetime(2024, 1, 1, tz="UTC")

def create_matomo_dump_dag(client_name):
    """Dynamically creates a DAG for fetching and dumping Matomo data."""
    @dag(
        dag_id=f"matomo_dump_{client_name}",
        default_args=default_args,
        schedule= "0 0 * * *",
        catchup=True,
        start_date=default_start_date,
        max_active_runs=1
    )
    def matomo_dump():
        matomo_site_id = Variable.get(f"{client_name}_matomo_site_id")
        matomo_db_name = Variable.get(f"{client_name}_matomo_db_name")

        fetch_matomo_data = PythonOperator(
            task_id='fetch_and_dump_matomo_data',
            python_callable=fetch_and_dump_data,
            op_args=[f"{matomo_site_id}", f"{matomo_db_name}", "{{ ds }}"],
            on_failure_callback=task_failed,
        )

        fetch_matomo_data

    return matomo_dump()


for client in clients:
    globals()[f"matomo_dump_{client}"] = create_matomo_dump_dag(client_name=client)