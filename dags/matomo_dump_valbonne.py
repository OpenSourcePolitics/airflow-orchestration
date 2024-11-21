import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from data_utils.matomo_pull.matomo_helper import fetch_and_dump_data
from data_utils.alerting.alerting import task_failed

matomo_site_id = Variable.get("valbonne_matomo_site_id")
matomo_db_name = Variable.get("valbonne_matomo_db_name")

with DAG(
        dag_id='matomo_dump_valbonne',
        default_args={'owner': 'airflow'},
        schedule=None,  # Disable automatic scheduling
        start_date=pendulum.datetime(2024, 10, 1, tz="UTC"),
        catchup=True
) as dag:

    fetch_matomo_data = PythonOperator(
        task_id='fetch_and_dump_matomo_data',
        python_callable=fetch_and_dump_data,
        op_args=[f"{matomo_site_id}", f"{matomo_db_name}", "{{ ds }}"],
        dag=dag,
        on_failure_callback=task_failed,
    )

    fetch_matomo_data