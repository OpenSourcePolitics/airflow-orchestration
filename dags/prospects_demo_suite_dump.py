import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_utils.alerting.alerting import task_failed
from data_utils.grist.demo_suite_keycloak import fetch_data_from_keycloak_and_dump_to_grist

with DAG(
        dag_id='prospects_demo_suite_dump',
        default_args={'owner': 'airflow'},
        schedule='45 0 * * *',
        start_date=pendulum.datetime(2024, 11, 15, tz="UTC"),
        catchup=False
) as dag:

    fetch_and_dump_data = PythonOperator(
        task_id='fetch_data_from_keycloak_and_dump_to_grist',
        python_callable=fetch_data_from_keycloak_and_dump_to_grist,
        dag=dag,
        on_failure_callback=task_failed,
    )

    fetch_and_dump_data