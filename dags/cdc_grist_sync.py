import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_utils.alerting.alerting import task_failed
from data_utils.grist.grist_push import fetch_and_dump_cdc_data
from clients import clients

connection_name="main_db_cluster_name"

with DAG(
        dag_id='cdc_grist_sync',
        default_args={'owner': 'airflow'},
        schedule='45 0 * * *',
        start_date=pendulum.datetime(2024, 11, 15, tz="UTC"),
        catchup=False
) as dag:

    dump_grist_data = PythonOperator(
        task_id='fetch_and_dump_cdc_data',
        python_callable=fetch_and_dump_cdc_data,
        op_args=[connection_name, clients],
        dag=dag,
        on_failure_callback=task_failed,
    )

    dump_grist_data