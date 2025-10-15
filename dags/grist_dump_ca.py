import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_utils.alerting.alerting import task_failed
from data_utils.grist.suivi_ca import fetch_and_dump_data

connection_name="main_db_cluster_name"

with DAG(
    dag_id="grist_suivi_ca",
    default_args={"owner": "airflow"},
    schedule="45 0 * * *",
    start_date=pendulum.datetime(2024, 11, 15, tz="UTC"),
    catchup=False,
) as dag:
    fetch_grist_data = PythonOperator(
        task_id='fetch_and_dump_grist_data',
        python_callable=fetch_and_dump_data,
        op_args=[f"{connection_name}"],
        dag=dag,
        on_failure_callback=task_failed,
    )

    fetch_grist_data