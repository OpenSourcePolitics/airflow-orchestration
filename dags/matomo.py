import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_utils.matomo_pull.matomo_helper import fetch_and_dump_data
from data_utils.alerting.alerting import task_failed


matomo_site_id = 3
matomo_db_name = "airflow_db_test"

# DAG Configuration
with DAG(
        dag_id='matomo_test',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
) as dag:

    fetch_matomo_data = PythonOperator(
        task_id='fetch_and_dump_matomo_data',
        python_callable=fetch_and_dump_data,
        op_args=[f"{matomo_site_id}", f"{matomo_db_name}"],
        dag=dag,
        on_failure_callback=task_failed,
    )

    fetch_matomo_data