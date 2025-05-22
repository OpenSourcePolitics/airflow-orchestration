import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from data_utils.alerting.alerting import task_failed
from data_utils.postgres.postgres_questionnaire_pull import make_form_filters

connection_name="db_cluster_name_data"

with DAG(
        dag_id='postgres_pivot_answers',
        default_args={'owner': 'airflow'},
        schedule= None,
        start_date=pendulum.datetime(2024, 11, 15, tz="UTC"),
        catchup=False
) as dag:

    fetch_answers_data = PythonOperator(
        task_id='fetch_and_dump_answers_data',
        python_callable=make_form_filters,
        op_args=[f"{connection_name}"],
        dag=dag,
        on_failure_callback=task_failed,
    )

    fetch_answers_data