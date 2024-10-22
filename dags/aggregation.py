from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from data_utils.metabase_aggregation.aggregation import perform_and_insert_aggregated_data

# Default arguments for the DAG tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
}

# Define the DAG
with DAG(
        'daily_aggregated_client_data_dag',
        default_args=default_args,
        description='A DAG to aggregate data from multiple clients and insert it into a database',
        schedule='@daily',
        start_date=datetime(2024, 10, 21),  # DAG start date
        catchup=False,
) as dag:

    # Task to run the aggregation and insertion function
    aggregate_and_insert_task = PythonOperator(
        task_id='perform_and_insert_aggregated_data_task',
        python_callable=perform_and_insert_aggregated_data,
        op_kwargs={},
    )

    aggregate_and_insert_task
