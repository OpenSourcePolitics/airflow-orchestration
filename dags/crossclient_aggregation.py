import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_utils.alerting.alerting import task_failed
from data_utils.crossclient_aggregation.crossclient_pull import create_aggregated_tables

connection_name="main_db_cluster_name"

queries = {
    "all_users": "SELECT id AS decidim_user_id, email, created_at, confirmed, sign_in_count, deleted_at, blocked, date_of_birth, gender FROM prod.all_users",
    "budgets": "SELECT id AS decidim_budget_id, title, amount, is_selected, categories, ps_title FROM prod.budgets",
}

with DAG(
        dag_id='crossclient_aggregation',
        default_args={'owner': 'airflow'},
        schedule='45 0 * * *',
        start_date=pendulum.datetime(2024, 11, 15, tz="UTC"),
        catchup=True
) as dag:

    aggregate_crossclient_data = PythonOperator(
        task_id='create_aggregated_tables',
        python_callable=create_aggregated_tables,
        op_args=[f"{connection_name}"],
        dag=dag,
        on_failure_callback=task_failed,
    )

    aggregate_crossclient_data