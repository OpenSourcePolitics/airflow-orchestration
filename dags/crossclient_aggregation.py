import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_utils.alerting.alerting import task_failed
from client_list import clients
from data_utils.crossclient_aggregation.crossclient_pull import create_aggregated_tables
import logging

queries = {
    "all_users": """SELECT id AS decidim_user_id, email, created_at, sign_in_count, confirmed, date_of_birth, gender, admin, deleted_at, blocked, spam
                FROM prod.all_users""",
    "budgets": """SELECT budgets_projects.id AS budgets_project_id, title, project_amount, is_selected, budget_id, budget_title, categories, project_url,
                components.ps_title
                FROM prod.budgets_projects
                JOIN prod.components on components.id = budgets_projects.decidim_component_id""",
    "participations": """SELECT participation_type, COUNT(*)
                        FROM prod.participations
                        GROUP BY participation_type""",
    "processes": """SELECT id AS ps_id, title, subtitle, published_at
                        FROM prod.stg_decidim_participatory_processes""",
    "components": """SELECT id AS component_id, manifest_name, component_name, published_at, ps_title, ps_subtitle, ps_type
                        FROM prod.components"""
}

with DAG(
        dag_id='crossclient_aggregation',
        default_args={'owner': 'airflow'},
        schedule='45 21 * * *',
        start_date=pendulum.datetime(2025, 6, 17, tz="UTC"),
        catchup=True

) as dag:
    aggregate_crossclient_data = PythonOperator(
        task_id='create_aggregated_tables',
        python_callable=create_aggregated_tables,
        op_args=[queries, clients],
        dag=dag,
        on_failure_callback=task_failed,
    )

    logger = logging.getLogger(__name__)
    logger.warn(":DEBUG: crossclient_aggregation - This is a log message")
    logger.warn(f":DEBUG: crossclient_aggregation> Queries : {queries}")
    logger.warn(f":DEBUG: crossclient_aggregation> Clients : {clients}")
    aggregate_crossclient_data
    logger.warn(f":DEBUG: crossclient_aggregation> DAG terminated.")
