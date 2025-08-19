import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_utils.alerting.alerting import task_failed
from clients import clients
from data_utils.crossclient_aggregation.crossclient_pull import create_aggregated_tables
from data_utils.grist.grist_pull_all_clients import fetch_and_dump_data
import logging

connection_name="main_db_cluster_name"
queries = {
    "all_users": """SELECT id AS decidim_user_id, email, date_of_birth, gender, created_at, sign_in_count, current_sign_in_at, confirmed, managed, admin, deleted_at, blocked, spam, spam_reported_at, spam_probability
                    FROM prod.all_users""",
    "users": """SELECT id AS decidim_user_id, email, date_of_birth, gender, created_at, sign_in_count, current_sign_in_at, confirmed, managed, admin, deleted_at, blocked
                FROM prod.users""",
    "users_growth": """
                    WITH months AS(
                        SELECT
                            DATE_TRUNC('month', created_at) AS created_at
                        FROM prod.users
                    ),
                    months_totals AS(
                        SELECT
                            months.created_at,
                            SUM(COUNT(*)) OVER (
                                ORDER BY months.created_at ASC ROWS UNBOUNDED PRECEDING
                            ) AS user_count
                        FROM months
                        GROUP BY created_at
                    ),
                    months_totals_offset AS(
                        SELECT
                            months_totals.created_at,
                            months_totals.user_count,
                            LAG(months_totals.user_count, 12) OVER (
                                ORDER BY months_totals.created_at ASC
                            ) AS user_count_12_months_ago
                        FROM months_totals
                        GROUP BY created_at, user_count
                    )
                    SELECT
                        created_at,
                        user_count,
                        user_count_12_months_ago
                    FROM months_totals_offset
                    WHERE (created_at >= DATE_TRUNC('month', NOW()))                    
                    """,
    "budgets": """SELECT budgets_projects.id AS budgets_project_id, title, project_amount, is_selected, budget_id, budget_title, categories, project_url,
                components.ps_title
                FROM prod.budgets_projects
                JOIN prod.components on components.id = budgets_projects.decidim_component_id""",
    "participations": """SELECT participation_type, components.component_name, components.ps_title, COUNT(participation_id) AS participation_count, COUNT(DISTINCT user_id) AS participating_user_count
                        FROM prod.participations
						JOIN prod.components on components.id = decidim_component_id
                        GROUP BY participation_type, component_name, ps_title""",
    "participation_date": """WITH group_by_date AS (
                            SELECT participation_date::date, COUNT(participation_id) AS participation_count
                            FROM prod.participations
							GROUP BY participation_date
                            )
                            SELECT participation_date, SUM(participation_count) AS participation_count FROM group_by_date
                            GROUP BY participation_date""",
    "participating_users": """SELECT id AS decidim_user_id, COUNT(participation_id) AS participations_count
                            FROM prod.users
                            JOIN prod.participations ON users.id = participations.user_id
                            GROUP BY decidim_user_id""",
    "processes": """SELECT id AS ps_id, title, subtitle, published_at
                    FROM prod.stg_decidim_participatory_processes""",
    "components": """SELECT id AS component_id, manifest_name, component_name, published_at, ps_title, ps_subtitle, ps_type
                    FROM prod.components""",
    "participatory_spaces": """WITH participatory_processes AS (
                                SELECT type, id, title, slug, published_at
                                FROM prod.stg_decidim_participatory_processes
                                ), assemblies AS (
                                SELECT 'assembly' AS type, id, title, slug, published_at
                                FROM prod.stg_decidim_assemblies
                                ), initiatives AS (
                                SELECT 'initiatives' AS type, 0 AS id, 'N/A' AS title, 'N/A' AS slug, NULL::date AS published_at
                                FROM prod.stg_decidim_initiatives LIMIT 1
                                )
                                SELECT * FROM participatory_processes UNION ALL SELECT * FROM initiatives UNION ALL SELECT * FROM assemblies""",
    "referrers": """SELECT date, sub_type, SUM(nb_visits) AS nb_visits
                    FROM prod.int_matomo_referrers
                    GROUP BY date, sub_type""",
    "initiatives": """SELECT id, created_at, parsed_state
                    FROM prod.initiatives""",
    "country_of_visit": """SELECT date, code, nb_visits
                    FROM matomo.users_country"""}

with DAG(
        dag_id='crossclient_aggregation',
        default_args={'owner': 'airflow'},
        schedule='45 21 * * *',
        start_date=pendulum.datetime(2025, 6, 17, tz="UTC"),
        catchup=False

) as dag:
    aggregate_crossclient_data = PythonOperator(
        task_id='create_aggregated_tables',
        python_callable=create_aggregated_tables,
        op_args=[queries, clients],
        dag=dag,
        on_failure_callback=task_failed,
    )

    fetch_grist_data = PythonOperator(
        task_id='fetch_and_dump_grist_data',
        python_callable=fetch_and_dump_data,
        op_args=[f"{connection_name}"],
        dag=dag,
        on_failure_callback=task_failed,
    )

    logger = logging.getLogger(__name__)
    logger.warn(":DEBUG: crossclient_aggregation - This is a log message")
    logger.warn(f":DEBUG: crossclient_aggregation> Queries : {queries}")
    logger.warn(f":DEBUG: crossclient_aggregation> Clients : {clients}")
    aggregate_crossclient_data
    fetch_grist_data
    logger.warn(f":DEBUG: crossclient_aggregation> DAG terminated.")