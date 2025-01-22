import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_utils.alerting.alerting import task_failed
from data_utils.odoo.odoo_helper import odoo_invoices_automation_helper

with DAG(
        dag_id='odoo_automation',
        default_args={'owner': 'airflow'},
        schedule='0 7 * * *',
        start_date=pendulum.datetime(2025, 1, 20, tz="UTC"),
        catchup=False
) as dag:

    fetch_odoo_invoices_data = PythonOperator(
        task_id='fetch_and_dump_odoo_invoices',
        python_callable=odoo_invoices_automation_helper,
        dag=dag,
        on_failure_callback=task_failed,
    )

    fetch_odoo_invoices_data