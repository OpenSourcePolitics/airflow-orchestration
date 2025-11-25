import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from data_utils.alerting.alerting import task_failed
from data_utils.grist.grist_dump_document import dump_document_to_postgres
from data_utils.grist.grist_helper import get_grist_api
from data_utils.postgres_helper import get_postgres_connection

connection_name = "main_db_cluster_name"

with DAG(
    dag_id="grist_suivi_ca",
    default_args={"owner": "airflow"},
    schedule="45 0 * * *",
    start_date=pendulum.datetime(2024, 11, 15, tz="UTC"),
    catchup=False,
    tags=["grist"],
) as dag:
    doc_id = Variable.get("grist_suivi_ca_doc_id")
    api = get_grist_api("grist_osp", doc_id)
    engine = get_postgres_connection(connection_name, "aggregated_client_data")
    fetch_grist_data = PythonOperator(
        task_id="fetch_and_dump_grist_data",
        python_callable=dump_document_to_postgres,
        op_kwargs={
            "api": api,
            "engine": engine,
            "prefix": "ca",
            "tables": ["Clients", "Prestations"],
        },
        dag=dag,
        on_failure_callback=task_failed,
    )

    fetch_grist_data
