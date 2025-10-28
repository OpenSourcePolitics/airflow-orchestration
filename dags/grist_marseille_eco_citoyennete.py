
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_utils.alerting.alerting import task_failed
from data_utils.grist.grist_dump_document import dump_document_to_postgres
from data_utils.grist.grist_helper import _get_grist_api
from airflow.models import Variable

connection_name="main_db_cluster_name"

with DAG(
    dag_id="grist_marseille_eco_citoyennete",
    default_args={"owner": "airflow"},
    schedule="15 3 * * *",
    start_date=pendulum.datetime(2024, 11, 15, tz="UTC"),
    catchup=False,
    tags=["marseille", "grist"],
) as dag:
    columns_to_explode = [("Mobilite", "Transport")]
    doc_id = Variable.get("grist_marseille_eco-citoyennete")
    api = _get_grist_api("grist_marseille", doc_id)
    fetch_grist_data = PythonOperator(
        task_id='fetch_and_dump_grist_data',
        python_callable=dump_document_to_postgres,
        op_args=[api, f"{connection_name}", "marseille", "eco_citoyennete", columns_to_explode],
        dag=dag,
        on_failure_callback=task_failed,
    )

    fetch_grist_data