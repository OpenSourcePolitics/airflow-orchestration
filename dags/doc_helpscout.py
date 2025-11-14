import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_utils.alerting.alerting import task_failed
from data_utils.doc_helpscout import dump_helpscout_collection_to_grist

# id of the collection to export. See here:
# https://developer.helpscout.com/docs-api/articles/list/
helpscout_collection = "5d1c770a04286369ad8d1458"

with DAG(
    dag_id="fecth_helpscout_doc",
    default_args={"owner": "airflow"},
    schedule="50 0 * * *",
    start_date=pendulum.datetime(2024, 11, 15, tz="UTC"),
    catchup=False,
) as dag:
    dump_articles = PythonOperator(
        task_id="fetch_and_dump_helpscout_articles",
        python_callable=dump_helpscout_collection_to_grist,
        op_args=[helpscout_collection],
        dag=dag,
        on_failure_callback=task_failed,
    )

    dump_articles
