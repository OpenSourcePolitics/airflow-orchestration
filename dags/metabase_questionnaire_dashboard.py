from airflow.models import Variable
from airflow.decorators import dag
from client_list import clients
from data_utils.metabase_automation.metabase_airflow_task import prepare_sub_collection, \
    create_copy_dashboard_task, create_update_dashboard_task


from data_utils.metabase_automation.metabase_generic_dashboard_data import metabase_generic_dashboard_data
from data_utils.alerting.alerting import task_failed

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def create_metabase_generic_dashboard_dag(client_name):
    @dag(
        dag_id=f"metabase_spam_dashboard_orchestration_{client_name}",  # Ensure unique dag_id
        default_args=default_args,
        schedule=None,
        catchup=False,
        on_failure_callback=task_failed
    )
    def create_and_update_generic_dashboard():

        client_metadata = metabase_generic_dashboard_data[client_name]
        clients_collection_name = client_metadata["collection_name"]
        client_database_id = client_metadata["database_id"]

        name_spam_dashboard = "Modération"
        metabase_reference_spam_dashboard_id = Variable.get("metabase_reference_spam_dashboard_id")

        sub_collection_id = prepare_sub_collection(
            collection_name=clients_collection_name,
            sub_collection_name="Analyse utilisateurs"
        )

        copy_spam_dashboard = create_copy_dashboard_task(metabase_reference_spam_dashboard_id,
                                                           name_spam_dashboard)()
        update_spam_dashboard_database = create_update_dashboard_task(name_spam_dashboard, client_database_id)()

        sub_collection_id >> copy_spam_dashboard >> update_spam_dashboard_database

    return create_and_update_generic_dashboard()


enabled = Variable.get("metabase_spam_dashboard_enabled")

if enabled == "True":
    # Dynamically generate DAGs for all clients
    for client in clients:
        globals()[f"metabase_generic_dashboard_orchestration_{client}"] = create_metabase_generic_dashboard_dag(client_name=client)