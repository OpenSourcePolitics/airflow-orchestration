from airflow.models import Variable
from airflow.decorators import dag
from clients import clients
from data_utils.metabase_automation.metabase_airflow_task import prepare_sub_collection, \
    create_copy_dashboard_task, create_update_dashboard_task
from data_utils.metabase_automation.metabase_automation import (
    get_generic_dashboard_names
)

from data_utils.alerting.alerting import task_failed

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def create_metabase_generic_dashboard_dag(client_name):
    @dag(
        dag_id=f"metabase_generic_dashboard_orchestration_{client_name}",  # Ensure unique dag_id
        default_args=default_args,
        schedule=None,
        catchup=False,
        on_failure_callback=task_failed
    )
    def create_and_update_generic_dashboard():

        client_metadata = clients[client_name]["metabase"]
        clients_collection_name = client_metadata["collection_name"]
        client_database_id = client_metadata["database_id"]
        client_language = client_metadata["language"]

        sub_collection_name, name_global_dashboard, name_local_dashboard = get_generic_dashboard_names(client_language)
        metabase_reference_global_dashboard_id = Variable.get(f"metabase_reference_global_dashboard_id_{client_language}")
        metabase_reference_local_dashboard_id = Variable.get(f"metabase_reference_local_dashboard_id_{client_language}")

        sub_collection_id = prepare_sub_collection(
            collection_name=clients_collection_name,
            sub_collection_name=sub_collection_name
        )

        copy_global_dashboard = create_copy_dashboard_task(metabase_reference_global_dashboard_id,
                                                           name_global_dashboard)()
        update_global_dashboard_database = create_update_dashboard_task(name_global_dashboard, client_database_id)()

        copy_local_dashboard = create_copy_dashboard_task(metabase_reference_local_dashboard_id, name_local_dashboard)()
        update_local_dashboard_database = create_update_dashboard_task(name_local_dashboard, client_database_id)()

        sub_collection_id >> [copy_global_dashboard, copy_local_dashboard]
        copy_global_dashboard >> update_global_dashboard_database
        copy_local_dashboard >> update_local_dashboard_database

    return create_and_update_generic_dashboard()


enabled = Variable.get("metabase_generic_dashboard_enabled")

if enabled == "True":
    # Dynamically generate DAGs for all clients
    for client in clients.keys():
        globals()[f"metabase_generic_dashboard_orchestration_{client}"] = create_metabase_generic_dashboard_dag(client_name=client)
