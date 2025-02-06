from airflow.models import Variable
from airflow.decorators import task, dag
from client_list import clients
from data_utils.metabase_automation.metabase_automation import (
    dashboard_copy,
    get_new_dashboard_id,
    replace_dashboard_source_db,
    get_collection_id_by_name,
    create_sub_collection_if_not_exist,
    clean_sub_collection,
    get_sub_collection_id_by_name,
    get_dashboard_names
)
from data_utils.metabase_automation.metabase_generic_dashboard_data import metabase_generic_dashboard_data
from data_utils.alerting.alerting import task_failed
import logging
import re

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

        client_metadata = metabase_generic_dashboard_data[client_name]
        clients_collection_name = client_metadata["collection_name"]
        client_database_id = client_metadata["database_id"]
        client_language = client_metadata["language"]

        sub_collection_name, name_global_dashboard, name_local_dashboard = get_dashboard_names(client_language)
        metabase_reference_global_dashboard_id = Variable.get(f"metabase_reference_global_dashboard_id_{client_language}")
        metabase_reference_local_dashboard_id = Variable.get(f"metabase_reference_local_dashboard_id_{client_language}")


        def sanitize_task_name(name):
            """
            Convert a string into a valid Airflow task_id by:
            - Converting to lowercase
            - Replacing spaces with underscores
            - Removing all non-alphanumeric characters except underscores
            """
            sanitized_name = re.sub(r'[^a-zA-Z0-9_]', '', name.replace(" ", "_").lower())
            return sanitized_name

        @task(task_id='prepare_sub_collection')
        def prepare_sub_collection(collection_name, sub_collection_name):
            """
            Ensure that the sub-collection exists inside the parent collection. If not, create it.
            Then retrieve the sub-collection ID and clean it before copying dashboards.
            """
            # Get the parent collection ID
            collection_id = get_collection_id_by_name(collection_name)

            # Ensure the sub-collection exists
            create_sub_collection_if_not_exist(collection_id, sub_collection_name)

            # Ensure the sub-collection is empty before use
            sub_collection_id = get_sub_collection_id_by_name(collection_id, sub_collection_name)
            logging.info(f"Cleaning sub-collection ID: {sub_collection_id}")
            clean_sub_collection(sub_collection_id)

            return sub_collection_id

        def create_copy_dashboard_task(dashboard_id, dashboard_name):
            task_safe_name = sanitize_task_name(dashboard_name)  # Ensure the task name is valid

            @task(task_id=f'copy_{task_safe_name}', provide_context=True)
            def copy_dashboard(**kwargs):
                """
                Copies a dashboard into the prepared sub-collection and returns its new ID.
                """
                # Retrieve the sub-collection ID from XCom
                sub_collection_id = kwargs['ti'].xcom_pull(task_ids='prepare_sub_collection')
                dashboard_copy(dashboard_id, sub_collection_id, dashboard_name)
                return get_new_dashboard_id(sub_collection_id, dashboard_name)

            return copy_dashboard

        def create_update_dashboard_task(dashboard_name, database_id):
            task_safe_name = sanitize_task_name(dashboard_name)

            @task(task_id=f'update_{task_safe_name}', provide_context=True)
            def update_dashboard(**kwargs):
                """
                Update the database source of the copied dashboard.
                """
                # Extract task ID string
                copy_dashboard_task_id = f'copy_{task_safe_name}'

                # Retrieve the new dashboard ID from XCom
                new_dashboard_id = kwargs['ti'].xcom_pull(task_ids=copy_dashboard_task_id)
                replace_dashboard_source_db(new_dashboard_id, database_id, 'prod')

            return update_dashboard

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
    for client in clients:
        globals()[f"metabase_generic_dashboard_orchestration_{client}"] = create_metabase_generic_dashboard_dag(client_name=client)
