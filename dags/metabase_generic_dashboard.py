from airflow import DAG
from airflow.operators.python import PythonOperator
from data_utils.metabase_automation.metabase_automation import (
    dashboard_copy,
    get_new_dashboard_id,
    replace_dashboard_source_db,
    MTB,
    get_all_db_ids, pin_dashboard_in_collection
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}


def get_collection_id_by_name(collection_name):
    """
    Retrieve the ID of a collection by its name.
    """
    collections = MTB.get('/api/collection/')
    for collection in collections:
        if collection['name'] == collection_name:
            return collection['id']
    raise RuntimeError(f"Collection with name '{collection_name}' not found.")


def get_sub_collection_id_by_name(parent_collection_id, sub_collection_name):
    """
    Retrieve the ID of a sub-collection by its name, given the parent collection ID.
    """
    # Retrieve the collection tree
    collection_tree = MTB.get('/api/collection/tree')

    # Find the parent collection
    for collection in collection_tree:
        if collection['id'] == parent_collection_id:
            # Look for the subcollection inside the parent collection
            for sub_collection in collection.get('children', []):
                if sub_collection['name'] == sub_collection_name:
                    return sub_collection['id']
            raise RuntimeError(
                f"Sub-collection with name '{sub_collection_name}' not found under parent collection ID {parent_collection_id}.")

    raise RuntimeError(f"Parent collection with ID {parent_collection_id} not found.")


def get_database_id_from_dashboard(dashboard_name, collection_name):
    """
    Retrieve the database ID from a dashboard named 'Tableau de bord général' in a specific collection.
    """
    collection_id = get_collection_id_by_name(collection_name)
    dashboard_id = get_new_dashboard_id(collection_id, dashboard_name)
    dashboard = MTB.get(f'/api/dashboard/{dashboard_id}')
    db_ids = get_all_db_ids(dashboard)
    if len(db_ids) == 1:
        return db_ids[0]
    raise RuntimeError(f"Expected one database ID, but found {len(db_ids)} for dashboard {dashboard_name}.")


def copy_dashboard_task(**kwargs):
    """
    Copy a dashboard and return the new dashboard ID.
    """
    dashboard_id = kwargs['dashboard_id']
    collection_name = kwargs['collection_name']
    dashboard_name = kwargs['dashboard_name']
    sub_collection_name = kwargs['sub_collection_name']

    # Get the collection ID by name
    collection_id = get_collection_id_by_name(collection_name)

    # Get the collection ID by name
    sub_collection_id = get_sub_collection_id_by_name(collection_id, sub_collection_name)

    # Copy the dashboard
    dashboard_copy(dashboard_id, sub_collection_id, dashboard_name)

    # Get the new dashboard ID
    new_db_id = get_new_dashboard_id(collection_id, dashboard_name)

    pin_dashboard_in_collection(new_db_id)

    return new_db_id


def update_dashboard_database_task(**kwargs):
    """
    Update the database source of the copied dashboard.
    """
    dashboard_name = kwargs['dashboard_name']
    collection_name = kwargs['collection_name']
    schema_name = kwargs['schema_name']

    # Get the new dashboard ID
    new_dashboard_id = kwargs['ti'].xcom_pull(task_ids='copy_dashboard')

    # Get the database ID from the "Tableau de bord général" dashboard
    new_db_id = get_database_id_from_dashboard(dashboard_name, collection_name)

    # Update the dashboard
    replace_dashboard_source_db(new_dashboard_id, new_db_id, schema_name)


with DAG(
        'metabase_dashboard_copy_and_update',
        default_args=default_args,
        description='Copy a Metabase dashboard and update its database source',
        schedule_interval=None,
        catchup=False,
) as dag:
    # Task 1: Copy the dashboard and get the new dashboard ID
    copy_dashboard_op = PythonOperator(
        task_id='copy_dashboard',
        python_callable=copy_dashboard_task,
        op_kwargs={
            'dashboard_id': 558,  # ID of the dashboard to copy
            'collection_name': "Marseille",  # Name of the target collection
            'sub_collection_name': "Test - TDB",  # Name of the target sub-collection
            'dashboard_name': "Tableau de bord global 🌍",  # Name of the new dashboard
        }
    )

    # Task 2: Update the database of the new dashboard
    update_dashboard_database_op = PythonOperator(
        task_id='update_dashboard_database',
        python_callable=update_dashboard_database_task,
        op_kwargs={
            'collection_name': "Marseille",  # Name of the collection containing the general dashboard
            'dashboard_name': "Tableau de bord global 🌍",  # Name of the copied dashboard
            'schema_name': 'prod',  # Schema to use for the new database
        },
        provide_context=True  # Enable XCom access
    )

    # Define task dependencies
    copy_dashboard_op >> update_dashboard_database_op
