from airflow.decorators import task
from .metabase_automation import (
    dashboard_copy,
    get_new_dashboard_id,
    replace_dashboard_source_db,
    create_sub_collection_if_not_exist,
    clean_sub_collection,
    get_sub_collection_id_by_name,
    get_collection_id_by_name,
    get_card_by_name_in_collection,
    copy_card_to_collection,
    replace_card_source_db,
    create_notification_alert_for_card_from_emails,
)

from airflow.models import Variable
import logging
import re

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



@task(task_id="copy_alert_card_and_create_notification", provide_context=True)
def copy_alert_card_and_create_notification(
    reference_collection_id: int,           # <-- collection id where the reference card lives
    card_name: str,
    target_db_id: int,
    schema_name: str = "prod",
    cron_expr: str = "0 0 9 ? * MON",
    send_once: bool = True,
    **kwargs
):
    """
    Copy the reference SQL card into the prepared sub-collection, switch DB, then create a notification on it.
    """
    recipients = Variable.get("metabase_alert_recipients", deserialize_json=True)

    ti = kwargs["ti"]
    target_collection_id = ti.xcom_pull(task_ids="prepare_sub_collection")

    # 1) Locate the reference card by name in the reference collection
    ref_card = get_card_by_name_in_collection(reference_collection_id, card_name)
    if not ref_card:
        raise RuntimeError(f"Reference card '{card_name}' not found in collection id {reference_collection_id}")

    # 2) Copy into the client's sub-collection (enforced into target collection)
    new_card_id = copy_card_to_collection(ref_card["id"], target_collection_id, new_name=card_name)

    # 3) Switch DB/schema
    replace_card_source_db(new_card_id, target_db_id, schema_name=schema_name)

    # 4) Create notification using user IDs resolved from emails
    notif_id = create_notification_alert_for_card_from_emails(
        card_id=new_card_id,
        recipient_emails=recipients,
        cron_expr=cron_expr,
        send_once=send_once
    )
    return notif_id
