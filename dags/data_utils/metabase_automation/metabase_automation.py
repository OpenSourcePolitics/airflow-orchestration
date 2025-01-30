from airflow.hooks.base import BaseHook
from metabase_api import Metabase_API
import logging
import json
import re

http_conn = BaseHook.get_connection("metabase_http")

# Initialize MTB using the retrieved connection details
MTB = Metabase_API(
    http_conn.host,
    http_conn.login,
    http_conn.password
)

def modify_dict(original_dict, keys_list, value):
    working_dict = original_dict
    for key in keys_list[:-1]:
        working_dict = working_dict[key]
    working_dict[keys_list[-1]] = value


def get_dashboard(dashboard_id):
    return MTB.get(f'/api/dashboard/{dashboard_id}')


def get_database_info(database_id):
    return MTB.get(f'/api/database/{database_id}?include=tables')


def get_field_info(field_id):
    return MTB.get(f'/api/field/{field_id}')


def get_all_db_ids(dashboard):
    if 'dashcards' in dashboard and isinstance(dashboard['dashcards'], list):
        return list(set([dashcard['card']['database_id'] for dashcard in dashboard['dashcards'] if
                         'card' in dashcard and 'database_id' in dashcard['card']]))
    return []


def get_tables_info(database_id, schema=None):
    database_info = get_database_info(database_id)
    if schema:
        return [{'table_id': table['id'], 'table_name': table['name'], 'schema': table['schema']} for table in
                database_info['tables'] if table['schema'] == schema]
    else:
        return [{'table_id': table['id'], 'table_name': table['name'], 'schema': table['schema']} for table in
                database_info['tables']]


def extract_field_integers(json_str):
    field_values = []
    matches = re.findall(r'"field",\s*(\d+)', json_str)
    for match in matches:
        field_values.append(int(match))
    field_values = list(set(field_values))
    return field_values


def get_new_field_id(old_field_id, table_id_mapping):
    field = get_field_info(old_field_id)
    old_table_id = field['table']['id']
    new_table_id = table_id_mapping[old_table_id]
    old_table = MTB.get(f'/api/table/{old_table_id}/query_metadata')
    new_table = MTB.get(f'/api/table/{new_table_id}/query_metadata')
    if not old_table:
        logging.error(f"Failed to fetch table metadata for table {old_table_id}")
        return
    elif not new_table:
        logging.error(f"Failed to fetch table metadata for table {new_table_id}")
        return
    field_name_to_new_id = {field['name']: field['id'] for field in new_table['fields']}
    field_id_to_name = {field['id']: field['name'] for field in old_table['fields']}
    if old_field_id in field_id_to_name and field_id_to_name[old_field_id] in field_name_to_new_id:
        new_field_id = field_name_to_new_id[field_id_to_name[old_field_id]]
        return new_field_id
    return None


def update_object_fields(object, table_id_mapping):
    object_json_str = json.dumps(object)
    field_ids_to_replace = extract_field_integers(object_json_str)
    for field_id in field_ids_to_replace:
        new_field_id = get_new_field_id(field_id, table_id_mapping)
        object_json_str = re.sub(rf'"field",\s*{field_id}', f'"field", {new_field_id}', object_json_str)
    object = json.loads(object_json_str)
    return object


def update_card_db(card_id, db_id, table_id_mapping):
    try:
        card_to_update = MTB.get(f'/api/card/{card_id}')
        if not card_to_update:
            logging.error(f"Failed to fetch card {card_id}")
            return

        modify_dict(card_to_update, ['dataset_query', 'database'], db_id)

        # We handle the cards that work with non SQL query
        if card_to_update["query_type"] == "query":
            old_table_id = card_to_update["table_id"]
            new_table_id = table_id_mapping[old_table_id]
            modify_dict(card_to_update, ['dataset_query', 'query', 'source-table'], new_table_id)
            # If the cards work with joins :
            if card_to_update["dataset_query"]["query"].get("joins"):
                for join in card_to_update["dataset_query"]["query"]["joins"]:
                    old_table_id = join["source-table"]
                    new_table_id = table_id_mapping[old_table_id]
                    modify_dict(join, ['source-table'], new_table_id)
            card_to_update = update_object_fields(card_to_update, table_id_mapping)

        # We handle the cards that work with a SQL query
        elif card_to_update["query_type"] == "native":
            card_to_update["dataset_query"]["native"]["template-tags"] = update_object_fields(
                card_to_update["dataset_query"]["native"]["template-tags"], table_id_mapping)

        response = MTB.put(f'/api/card/{card_id}', json=card_to_update)
        if response:
            logging.info(f"Card {card_id} updated successfully.")
        else:
            logging.error(f"Failed to update card {card_id}")
    except Exception as e:
        logging.error(f"Unexpected error during card update: {str(e)}")


def update_dashcard_filters(dashcard, table_id_mapping):
    if dashcard.get("parameter_mappings"):
        for mapping in dashcard.get("parameter_mappings", []):
            mapping["target"] = update_object_fields(mapping["target"], table_id_mapping)
    return dashcard


def update_dashboard(dashboard_id, dashboard):
    response = MTB.put(f'/api/dashboard/{dashboard_id}', json=dashboard)
    if response:
        logging.info(f"Dashboard {dashboard_id} updated successfully.")
    else:
        logging.error(f"Failed to update dashboard {dashboard_id}")


def get_new_dashboard_id(sub_collection_id, dashboard_name):
    """
    Retrieve the ID of the newly created dashboard in a specific collection by its name.

    Args:
        sub_collection_id (int): The ID of the collection where the dashboard is copied.
        dashboard_name (str): The name of the dashboard to locate.

    Returns:
        int: The ID of the dashboard if found, otherwise raises an exception.
    """
    # Fetch all dashboards in the specified collection
    dashboards = MTB.get(f'/api/dashboard/', params={'collection_id': sub_collection_id})
    if not dashboards:
        raise RuntimeError(f"No dashboards found in collection {sub_collection_id}.")

    # Search for the dashboard by name
    for dashboard in dashboards:
        if dashboard['name'] == dashboard_name and dashboard['collection_id'] == sub_collection_id:
            logging.info(
                f"Found new dashboard {dashboard['name']} with id {dashboard['id']} in collection {sub_collection_id}."
            )
            return dashboard['id']

    raise RuntimeError(f"Dashboard with name '{dashboard_name}' not found in collection {sub_collection_id}.")


def replace_dashboard_source_db(dashboard_id, new_db_id, schema_name):
    dashboard = get_dashboard(dashboard_id)

    old_db_ids = get_all_db_ids(dashboard)
    if len(old_db_ids) > 1:
        logging.error("Multiple database IDs found. This script does not support multiple source databases.")
        return

    old_db_id = old_db_ids[0]
    old_tables = get_tables_info(old_db_id)

    if not schema_name:
        raise ValueError("Schema Name is required.")

    # We build the tables mapping between new and old tables id
    new_tables = get_tables_info(new_db_id, schema_name)
    table_id_mapping = {}
    for old_table in old_tables:
        for new_table in new_tables:
            if old_table['table_name'] == new_table['table_name'] and old_table['schema'] == new_table['schema']:
                table_id_mapping[old_table['table_id']] = new_table['table_id']

    # If there are some dashcards in the dashboard, we update them
    if 'dashcards' in dashboard and isinstance(dashboard['dashcards'], list):
        updated_cards = []
        for dashcard in dashboard['dashcards']:
            # Check if the dashcard contains a card
            if 'card' in dashcard and 'database_id' in dashcard['card']:
                card = dashcard["card"]
                if card["id"] not in updated_cards:
                    update_card_db(card["id"], new_db_id, table_id_mapping)
                    updated_cards.append(card["id"])
                dashcard = update_dashcard_filters(dashcard, table_id_mapping)
        update_dashboard(dashboard_id, dashboard)


def pin_dashboard_in_collection(dashboard_id, pinned=True):
    """
    Pin or unpin a dashboard in a collection.

    Args:
        dashboard_id (int): The ID of the dashboard.
        pinned (bool): True to pin, False to unpin.
    """
    payload = {"pinned": pinned}
    response = MTB.put(f'/api/dashboard/{dashboard_id}', json=payload)

    # Debugging: Log the response
    logging.info(f"Response from pin_dashboard_in_collection: {response}")

    # Handle response properly
    if response and response == 200:
        action = "pinned" if pinned else "unpinned"
        print(f"Dashboard {dashboard_id} successfully {action}.")
    else:
        raise RuntimeError(f"Failed to update pin status for dashboard {dashboard_id}. "
                           f"Error: {response}")


def dashboard_copy(dashboard_id, collection_id, dashboard_name):
    try:
        if not dashboard_name:
            raise ValueError("Dashboard Name is required.")

        payload = {
            "name": dashboard_name,
            "collection_id": collection_id,
            "is_deep_copy": True
        }

        response = MTB.post(f'/api/dashboard/{dashboard_id}/copy', json=payload)
        if response:
            logging.info("Dashboard copied successfully.")
        else:
            logging.error(f"Failed to copy dashboard: {dashboard_id}")
    except ValueError as ve:
        logging.error(f"Input error: {str(ve)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

