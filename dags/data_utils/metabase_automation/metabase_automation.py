from airflow.hooks.base import BaseHook
from metabase_api import Metabase_API
import logging
import json
import re

http_conn = BaseHook.get_connection("metabase_http")

class MetabaseClient:
    """
    Singleton class to initialize and manage Metabase API calls.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            http_conn = BaseHook.get_connection("metabase_http")
            cls._instance = super(MetabaseClient, cls).__new__(cls)
            cls._instance.api = Metabase_API(http_conn.host, http_conn.login, http_conn.password)
        return cls._instance.api

# Initialize Metabase client
MTB = MetabaseClient()

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


def get_collection_id_by_name(collection_name):
    """
    Retrieve the ID of a collection by its name.
    """
    collections = MTB.get('/api/collection/')
    for collection in collections:
        if collection['name'] == collection_name:
            logging.info(f'Found collection: {collection["name"]} and ID: {collection["id"]}')
            return collection['id']
    raise RuntimeError(f"Collection with name '{collection_name}' not found.")


def get_sub_collection_id_by_name(parent_collection_id, sub_collection_name):
    """
    Retrieve the ID of a sub-collection by its name, given the parent collection ID.
    """
    collection_tree = MTB.get('/api/collection/tree')
    for collection in collection_tree:
        if collection['id'] == parent_collection_id:
            for sub_collection in collection.get('children', []):
                if sub_collection['name'] == sub_collection_name:
                    return sub_collection['id']
            raise RuntimeError(
                f"Sub-collection with name '{sub_collection_name}' not found under parent collection ID {parent_collection_id}.")
    raise RuntimeError(f"Parent collection with ID {parent_collection_id} not found.")


def create_sub_collection_if_not_exist(collection_id, sub_collection_name):
    try:
        # Check if the sub-collection already exists
        sub_collection_id = get_sub_collection_id_by_name(collection_id, sub_collection_name)
        logging.info(f"Sub-collection '{sub_collection_name}' already exists with ID: {sub_collection_id}")

    except RuntimeError:
        # If the sub-collection does not exist, create it
        logging.info(f"Sub-collection '{sub_collection_name}' does not exist. Creating it now.")

        payload = {
            "name": sub_collection_name,
            "parent_id": collection_id
        }

        response = MTB.post('/api/collection/', json=payload)
        if not response or "id" not in response:
            raise RuntimeError(
                f"Failed to create sub-collection '{sub_collection_name}'.")

        sub_collection_id = response["id"]
        logging.info(f"Created new sub-collection '{sub_collection_name}' with ID: {sub_collection_id}")


def clean_sub_collection(collection_id):
    """
    Move all items within a specified collection to the trash.
    """
    response = MTB.get(f'/api/collection/{collection_id}/items')
    if not isinstance(response, dict) or 'data' not in response:
        raise RuntimeError(f"Unexpected API response format: {response}")
    items = response['data']
    if not items:
        logging.info(f"No items found in collection ID {collection_id}.")
        return
    for item in items:
        item_id = item.get('id')
        item_model = item.get('model')
        if not item_id or not item_model:
            logging.warning(f"Skipping invalid item: {item}")
            continue
        try:
            logging.info(f"Archiving item ID: {item_id}, Type: {item_model}")
            MTB.put(f'/api/{item_model}/{item_id}', json={'archived': True})
        except Exception as e:
            logging.error(f"Error archiving item ID {item_id}: {str(e)}")


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


def get_dashboard_names(language):
    """
    Returns localized names for the sub-collection and dashboards based on the selected language.
    """
    translations = {
        "fr": ("Tableaux de bord génériques", "Tableau de bord global 🌍", "Tableau de bord local 📍"),
        "nl": ("Generieke dashboards", "Globaal dashboard 🌍", "Lokaal dashboard 📍"),
        "en": ("Generic dashboards", "Global dashboard 🌍", "Local dashboard 📍"),
        "de": ("Generische Dashboards", "Globales Dashboard 🌍", "Lokales Dashboard 📍"),
    }

    return translations.get(language, translations["fr"])  # Default to French if language not found

