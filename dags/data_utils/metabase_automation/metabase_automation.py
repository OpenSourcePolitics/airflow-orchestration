from airflow.hooks.base import BaseHook
from metabase_api import Metabase_API
import logging
import json
import re
import requests

http_conn = BaseHook.get_connection("metabase_http")


class MetabaseClient:
    """
    Singleton class to initialize and manage Metabase API calls using an API token.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            http_conn = BaseHook.get_connection("metabase_http")
            token = http_conn.password
            if not token:
                raise ValueError("API token for Metabase is missing in the connection extras.")

            cls._instance = super(MetabaseClient, cls).__new__(cls)
            cls._instance.api = Metabase_API(http_conn.host, api_key=token)  # Use token instead of credentials
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


def get_generic_dashboard_names(language):
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


def get_card_by_name_in_collection(collection_id: int, card_name: str):
    """Return card object from a collection by its exact name."""
    items = MTB.get(f"/api/collection/{collection_id}/items", params={"limit": 1000})
    if not isinstance(items, dict) or "data" not in items:
        raise RuntimeError(f"Unexpected response while listing collection items: {items}")
    for it in items["data"]:
        if it.get("model") == "card" and it.get("name") == card_name:
            return MTB.get(f"/api/card/{it['id']}")
    return {}


def copy_card_to_collection(card_id: int, target_collection_id: int, new_name) -> int:
    """Copy an existing card into a target collection, then enforce the collection_id. Returns the new card ID."""
    payload = {
        "name": new_name,
        "collection_id": target_collection_id,
        "include_parameters": True,
        "is_deep_copy": True,
    }
    resp = MTB.post(f"/api/card/{card_id}/copy", json=payload)
    if not isinstance(resp, dict) or "id" not in resp:
        raise RuntimeError(f"Failed to copy card {card_id} → {target_collection_id}. Response: {resp}")
    new_id = resp["id"]
    logging.info(f"Card copied: src={card_id} -> dst={new_id} (collection {target_collection_id})")

    # Enforce destination collection and name
    new_card = MTB.get(f"/api/card/{new_id}")
    changed = False
    if new_card.get("collection_id") != target_collection_id:
        new_card["collection_id"] = target_collection_id
        changed = True
    if new_name and new_card.get("name") != new_name:
        new_card["name"] = new_name
        changed = True
    if new_card.get("archived"):
        new_card["archived"] = False
        changed = True

    if changed:
        upd = MTB.put(f"/api/card/{new_id}", json=new_card)
        if not upd:
            raise RuntimeError(f"Failed to enforce collection/name on copied card {new_id}")
        logging.info(f"Card {new_id} moved/normalized into collection {target_collection_id} with name '{new_name}'")

    return new_id


def replace_card_source_db(card_id: int, new_db_id: int, schema_name: str):
    """Switch a single card (SQL or query builder) to another database and remap table/field ids."""
    card = MTB.get(f"/api/card/{card_id}")
    if not card:
        raise RuntimeError(f"Card {card_id} not found for DB switch")

    # Set dataset_query.database
    modify_dict(card, ["dataset_query", "database"], new_db_id)

    table_id_mapping = {}
    if card.get("query_type") == "query":
        # query builder -> need old_db_id to build mapping (from source-table ids)
        old_db_id = card["dataset_query"].get("database")
        if not old_db_id:
            # Fallback: take database_id from card root if present
            old_db_id = card.get("database_id")
        if not old_db_id:
            raise RuntimeError(f"Cannot infer old_db_id for card {card_id}")

        # Build mapping for tables across schemas
        old_tables = get_tables_info(old_db_id)
        new_tables = get_tables_info(new_db_id, schema_name)
        for ot in old_tables:
            for nt in new_tables:
                if ot["table_name"] == nt["table_name"] and ot["schema"] == nt["schema"]:
                    table_id_mapping[ot["table_id"]] = nt["table_id"]

        # Update source-table + joins + field references
        if "table_id" in card:  # sometimes present
            if card["table_id"] in table_id_mapping:
                card["table_id"] = table_id_mapping[card["table_id"]]
        if "dataset_query" in card and "query" in card["dataset_query"]:
            q = card["dataset_query"]["query"]
            if "source-table" in q and q["source-table"] in table_id_mapping:
                q["source-table"] = table_id_mapping[q["source-table"]]
            for j in q.get("joins", []):
                if "source-table" in j and j["source-table"] in table_id_mapping:
                    j["source-table"] = table_id_mapping[j["source-table"]]
        card = update_object_fields(card, table_id_mapping)

    elif card.get("query_type") == "native":
        # native SQL: update template-tags object (fields / table refs inside tags)
        native = card.get("dataset_query", {}).get("native", {})
        if "template-tags" in native:
            # Build mapping using all tables of old/new DBs as above
            # We still need an old_db_id hint; use card.get("database_id") or dataset_query.database before override
            old_db_id = card.get("database_id")
            if not old_db_id:
                # If we already overwrote dataset_query.database above, you may need to fetch it first.
                logging.warning(f"Card {card_id}: could not find 'database_id' on root; template-tags mapping may be partial.")
            if old_db_id:
                old_tables = get_tables_info(old_db_id)
                new_tables = get_tables_info(new_db_id, schema_name)
                for ot in old_tables:
                    for nt in new_tables:
                        if ot["table_name"] == nt["table_name"] and ot["schema"] == nt["schema"]:
                            table_id_mapping[ot["table_id"]] = nt["table_id"]
            native["template-tags"] = update_object_fields(native.get("template-tags", {}), table_id_mapping)
            card["dataset_query"]["native"] = native

    resp = MTB.put(f"/api/card/{card_id}", json=card)
    if not resp:
        raise RuntimeError(f"Failed to update card {card_id} with new DB {new_db_id}")
    logging.info(f"Card {card_id} switched to DB {new_db_id} (schema: {schema_name})")



def get_users_by_email_map():
    """Return {lowercased_email: user_id} for all Metabase users.
    Handles both list responses and paginated dict responses {data: [...]}.
    """
    resp = MTB.get("/api/user")

    # Normalize to a list of user dicts
    if isinstance(resp, dict) and isinstance(resp.get("data"), list):
        users = resp["data"]
    elif isinstance(resp, list):
        users = resp
    else:
        logging.warning(f"/api/user returned unexpected payload shape: {type(resp)} -> {resp}")
        users = []

    out = {}
    for u in users:
        email = (u.get("email") or "").lower().strip()
        uid = u.get("id")
        if email and isinstance(uid, int):
            out[email] = uid

    logging.info(f"[get_users_by_email_map] mapped {len(out)} users by email")
    return out



def _post_json(url: str, headers: dict, payload: dict) -> tuple[int, str]:
    """POST JSON and return (status_code, text). Never swallows errors."""
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        return r.status_code, r.text
    except Exception as e:
        return -1, f"EXCEPTION: {e}"


def _metabase_session_headers_and_base():
    http_conn = BaseHook.get_connection("metabase_http")
    base = http_conn.host.rstrip("/")

    metabase_user_info = BaseHook.get_connection("metabase_user_info")
    username = metabase_user_info.login
    password = metabase_user_info.password

    if not username or not password:
        raise RuntimeError(
            "Missing Metabase username/password for session auth. "
        )

    resp = requests.post(
        f"{base}/api/session",
        json={"username": username, "password": password},
        timeout=30,
        headers={"Content-Type": "application/json"},
    )
    sess = resp.json().get("id")
    if not sess:
        raise RuntimeError(f"Metabase login did not return a session id: {resp.text}")

    headers = {
        "Content-Type": "application/json",
        "X-Metabase-Session": sess,
    }
    return base, headers


def create_notification_alert_for_card_from_emails(
    card_id: int,
    recipient_emails: list[str],
    cron_expr: str = "0 0 9 ? * MON",
    tz: str = "Europe/Paris",
    send_once: bool = True
) -> dict:
    email_map = get_users_by_email_map()
    norm = [e.lower().strip() for e in (recipient_emails or [])]
    user_ids = [email_map[e] for e in norm if e in email_map]
    unresolved = [e for e in norm if e not in email_map]
    if not user_ids:
        raise RuntimeError(
            f"No valid Metabase users resolved from recipient_emails={recipient_emails}. "
            f"Unresolved={unresolved}"
        )
    if unresolved:
        logging.warning(f"Some recipient emails could not be resolved and will be skipped: {unresolved}")

    recipients_user = [
        {"type": "notification-recipient/user", "user_id": uid}
        for uid in user_ids
    ]

    base, headers = _metabase_session_headers_and_base()
    url_notif = f"{base}/api/notification"

    payload = {
        "payload_type": "notification/card",
        "payload": {
            "card_id": card_id,
            "send_condition": "has_result",
            "send_once": send_once
        },
        "handlers": [
            {
                "active": True,
                "channel_type": "channel/email",
                "recipients": recipients_user
            }
        ],
        "subscriptions": [
            {
                "type": "notification-subscription/cron",
                "cron_schedule": cron_expr,
                "timezone": tz
            }
        ]
    }
    logging.info(f"POST {url_notif} payload: {payload}")
    sc, body = _post_json(url_notif, headers, payload)
    logging.info(f"/api/notification -> status={sc}, body={body}")
    if 200 <= sc < 300 and body:
        return json.loads(body)

    raise RuntimeError(f"Failed to create notification. status={sc}, body={body}")

