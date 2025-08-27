from keycloak import KeycloakOpenIDConnection, KeycloakAdmin
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import pandas as pd
from types import SimpleNamespace
from grist_api import GristDocAPI
import requests

# Retrieve the connection object using Airflow's BaseHook
grist_connection = BaseHook.get_connection("grist_osp")
grist_api_key = grist_connection.password
grist_server = grist_connection.host
grist_commercial_doc_id = Variable.get("grist_commercial_doc_id")

keycloak_connection = BaseHook.get_connection("keycloak_demo_suite")
keycloak_server_url = keycloak_connection.host
keycloak_username = keycloak_connection.login
keycloak_password = keycloak_connection.password

webhook_url = Variable.get("n8n_webhook_prospect_demo_suite_coop")
grist_table_name = Variable.get("prospects_demo_suite_coop")

def init_keycloak_admin(
        server_url: str,
        username: str,
        password: str,
        realm_name: str,
        client_id: str = "admin-cli",
        user_realm_name: str = "master",
) -> KeycloakAdmin:
    keycloak_connection = KeycloakOpenIDConnection(
        server_url=server_url,
        username=username,
        password=password,
        realm_name=realm_name,
        verify=True,
        client_id=client_id,
        user_realm_name=user_realm_name,
    )

    return KeycloakAdmin(connection=keycloak_connection)


def format_users_dataframe(users: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(users)

    cols = ["username", "firstName", "lastName", "email", "emailVerified", "createdTimestamp"]
    df = df[cols]
    df.columns = ["pseudo", "prenom", "nom", "email", "email_verifie", "date_de_creation"]

    if "date_de_creation" in df.columns:
        df["date_de_creation"] = (
            pd.to_datetime(df["date_de_creation"], unit="ms", utc=True)
              .dt.strftime("%d-%m-%Y")
        )

    return df


def dump_df_to_grist_table(
    api: GristDocAPI,
    table_name: str,
    df: pd.DataFrame,
    chunk_size: int = 200,
) -> None:
    """
    Push a pandas DataFrame into a Grist document table using sync_table.

    IMPORTANT:
    py_grist_api.sync_table interprets each colspec as a structure whose
    *second* item is the attribute name to read from each row via getattr(row, ncol).
    Therefore, rows must be objects exposing attributes named exactly like
    the 2nd element of each colspec (e.g., 'email', 'prenom', ... below).
    """

    key_cols = [
        ["Email", "email", "Text"],
        ["Prenom", "prenom", "Text"],
        ["Nom", "nom", "Text"],
    ]
    other_cols = [
        ("Email_verifie", "email_verifie", "Toggle"),
        ("Date_de_creation", "date_de_creation", "Date"),
    ]

    df_to_attr = {
        "email": "email",
        "prenom": "prenom",
        "nom": "nom",
        "pseudo": "pseudo",
        "email_verifie": "email_verifie",
        "date_de_creation": "date_de_creation",
    }

    # Build list of objects exposing those attributes
    records = []
    safe_df = df.replace({pd.NA: None})
    for _, row in safe_df.iterrows():
        payload = {}
        for df_col, attr_name in df_to_attr.items():
            payload[attr_name] = row.get(df_col, None)
        records.append(SimpleNamespace(**payload))

    # Sync to Grist (rows are objects, so getattr(row, <2nd colspec item>) will work)
    api.sync_table(
        table_name,
        records,
        key_cols,
        other_cols,
        grist_fetch=None,
        chunk_size=chunk_size,
        filters=None,
    )


def find_new_prospects(df_keycloak: pd.DataFrame, df_grist_existing: pd.DataFrame) -> pd.DataFrame:
    """
    Compare two DataFrames and return prospects present in Keycloak but not in Grist.
    Matching on (email, prenom, nom).
    """
    # Normalize
    for df in (df_keycloak, df_grist_existing):
        for col in ("email", "prenom", "nom"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.lower()
            else:
                df[col] = None

    merged = df_keycloak.merge(
        df_grist_existing,
        on=["email", "prenom", "nom"],
        how="left",
        indicator=True
    )

    # Keep only rows that don’t exist in Grist
    new_df = merged[merged["_merge"] == "left_only"]

    # Return the useful fields
    return new_df[["email", "prenom", "nom"]].reset_index(drop=True)


def send_new_prospects_to_n8n(webhook_url: str, new_df: pd.DataFrame, timeout: int = 10) -> None:
    """
    Send one POST request per new prospect to an n8n webhook.
    The payload JSON contains: email, prenom, nom.
    """
    if new_df.empty:
        return

    # Convert to list of dicts
    payloads = new_df.replace({pd.NA: None}).to_dict(orient="records")

    for p in payloads:
        # Minimal JSON body expected by your n8n workflow
        body = {
            "email":  p.get("email"),
            "prenom": p.get("prenom"),
            "nom":    p.get("nom"),
        }
        # You can add headers or auth if your webhook requires it
        r = requests.post(webhook_url, json=body, timeout=timeout)
        r.raise_for_status()


def fetch_existing_grist_prospects(api: GristDocAPI, table_name: str) -> pd.DataFrame:
    """
    Fetch current rows from a Grist table and return them as a pandas DataFrame
    with normalized column names to match our Keycloak DF:
      Grist IDs -> DataFrame columns
      Email     -> email
      Prenom    -> prenom
      Nom       -> nom
      Pseudo    -> pseudo   (optional, if present)
    """
    rows = api.fetch_table(table_name)
    data = pd.DataFrame(rows)
    data = data[["Email", "Prenom", "Nom"]]
    data.columns = ["email", "prenom", "nom"]
    return pd.DataFrame(data)



def fetch_data_from_keycloak_and_dump_to_grist():
    api = GristDocAPI(grist_commercial_doc_id, server=grist_server, api_key=grist_api_key)
    existing_df = fetch_existing_grist_prospects(api, grist_table_name)

    kc = init_keycloak_admin(
        server_url=keycloak_server_url,
        username=keycloak_username,
        password=keycloak_password,
        realm_name="demo",
    )
    users = kc.get_users()
    df = format_users_dataframe(users)
    dump_df_to_grist_table(api, grist_table_name, df)

    new_df = find_new_prospects(df, existing_df)
    send_new_prospects_to_n8n(webhook_url, new_df)