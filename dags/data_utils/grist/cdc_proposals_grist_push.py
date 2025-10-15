from airflow.hooks.base import BaseHook
from grist_api import GristDocAPI
from sqlalchemy import text
from ..postgres_helper import get_postgres_connection
from airflow.models import Variable

# Retrieve the connection object using Airflow's BaseHook
connection = BaseHook.get_connection("grist_cdc")
grist_api_key = connection.password
grist_server = connection.host
grist_cdc_doc_id = Variable.get("grist_cdc_doc_id")

# Get api key from your Profile Settings, and run with GRIST_API_KEY=<key>
api = GristDocAPI(grist_cdc_doc_id, server=grist_server, api_key=grist_api_key)

table_name = "Propositions_Decidim"

def retrieve_sql_data(engine):
    query = f"""
                SELECT
                    id,
                    decidim_participatory_space_slug,
                    title,
                    body,
                    url,
                    translated_state,
                    first_category,
                    comments_count,
                    endorsements_count,
                    CURRENT_TIMESTAMP(3) as imported_at
                FROM prod.proposals
                ORDER BY id
            """
    with engine.connect() as connection:
        result = connection.execute(text(query))
        rows_to_dump = result.all()
    connection.close()

    return rows_to_dump

def dump_to_grist(rows_to_dump):
    new_data = rows_to_dump
    key_cols = [["proposal_id", "id", "Numeric"]]
    other_cols = [
                    ("Consultation", "decidim_participatory_space_slug", "Text"),
                    ("Titre_de_la_contribution", "title", "Text"),
                    ("Corps_de_la_contribution", "body", "Text"),
                    ("url", "url", "Text"),
                    ("Etat", "translated_state", "Text"),
                    ("Categorie", "first_category", "Text"),
                    ("Nombre_de_commentaires", "comments_count", "Numeric"),
                    ("Nombre_de_soutiens", "endorsements_count", "Numeric"),
                    ("Date_d_update_de_la_ligne", "imported_at", "DateTime")
                ]
    api.sync_table(table_name, new_data, key_cols, other_cols, grist_fetch=None, chunk_size=200, filters=None)    

def fetch_and_dump_cdc_data(connection_name, clients):

    client = "cdc"
    db_name = clients[client]["postgres"]["database_name"]
    engine = get_postgres_connection(connection_name, db_name)
    rows_to_dump = retrieve_sql_data(engine)
    dump_to_grist(rows_to_dump)