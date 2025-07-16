from airflow.hooks.base import BaseHook
from grist_api import GristDocAPI
import pandas as pd
from ..postgres_helper.postgres_helper import get_postgres_connection
from airflow.models import Variable

# Retrieve the connection object using Airflow's BaseHook
connection = BaseHook.get_connection("grist_osp")
grist_api_key = connection.password
grist_server = connection.host
grist_cdc_doc_id = Variable.get("grist_cdc_doc_id")

# Get api key from your Profile Settings, and run with GRIST_API_KEY=<key>
api = GristDocAPI(grist_cdc_doc_id, server=grist_server, api_key=grist_api_key)

table_name = "Propositions_brutes"

def retrieve_sql_data(connection):
    query = f"""
                SELECT
                *
                FROM prod.proposals
                ORDER BY id DESC
            """
    df = pd.read_sql(query, connection)
    connection.close()

    return df

def dump_to_grist(df):
    #record_ids = (get a list of record ids from the existing Grist table)
    #api.delete_records(table_name, record_ids, chunk_size=None)
    #record_dicts = df.to_dict()
    #api.add_records(table_name, record_dicts, chunk_size=None)

    #new data should be list of objects with column IDs as attributes
    #(e.g. namedtuple or sqlalchemy result rows)
    new_data : pd.to_sql(df)
    api.sync_table(table_name, new_data, key_cols="id", other_cols, grist_fetch=None, chunk_size=None, filters=None)

    #json_data = df.to_json()
    #call(url="https://{grist_server}/api/docs/{grist_cdc_doc_id}/tables/{table_name}/records?allow_empty_require=True", json_data, method="PUT", prefix=None)[
    

def fetch_and_dump_data(clients):

    client = "cdc"
    db_name = clients[client]["postgres"]["database_name"]

    engine = get_postgres_connection("main_db_cluster_name", db_name)
    connection = engine.connect()

    df = retrieve_sql_data(connection)

    dump_to_grist(df)

