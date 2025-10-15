from airflow.hooks.base import BaseHook
from grist_api import GristDocAPI
import pandas as pd
from ..postgres_helper import (
    dump_data_to_postgres,
    get_postgres_connection,
)
from airflow.models import Variable

# Retrieve the connection object using Airflow's BaseHook
connection = BaseHook.get_connection("grist_osp")
grist_api_key = connection.password
grist_server = connection.host
grist_ca_doc_id = Variable.get("grist_suivi_ca_doc_id")

# Get api key from your Profile Settings, and run with GRIST_API_KEY=<key>
api = GristDocAPI(grist_ca_doc_id, server=grist_server, api_key=grist_api_key)


def fetch_and_dump_data(connection_name):
    data = api.fetch_table("SUIVI_CLIENTS")
    df = pd.DataFrame(data)

    engine = get_postgres_connection(connection_name, "aggregated_client_data")
    connection = engine.connect()
    table_name = "grist_suivi_ca"

    dump_data_to_postgres(connection, df, table_name, if_exists="replace")

    connection.close()
