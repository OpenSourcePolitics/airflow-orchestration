from airflow.models import Variable

from ..postgres_helper import (
    dump_data_to_postgres,
    get_postgres_connection,
)
from .grist_helper import fetch_grist_table_data, get_grist_api

# Retrieve the connection object using Airflow's BaseHook
grist_ca_doc_id = Variable.get("grist_suivi_ca_doc_id")
if not isinstance(grist_ca_doc_id, str):
    raise ValueError("grist_suivi_ca_doc_id variable not set")
api = get_grist_api("grist_osp", grist_ca_doc_id)


def fetch_and_dump_data(connection_name):
    df = fetch_grist_table_data(api, "SUIVI_CLIENTS")

    engine = get_postgres_connection(connection_name, "aggregated_client_data")
    connection = engine.connect()
    table_name = "grist_suivi_ca"

    dump_data_to_postgres(connection, df, table_name, if_exists="replace")

    connection.close()
