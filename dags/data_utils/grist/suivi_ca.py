from .grist_helper import _get_grist_api
import pandas as pd
from ..postgres_helper import (
    dump_data_to_postgres,
    get_postgres_connection,
)
from airflow.models import Variable

# Retrieve the connection object using Airflow's BaseHook
grist_ca_doc_id = Variable.get("grist_suivi_ca_doc_id")


def fetch_and_dump_data(connection_name):
    api = _get_grist_api("grist_osp", grist_ca_doc_id)
    data = api.fetch_table("SUIVI_CLIENTS")
    df = pd.DataFrame(data)

    engine = get_postgres_connection(connection_name, "aggregated_client_data")
    connection = engine.connect()
    table_name = "grist_suivi_ca"

    dump_data_to_postgres(connection, df, table_name, if_exists="replace")

    connection.close()
