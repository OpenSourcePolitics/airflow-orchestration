from airflow.hooks.base import BaseHook
from grist_api import GristDocAPI
import pandas as pd
from ..postgres_helper.postgres_helper import dump_data_to_postgres, get_postgres_connection, drop_table_in_postgres
from airflow.models import Variable

# Retrieve the connection object using Airflow's BaseHook
connection = BaseHook.get_connection("grist_osp")
grist_api_key = connection.password
grist_server = connection.host
grist_ca_doc_id = Variable.get("grist_all_clients_doc_id")

# Get api key from your Profile Settings, and run with GRIST_API_KEY=<key>
api = GristDocAPI(grist_ca_doc_id, server=grist_server, api_key=grist_api_key)


def fetch_and_dump_data(connection_name):
    data = api.fetch_table('Liste_de_tous_les_clients')
    df = pd.DataFrame(data)

    df['Prestations_2025'] = df['Prestations_2025'].astype(str)

    # Add boolean columns for each field
    fields = ['Abo Decidim', 'Abo Grist', 'Abo Metabase', 'Bénévolat',
              'Conseil Decidim', 'Conseil Grist', 'Conseil Metabase',
              'Synthèses', 'Technique Decidim', 'Technique Metabase', 'Terminé']

    # Ajouter des colonnes booléennes pour chaque champ
    for field in fields:
        df[field] = df['Prestations_2025'].apply(lambda x: field in x)

    engine = get_postgres_connection(connection_name, "aggregated_client_data")
    connection = engine.connect()
    table_name = "all_clients"

    drop_table_in_postgres(connection, table_name)
    dump_data_to_postgres(connection, df, table_name)

    connection.close()
