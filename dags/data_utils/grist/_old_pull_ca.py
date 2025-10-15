from ..postgres_helper import (
    dump_data_to_postgres,
    get_postgres_connection,
)
from airflow.models import Variable
from .grist_helper import fetch_grist_table_data

grist_ca_doc_id = Variable.get("grist_ca_doc_id")


def fetch_and_dump_data(connection_name):
    # Fetch data from Grist using the new utility function
    df = fetch_grist_table_data(grist_ca_doc_id, "Suivi_CA_par_clients")

    df["Prestations_2024"] = df["Prestations_2024"].astype(str)

    # Add boolean columns for each field
    fields = [
        "Abo Decidim",
        "Abo Grist",
        "Abo Metabase",
        "Bénévolat",
        "Conseil Decidim",
        "Conseil Grist",
        "Conseil Metabase",
        "Synthèses",
        "Technique Decidim",
        "Technique Metabase",
        "Terminé",
    ]

    # Ajouter des colonnes booléennes pour chaque champ
    for field in fields:
        df[field] = df["Prestations_2024"].apply(lambda x: field in x)

    engine = get_postgres_connection(connection_name, "aggregated_client_data")
    connection = engine.connect()
    table_name = "grist_test_ca"

    dump_data_to_postgres(connection, df, table_name, if_exists="replace")

    connection.close()
