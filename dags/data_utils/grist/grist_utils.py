from airflow.hooks.base import BaseHook
from grist_api import GristDocAPI
import pandas as pd
from airflow.models import Variable

# Retrieve the connection object using Airflow's BaseHook
connection = BaseHook.get_connection("grist_osp")
grist_api_key = connection.password
grist_server = connection.host


def fetch_grist_table_data(doc_id, table_name, errors="coerce"):
    """
    Fetch data from a Grist table and return it as a pandas DataFrame with type validation.

    Args:
        doc_id (str): The Grist document ID to fetch data from
        table_name (str): Name of the Grist table to fetch data from

    Returns:
        pandas.DataFrame: The fetched data from Grist with validated data types
    """
    # Create Grist API instance with the provided document ID
    api = GristDocAPI(doc_id, server=grist_server, api_key=grist_api_key)

    # Fetch data from Grist table
    data = api.fetch_table(table_name)
    df = pd.DataFrame(data)

    # Validate and clean data types
    # Grist does not have strict typing: string values can
    # be stored in numerical columns. To avoir sql errors,
    # we replace these values by none.
    for column in df.columns:
        # Handle numeric columns - convert invalid values to NaN
        if df[column].dtype == "object":
            # Try to convert to numeric, replacing non-numeric values with NaN
            df[column] = pd.to_numeric(df[column], errors=errors)

    return df
