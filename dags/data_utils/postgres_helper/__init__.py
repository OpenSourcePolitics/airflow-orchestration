from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook
import pandas as pd
from typing import Literal

def get_postgres_connection(connection_name, database):
    """Extracts PostgreSQL connection details from Airflow and establishes a connection."""
    try:
        # Retrieve the connection object using Airflow's BaseHook
        connection = BaseHook.get_connection(connection_name)

        # Extract connection details
        user = connection.login
        password = connection.password
        host = connection.host
        port = connection.port

        # Create the SQLAlchemy engine
        return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

    except Exception as e:
        print(f"Failed to connect to PostgreSQL using Airflow connection: {e}")
        raise  # Raise exception to ensure the DAG fails if the connection cannot be established


# Clean data in PostgreSQL within the date range
def drop_table_in_postgres(connection, table_name):
    """
    Drops the table if it exists.
    Note: this function should not be used unless stricly necessary.
    Droping tables are really rare event, and we rarely want to automate them.
    Prefer to use `dump_data_to_postgres` with the `if_exists="replace"` argument.
    """
    try:
        # Check if table exists and drop it only if it does
        drop_query = text(f"DROP TABLE IF EXISTS {table_name}")
        connection.execute(drop_query)
        print(f"Dropped table {table_name} if it existed")
    except Exception as e:
        print(f"Failed to drop table {table_name}: {e}")


# Dump DataFrame to PostgreSQL table
def dump_data_to_postgres(
    connection, data: pd.DataFrame, table_name, schema="public", if_exists: Literal["fail", "replace", "append"]="append", dtype=None
):
    """Dumps the DataFrame into the specified PostgreSQL table."""
    try:
        data.to_sql(
            table_name, connection, schema=schema, if_exists=if_exists, index=False, dtype=dtype)
        print(f"Data for {table_name} dumped successfully into the table.")
    except Exception as e:
        print(f"Failed to dump data into {table_name}: {e}")
        raise


# Clean data in PostgreSQL within the date range
def clean_data_in_postgres(connection, table_name, start_date, end_date):
    """Deletes rows in the table where the 'date' is between the start_date and end_date."""
    try:
        delete_query = text(
            f"DELETE FROM {table_name} WHERE date BETWEEN :start_date AND :end_date"
        )
        connection.execute(
            delete_query, {"start_date": start_date, "end_date": end_date}
        )
        print(f"Cleaned data in {table_name} between {start_date} and {end_date}.")
    except Exception as e:
        print(f"Failed to clean data in {table_name}: {e}")
