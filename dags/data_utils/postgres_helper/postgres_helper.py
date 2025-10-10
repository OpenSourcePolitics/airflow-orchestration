from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook
import logging


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
    """Drops the table if it exists."""
    try:
        # Check if table exists and drop it only if it does
        drop_query = text(f"DROP TABLE IF EXISTS {table_name}")
        connection.execute(drop_query)
        print(f"Dropped table {table_name} if it existed")
    except Exception as e:
        print(f"Failed to drop table {table_name}: {e}")


# Dump DataFrame to PostgreSQL table
def dump_data_to_postgres(
    connection, data, table_name, schema="public", if_exists="append"
):
    """Dumps the DataFrame into the specified PostgreSQL table."""
    try:
        data.to_sql(
            table_name, connection, schema=schema, if_exists=if_exists, index=False
        )
        print(f"Data for {table_name} dumped successfully into the table.")
    except Exception as e:
        print(f"Failed to dump data into {table_name}: {e}")
        raise
