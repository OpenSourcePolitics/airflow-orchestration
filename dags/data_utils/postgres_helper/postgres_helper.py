from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook
import logging



def get_postgres_connection(connection_name, database):
    """Extracts PostgreSQL connection details from Airflow and establishes a connection."""
    try:
        logger = logging.getLogger(__name__)

        # Retrieve the connection object using Airflow's BaseHook
        connection = BaseHook.get_connection(connection_name)

        logger.warn(f":DEBUG: get_postgres_connection> Login : {connection.login}")
        logger.warn(f":DEBUG: get_postgres_connection> Password : {connection.password}")
        logger.warn(f":DEBUG: get_postgres_connection> Host : {connection.host}")
        logger.warn(f":DEBUG: get_postgres_connection> Port : {connection.port}")
        logger.warn(f":DEBUG: get_postgres_connection> postgresql://{user}:{password}@{host}:{port}/{database}")
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
    """Deletes rows in the table where the 'date' is between the start_date and end_date."""
    try:
        delete_query = text(
            f"DROP TABLE {table_name}"
        )
        connection.execute(delete_query)
        print(f"Cleaned data in {table_name}")
    except Exception as e:
        print(f"Failed to clean data in {table_name}: {e}")


# Clean data in PostgreSQL within the date range
def clean_data_in_postgres(connection, table_name, start_date, end_date):
    """Deletes rows in the table where the 'date' is between the start_date and end_date."""
    try:
        delete_query = text(
            f"DELETE FROM {table_name} WHERE date BETWEEN :start_date AND :end_date"
        )
        connection.execute(delete_query, {'start_date': start_date, 'end_date': end_date})
        print(f"Cleaned data in {table_name} between {start_date} and {end_date}.")
    except Exception as e:
        print(f"Failed to clean data in {table_name}: {e}")


# Dump DataFrame to PostgreSQL table
def dump_data_to_postgres(connection, data, table_name, schema='public', if_exists='append'):
    """Dumps the DataFrame into the specified PostgreSQL table."""
    try:
        data.to_sql(table_name, connection, schema=schema, if_exists=if_exists, index=False)
        print(f"Data for {table_name} dumped successfully into the table.")
    except Exception as e:
        print(f"Failed to dump data into {table_name}: {e}")
        raise
