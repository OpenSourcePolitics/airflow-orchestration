from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook


def get_postgres_connection(database):
    """Extracts PostgreSQL connection details from Airflow and establishes a connection."""
    try:
        # Retrieve the connection object using Airflow's BaseHook
        connection = BaseHook.get_connection('matomo_postgres')

        # Extract connection details
        user = connection.login
        password = connection.password
        host = connection.host
        port = connection.port

        # Create the SQLAlchemy engine
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        conn = engine.connect()
        print("Successfully connected to the PostgreSQL database using Airflow connection.")
        return conn

    except Exception as e:
        print(f"Failed to connect to PostgreSQL using Airflow connection: {e}")
        raise  # Raise exception to ensure the DAG fails if the connection cannot be established


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
def dump_data_to_postgres(connection, data, table_name):
    """Dumps the DataFrame into the specified PostgreSQL table."""
    try:
        data.to_sql(table_name, connection, if_exists='append', index=False)
        print(f"Data for {table_name} dumped successfully into the table.")
    except Exception as e:
        print(f"Failed to dump data into {table_name}: {e}")