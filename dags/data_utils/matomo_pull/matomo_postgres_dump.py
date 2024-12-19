from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook
from sqlalchemy import inspect


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
    """
    Dumps the DataFrame into the specified PostgreSQL table, creating missing columns if necessary.

    Parameters:
    connection: SQLAlchemy engine or connection object
        The connection to the PostgreSQL database.
    data: pandas.DataFrame
        The DataFrame containing the data to be dumped.
    table_name: str
        The name of the PostgreSQL table to insert the data into.
    """

    # Convert DataFrame columns to lowercase
    data.columns = [col.lower() for col in data.columns]

    try:
        # Inspect the existing columns in the table
        inspector = inspect(connection)
        existing_columns = []
        if table_name in inspector.get_table_names():
            existing_columns = [col['name'] for col in inspector.get_columns(table_name)]


        # Identify missing columns
        missing_columns = set(data.columns) - set(existing_columns)

        # Add missing columns to the table
        for column in missing_columns:
            dtype = data[column].dtype
            if dtype == 'int64':
                sql_type = 'INTEGER'
            elif dtype == 'float64':
                sql_type = 'FLOAT'
            elif dtype == 'bool':
                sql_type = 'BOOLEAN'
            elif dtype == 'datetime64[ns]':
                sql_type = 'TIMESTAMP'
            else:
                sql_type = 'TEXT'

            alter_query = f'ALTER TABLE {table_name} ADD COLUMN {column} {sql_type};'
            try:
                connection.execute(alter_query)
                print(f"Column added: {column} ({sql_type})")
            except Exception as e:
                print(f"Error adding column {column}: {e}")

        # Insert the data into the table
        data.to_sql(table_name, connection, if_exists='append', index=False)
        print(f"Data for {table_name} dumped successfully into the table.")

    except Exception as e:
        # Log the error if the data dump fails
        print(f"Failed to dump data into {table_name}: {e}")
