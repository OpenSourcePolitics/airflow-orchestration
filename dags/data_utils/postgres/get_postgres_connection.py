from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

def get_postgres_connection(db_cluster, database):
    """
    Extracts PostgreSQL connection details from Airflow and establishes a connection using SQLAlchemy.
    """
    try:
        # Retrieve the connection object from Airflow
        connection = BaseHook.get_connection(db_cluster)

        # Extract connection details
        user = connection.login
        password = connection.password
        host = connection.host
        port = connection.port

        # Create the SQLAlchemy engine
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        conn = engine.connect()
        print("Successfully connected to the PostgreSQL database via Airflow.")
        return conn

    except Exception as e:
        print(f"Failed to connect to PostgreSQL via Airflow: {e}")
        raise  # Raise exception to ensure the DAG fails if the connection cannot be established
