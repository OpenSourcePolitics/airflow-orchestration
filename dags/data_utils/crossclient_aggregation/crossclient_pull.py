import pandas as pd
from ..postgres_helper import dump_data_to_postgres, get_postgres_connection
import logging

def retrieve_all_clients_data(query, connection):

    return pd.read_sql(query, connection)

def aggregate_data_by_client(client, df):
    df = df.assign(client=client)
    df.insert(0, 'client', df.pop('client'))
    return df

def fetch_crossclient_data(query, client, fetch_db_name):
    engine = get_postgres_connection("main_db_cluster_name", fetch_db_name)
    connection = engine.connect()

    df = retrieve_all_clients_data(query, connection)
    df = aggregate_data_by_client(client, df)

    connection.close()

    return df

def create_aggregated_tables(queries, clients):
    logger = logging.getLogger(__name__)
    logger.warn(":DEBUG: create_aggregated_tables> Starting process")
    logger.warn(f":DEBUG: create_aggregated_tables> There is {len(queries)} queries")
    for query_key in queries:
        logger.warn(f":DEBUG: create_aggregated_tables> Executing query : '{query_key}': '{queries[query_key]}'")
        query = queries[query_key]
        frames = []
        for client in clients.keys():
            fetch_db_name = clients[client]["postgres"]["database_name"]
            logger.warn(f":DEBUG: create_aggregated_tables> Client: {client}")
            logger.warn(f":DEBUG: create_aggregated_tables> DB Name: {fetch_db_name}")
            df = fetch_crossclient_data(query, client, fetch_db_name)
            frames.append(df)
        
        result = pd.concat(frames)

        table_name = f'aggregate_by_{query_key}'
        engine = get_postgres_connection("main_db_cluster_name", "aggregated_client_data")
        connection = engine.connect()
        dump_data_to_postgres(connection, result, table_name, schema='public', if_exists='replace')
        connection.close()
    logger.warn(":DEBUG: create_aggregated_tables> Task terminated.")

