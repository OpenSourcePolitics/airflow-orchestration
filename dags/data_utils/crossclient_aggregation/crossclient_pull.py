import pandas as pd
from ..postgres_helper.client_db_list import database_name
from ..postgres_helper.postgres_helper import dump_data_to_postgres, get_postgres_connection

def retrieve_all_clients_data(query, connection):

    df = pd.read_sql(query, connection)
    return df

def aggregate_data_by_client(client, df):
    df = df.assign(client=client)
    df.insert(0, 'client', df.pop('client'))
    return df

def fetch_crossclient_data(frames, query, client, fetch_db_name):
    engine = get_postgres_connection("main_db_cluster_name", fetch_db_name)
    connection = engine.connect()

    df = retrieve_all_clients_data(query, connection)
    df = aggregate_data_by_client(client, df)

    connection.close()

def create_aggregated_tables(queries, clients):
    for query in queries:
        frames = []
        for client in clients:
            fetch_db_name = database_name[client]
            df = fetch_crossclient_data(query, client, fetch_db_name)
            frames.append(df)
            result = pd.concat(frames)
            
        table_name = f'aggregate_by_{query}'
        engine = get_postgres_connection("main_db_cluster_name", dump_db_name='aggregated_client_data')
        connection = engine.connect()
        dump_data_to_postgres(connection, result, table_name, schema='public', if_exists='replace')
        connection.close()
