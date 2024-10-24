from sqlalchemy import text

from ..postgres.get_postgres_connection import get_postgres_connection
import pandas as pd

db_cluster_preprod = 'db_cluster_name_data'
db_cluster_prod = 'db_cluster_name_data_prod'

def execute_queries(connection, queries):
    """
    Executes a list of queries on the specified connection.
    """
    results = {}
    for query_name, query in queries.items():
        try:
            result = connection.execute(text(query))
            count = result.fetchone()[0]
            results[query_name] = count
        except Exception as e:
            print(f"Failed to execute query {query_name}: {e}")
            results[query_name] = None  # Return None if the query fails
    return results


def aggregate_data_for_clients(db_cluster, client_databases, queries):
    """
    Performs data aggregation for each client and returns a DataFrame.
    """
    data = []

    for client_db in client_databases:
        connection = get_postgres_connection(db_cluster, client_db)
        client_results = execute_queries(connection, queries)
        client_results['client'] = client_db  # Add client name to results
        data.append(client_results)
        connection.close()  # Close the connection after query execution

    # Convert results to DataFrame
    df = pd.DataFrame(data)
    return df

def aggregate_data_for_clients_for_unique_query(db_cluster, client_databases, query):
    """
    Performs data aggregation for each client and returns a DataFrame.
    """
    data = []

    for client_db in client_databases:
        connection = get_postgres_connection(db_cluster, client_db)
        result = connection.execute(text(query))
        df = pd.DataFrame(result)
        df['client'] = client_db  # Add client name to results
        data.append(df)
        connection.close()  # Close the connection after query execution

    # Convert results to DataFrame
    df = pd.concat(data)
    return df

def clean_data_in_postgres(connection, table):
    """Deletes rows in the table."""
    try:
        delete_query = text(
            f"DELETE FROM {table};"
        )

        connection.execute(delete_query)
        print(f"Cleaned data in table {table}")
    except Exception as e:
        print(f"Failed to clean data in table {table}: {e}")

def insert_data_to_aggregated_db(db_cluster, dataframe, target_database, target_table):
    """
    Inserts the aggregated data into a target PostgreSQL table.
    """
    try:
        connection = get_postgres_connection(db_cluster, target_database)
        clean_data_in_postgres(connection, target_table)
        dataframe.to_sql(target_table, con=connection, if_exists='replace', index=False)
        print(f"Data successfully inserted into {target_table} in {target_database}.")
        connection.close()
    except Exception as e:
        print(f"Failed to insert data into {target_table}: {e}")
        raise


def perform_and_insert_aggregated_data():
    client_databases = ["angers", "cdc", "cea", "cese", "grand_nancy", "lyon", "marseille", "meyzieu", "sytral", "thionville", "toulouse", "tours", "valbonne"]

    # List of aggregation queries
    queries = {
        'user_count': "SELECT COUNT(*) AS user_count FROM prod.users;",
        'participating_user_count': "SELECT COUNT(*) AS participating_user_count FROM prod.users WHERE has_answered_survey OR is_endorsing OR is_following OR has_authored_comment OR has_authored_proposal OR has_voted_on_project OR has_voted_on_proposal;",
        'participatory_process_count': "SELECT COUNT(*) AS participatory_process_count FROM prod.stg_decidim_participatory_processes;",
        'participations_count': "SELECT COUNT(*) AS participations_count FROM prod.participations WHERE participation_type IS NOT NULL;",
    }

    # Perform data aggregation for all clients
    aggregated_data = aggregate_data_for_clients(db_cluster_prod, client_databases, queries)

    # Display the aggregated data (optional)
    print(aggregated_data.head(5))

    # Insert the aggregated data into a new database and table
    target_database = "aggregated_client_data"
    target_table = "aggregated_data"

    insert_data_to_aggregated_db(db_cluster_preprod, aggregated_data, target_database, target_table)

def aggregate_by_date_task():
    client_databases = ["angers", "cdc", "cea", "cese", "grand_nancy", "lyon", "marseille", "meyzieu", "sytral", "thionville", "toulouse", "tours", "valbonne"]
    
    query = """
            SELECT 
                COUNT(*) as users_count,
                DATE(created_at) AS date_of_creation
            FROM prod.users
            GROUP BY date_of_creation
            """

    target_database = "aggregated_client_data"

    target_table = "aggregate_by_date"
    df = aggregate_data_for_clients_for_unique_query(db_cluster_prod, client_databases, query)
    insert_data_to_aggregated_db(db_cluster_preprod, df, target_database, target_table)

def aggregate_by_participation_type_task():
    client_databases = ["angers", "cdc", "cea", "cese", "grand_nancy", "lyon", "marseille", "meyzieu", "sytral", "thionville", "toulouse", "tours", "valbonne"]
    
    query = """
            SELECT 
                COUNT (*),
                participation_type
            FROM prod.participations
            GROUP BY participation_type
            """

    target_database = "aggregated_client_data"
    target_table = "aggregate_by_participation_type"
    df = aggregate_data_for_clients_for_unique_query(db_cluster_prod, client_databases, query)
    insert_data_to_aggregated_db(db_cluster_preprod, df, target_database, target_table)