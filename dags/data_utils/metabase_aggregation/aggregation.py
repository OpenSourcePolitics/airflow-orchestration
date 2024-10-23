from sqlalchemy import text

from ..postgres.get_postgres_connection import get_postgres_connection
import pandas as pd

db_cluster = 'db_cluster_name_data'


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


def aggregate_data_for_clients(client_databases, queries):
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

def clean_data_in_postgres(connection):
    """Deletes rows in the table where the 'date' is between the start_date and end_date."""
    try:
        delete_query = text(
            f"DELETE FROM aggregated_data;"
        )

        connection.execute(delete_query)
        print(f"Cleaned data in aggregated_data")
    except Exception as e:
        print(f"Failed to clean data in aggregated_data: {e}")

def insert_data_to_aggregated_db(dataframe, target_database, target_table):
    """
    Inserts the aggregated data into a target PostgreSQL table.
    """
    try:
        connection = get_postgres_connection(db_cluster, target_database)
        clean_data_in_postgres(connection)
        dataframe.to_sql(target_table, con=connection, if_exists='replace', index=False)
        print(f"Data successfully inserted into {target_table} in {target_database}.")
        connection.close()
    except Exception as e:
        print(f"Failed to insert data into {target_table}: {e}")
        raise


def perform_and_insert_aggregated_data():
    client_databases = ["lyon", "marseille", "toulouse", "grand_nancy", "tours"]

    # List of aggregation queries
    queries = {
        'user_count': "SELECT COUNT(*) AS user_count FROM prod.users;",
        'participating_user_count': "SELECT COUNT(*) AS participating_user_count FROM prod.users WHERE has_answered_survey OR is_endorsing OR is_following OR has_authored_comment OR has_authored_proposal OR has_voted_on_project OR has_voted_on_proposal;",
        'participatory_process_count': "SELECT COUNT(*) AS participatory_process_count FROM prod.stg_decidim_participatory_processes;",
        'participations_count': "SELECT COUNT(*) AS participations_count FROM prod.participations WHERE participation_type IS NOT NULL;",
    }

    # Perform data aggregation for all clients
    aggregated_data = aggregate_data_for_clients(client_databases, queries)

    # Display the aggregated data (optional)
    print(aggregated_data.head(5))

    # Insert the aggregated data into a new database and table
    target_database = "aggregated_client_data"
    target_table = "aggregated_data"

    insert_data_to_aggregated_db(aggregated_data, target_database, target_table)