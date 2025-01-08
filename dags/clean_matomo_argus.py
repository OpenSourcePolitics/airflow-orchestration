from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import mysql.connector

def clean_data_for_date(execution_date, **kwargs):
    """
    Clean data for a specific day from Matomo tables and store cleaned idvisit and idaction_name for audit purposes.
    """
    date_to_clean = execution_date.strftime('%Y-%m-%d')

    # MySQL connection setup
    conn = mysql.connector.connect(
        host='*****',
        user='*****',
        password='*****',
        database='*****',
        ssl_disabled=True
    )

    cursor = conn.cursor()

    try:
        # Create table to store idvisit
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bordeaux_log_visit (
                idvisit BIGINT NOT NULL,
                clean_date DATE NOT NULL
            );
        """)

        # Insert idvisit for the given date
        cursor.execute(f"""
            INSERT INTO bordeaux_log_visit (idvisit, clean_date)
            SELECT idvisit, '{date_to_clean}'
            FROM matomo_log_visit
            WHERE DATE(visit_first_action_time) = '{date_to_clean}'
              AND referer_url LIKE '%bordeaux%'
              AND idsite = 23;
        """)

        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM bordeaux_log_visit WHERE clean_date = %s;", (date_to_clean,))
        count = cursor.fetchone()[0]
        print(f"{count} rows inserted into bordeaux_log_visit for date {date_to_clean}.")

        if count == 0:
            print(f"No data to clean for {date_to_clean}.")
            return

        # Create table to store idaction
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bordeaux_log_actions (
                idvisit BIGINT NOT NULL,
                idaction_value BIGINT NOT NULL,
                idaction_type VARCHAR(50) NOT NULL,
                clean_date DATE NOT NULL
            );
        """)

        # List of columns to extract from matomo_log_link_visit_action
        action_columns = [
            'idaction_url_ref', 'idaction_name_ref', 'idaction_name', 'idaction_url',
            'idaction_event_action', 'idaction_event_category', 'idaction_content_interaction',
            'idaction_content_name', 'idaction_content_piece', 'idaction_content_target',
            'idaction_product_cat', 'idaction_product_cat2', 'idaction_product_cat3', 'idaction_product_cat4',
            'idaction_product_cat5', 'idaction_product_name', 'idaction_product_sku'
        ]

        # Loop through each column and insert data into bordeaux_log_actions
        for column in action_columns:
            action_type = column
            cursor.execute(f"""
                INSERT INTO bordeaux_log_actions (idvisit, idaction_value, idaction_type, clean_date)
                SELECT DISTINCT idvisit, {column}, '{action_type}', '{date_to_clean}'
                FROM matomo_log_link_visit_action 
                WHERE idvisit IN (SELECT idvisit FROM bordeaux_log_visit WHERE clean_date = '{date_to_clean}')
                AND {column} IS NOT NULL;
            """)
        conn.commit()

        # Count inserted rows in bordeaux_log_actions
        cursor.execute(
            "SELECT COUNT(*) FROM bordeaux_log_actions WHERE clean_date = %s;",
            (date_to_clean,))
        log_actions_count = cursor.fetchone()[0]
        print(f"{log_actions_count} rows inserted into bordeaux_log_actions for date {date_to_clean}.")

        # Clean matomo_log_link_visit_action
        cursor.execute(f"""
            DELETE FROM matomo_log_link_visit_action
            WHERE idvisit IN (SELECT idvisit FROM bordeaux_log_visit WHERE clean_date = '{date_to_clean}');
        """)

        # Clean matomo_log_conversion
        cursor.execute(f"""
            DELETE FROM matomo_log_conversion
            WHERE idvisit IN (SELECT idvisit FROM bordeaux_log_visit WHERE clean_date = '{date_to_clean}');
        """)

        # Clean matomo_log_visit
        cursor.execute(f"""
            DELETE FROM matomo_log_visit
            WHERE idvisit IN (SELECT idvisit FROM bordeaux_log_visit WHERE clean_date = '{date_to_clean}');
        """)

        # Clean matomo_log_conversion_item
        cursor.execute(f"""
            DELETE FROM matomo_log_conversion_item
            WHERE idvisit IN (SELECT idvisit FROM bordeaux_log_visit WHERE clean_date = '{date_to_clean}');
        """)

        # Clean matomo_log_action based on idaction_value with date filter
        cursor.execute(f"""
            DELETE FROM matomo_log_action
            WHERE idaction IN (
                SELECT DISTINCT idaction_value
                FROM bordeaux_log_actions
                WHERE clean_date = '{date_to_clean}'
            );
        """)

        conn.commit()

        print(
            f"Data cleaned successfully for {date_to_clean}. The IDs are stored in bordeaux_log_visit and bordeaux_log_actions.")

    except Exception as e:
        print(f"Error cleaning data for {date_to_clean}: {e}")

    finally:
        cursor.close()
        conn.close()

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clean_matomo_data',
    default_args=default_args,
    description='DAG to clean Matomo data day by day',
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
)

# Define the PythonOperator
clean_data_task = PythonOperator(
    task_id='clean_data_for_date',
    python_callable=clean_data_for_date,
    provide_context=True,
    dag=dag,
)

clean_data_task
