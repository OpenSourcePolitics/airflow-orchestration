from .postgres_helper import get_postgres_connection
from sqlalchemy import text


def drop_airbyte_metadata(connection):
    """
    Drop all tables from airbyte_internal schema and all tables in public/matomo schema
    that start with _airbyte.
    """
    # Drop all tables from airbyte_internal schema
    try:
        drop_airbyte_internal_query = text("""
            DO $$ 
            DECLARE 
                r RECORD; 
            BEGIN 
                FOR r IN (
                    SELECT tablename 
                    FROM pg_tables 
                    WHERE schemaname = 'airbyte_internal'
                ) 
                LOOP 
                    EXECUTE 
                        'DROP TABLE airbyte_internal.' || quote_ident(r.tablename) || ' CASCADE'; 
                END LOOP; 
            END $$;
        """)

        # Enabling autocommit is crucial when executing a DROP TABLE statement in Airflow.
        # By default, SQLAlchemy wraps commands in an implicit transaction, which can cause issues
        # if the DROP operation conflicts with active transactions or locks.
        # Setting autocommit=True ensures that each statement runs outside of a transaction,
        # preventing any potential conflicts and allowing the DROP TABLE command to execute successfully.

        connection.execution_options(autocommit=True).execute(drop_airbyte_internal_query)

        print("Dropped all tables from airbyte_internal schema.")

    except Exception as e:
        print(f"Failed to drop tables from airbyte_internal schema: {e}")

    # Drop all _airbyte tables from public and matomo schema
    try:
        drop_airbyte_tables_query = text("""
        DO $$ 
        DECLARE 
            r RECORD; 
        BEGIN 
            FOR r IN (
                SELECT tablename, schemaname 
                FROM pg_tables 
                WHERE tablename LIKE '_airbyte%' 
                  AND schemaname IN ('public', 'matomo')
            ) 
            LOOP 
                EXECUTE 
                    'DROP TABLE ' || quote_ident(r.schemaname) || '.' || quote_ident(r.tablename) || ' CASCADE'; 
            END LOOP; 
        END $$;
        """)

        connection.execution_options(autocommit=True).execute(drop_airbyte_tables_query)
        print("Dropped all _airbyte tables from public and matomo schema.")

    except Exception as e:
        print(f"Failed to drop _airbyte tables from public/matomo schema: {e}")

database_name = {
    "angers": "angers",
    "arcueil": "arcueil",
    "bagneux": "bagneux",
    "casa": "casa",
    "cachan": "cachan",
    "cdc": "cdc",
    "cea": "cea",
    "cese": "cese",
    "chambery": "chambery",
    "colombes": "colombes",
    "cultuur_connect": "cultuur_connect",
    "grand_nancy": "grand_nancy",
    "lyon": "lyon",
    "loire_atlantique": "loire-atlantique",
    "malakoff": "malakoff",
    "marseille": "marseille",
    "mel": "mel",
    "montpellier": "montpellier",
    "meyzieu": "meyzieu",
    "nanterre": "nanterre",
    "nets4dem": "nets4dem",
    "plaine_commune": "plaine_commune",
    "real_deal": "real_deal",
    "sytral": "sytral",
    "thionville": "thionville",
    "toulouse": "toulouse",
    "tours": "tours",
    "valbonne": "valbonne",
    "villeurbanne": "villeurbanne"
}

def airbyte_cleanup(database):
    connection = get_postgres_connection("main_db_cluster_name", database_name[database])
    drop_airbyte_metadata(connection)
    connection.close()