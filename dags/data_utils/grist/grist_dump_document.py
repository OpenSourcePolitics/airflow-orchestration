from typing import Tuple, List
import pandas as pd
from grist_api import GristDocAPI
from sqlalchemy.engine import Connection
from .grist_types import GristTypes
from .grist_helper import list_grist_tables, sanitize_identifier
from ..postgres_helper import (
    get_postgres_connection,
)


def dump_grist_table_to_postgres(
    api: GristDocAPI,
    connection: Connection,
    grist_table_name,
    prefix,
    if_exists,
    schema,
    columns_to_explode=[],
):
    columns = api.columns(grist_table_name).json()["columns"]
    table_name = sanitize_identifier(grist_table_name)
    column_types = []
    for c in columns:
        explode = c["id"] in columns_to_explode
        column_types.append(GristTypes(c["id"], c["fields"], explode=explode))

    dtype_map = {}
    records = api.call(f"tables/{grist_table_name}/records").json()["records"]
    df = pd.DataFrame([r["fields"] for r in records])
    for c in column_types:
        df = c.modify_df(df)
        if c.sql_type is not None:
            dtype_map[c.id] = c.sql_type

    df.to_sql(
        f"{prefix}_{table_name}",
        connection,
        schema=schema,
        dtype=dtype_map,
        if_exists=if_exists,
        index_label="id",
    )

    """
    # This code is not used because exploding and unique foreign keys is not compatible.
    # For details, see here: https://github.com/OpenSourcePolitics/airflow-orchestration/pull/82

    with engine.begin() as conn:
        conn.execute(
            sqlalchemy.text(
                'ALTER TABLE "table" ADD CONSTRAINT table_id_key UNIQUE ("id");'
            )
        )

        # Add foreign key constraints after data is inserted
        for c in column_types:
            constraint = c.constraint("table")
            if constraint is not None:
                conn.execute(constraint)
        conn.commit()
    """


def dump_document_to_postgres(
    api: GristDocAPI,
    connection_name: str,
    database: str,
    prefix: str,
    columns_to_explode: List[Tuple[str, str]] = [],
    schema: str = "grist",
    if_exists="replace",
    include_metadata: bool = False,
):
    """
    columns_to_explode is a list of (table_name, column_name) that the user wants to explode.
    They must be either choiceList of refList columns.
    Note that you have to use the table and column names that grist uses.
    """
    engine = get_postgres_connection(connection_name, database)
    connection = engine.connect()
    try:
        grist_tables = list_grist_tables(api, include_metadata=include_metadata)

        for table_id in grist_tables:
            columns_for_this_table = [
                c for (t, c) in columns_to_explode if t == table_id
            ]
            dump_grist_table_to_postgres(
                api,
                connection,
                table_id,
                if_exists=if_exists,
                prefix=prefix,
                schema=schema,
                columns_to_explode=columns_for_this_table,
            )
    finally:
        connection.close()
