from typing import Tuple, List
import pandas as pd
from grist_api import GristDocAPI
from sqlalchemy.engine import Connection
from sqlalchemy import text as sql_text
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
    column_types: List[GristTypes] = []
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
        if c.explode:
            # create auxiliary table with a one to many relationship
            df = df[[c.id]].explode()
            exploded_table_name = f"{prefix}_{table_name}_exploded_by_{c.id}"
            df.to_sql(
                exploded_table_name,
                connection,
                schema=schema,
                if_exists=if_exists,
                index=False,
            )
            connection.execute(
                sql_text(
                    f'ALTER TABLE "{exploded_table_name}" ADD CONSTRAINT fk_source_{c.id} FOREIGN KEY ("source_{c.id}") REFERENCES "{table_name}" ("id");'
                )
            )
            if c.ref_table:
                connection.execute(
                    sql_text(
                        f'ALTER TABLE "{exploded_table_name}" ADD CONSTRAINT fk_target_{c.id} FOREIGN KEY ("target_{c.id}") REFERENCES "{c.ref_table}" ("id");'
                    )
                )

    df.to_sql(
        f"{prefix}_{table_name}",
        connection,
        schema=schema,
        dtype=dtype_map,
        if_exists=if_exists,
        index_label="id",
    )

    connection.execute(
        sql_text('ALTER TABLE "table" ADD CONSTRAINT table_id_key UNIQUE ("id");')
    )

    # Add foreign key constraints after data is inserted
    for c in column_types:
        constraint = c.constraint("table")
        if constraint is not None:
            connection.execute(constraint)
    connection.commit()


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
