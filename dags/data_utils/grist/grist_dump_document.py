import hashlib
from typing import Any, Dict, List, Tuple

import pandas as pd
from grist_api import GristDocAPI
from sqlalchemy.engine import Connection
from sqlalchemy.sql import text as sql_text
from sqlalchemy.sql.schema import sqlalchemy

from .grist_helper import list_grist_tables, sanitize_identifier
from .grist_types import GristTypes

# =========================
# === Internal helpers  ===
# =========================


def _safe_cname(name: str) -> str:
    """
    Produce a lowercase, Postgres-safe constraint name with max length 63.
    If the name exceeds 63 characters, keep a 54-char prefix + '_' + 8-hex hash.
    """
    base = name.lower()
    if len(base) <= 63:
        return base
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:8]
    return f"{base[:54]}_{h}"


def _fetch_grist_columns(api: GristDocAPI, table_id: str) -> List[Dict[str, Any]]:
    """
    Fetch Grist column metadata for a given table id.
    """
    columns_response = api.columns(table_id)
    if columns_response is None:
        raise ValueError(f"Failed fetching columns for table '{table_id}'")
    return columns_response.json()["columns"]


def _build_grist_types(
    columns: List[Dict[str, Any]], columns_to_explode: List[str]
) -> List[GristTypes]:
    """
    Build GristTypes instances with the 'explode' flag set per column id.
    """
    col_types: List[GristTypes] = []
    for c in columns:
        explode = c["id"] in columns_to_explode
        col_types.append(GristTypes(c["id"], c["fields"], explode=explode))
    return col_types


def _fetch_table_records_df(api: GristDocAPI, table_id: str) -> pd.DataFrame:
    """
    Fetch table records and materialize a pandas DataFrame that includes the physical 'id' column
    taken from the Grist record id (r['id']).
    """
    records = api.call(f"tables/{table_id}/records").json()["records"]
    df = pd.DataFrame([{**r["fields"], "id": r["id"]} for r in records])
    if not df.empty:
        # Ensure 'id' is numeric; DB type will be inferred or provided by dtype map later
        df["id"] = pd.to_numeric(df["id"], errors="raise", downcast="integer")
    return df


def _apply_column_transforms(
    df: pd.DataFrame, column_types: List[GristTypes]
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Apply per-column transformations via GristTypes.modify_df and collect explicit SQL dtypes.
    Additionally, normalize REF columns (0 -> NULL) and ensure pandas nullable dtype where needed.
    """
    dtype_map: Dict[str, Any] = {}
    for c in column_types:
        # Let GristTypes adjust dataframe values / shapes / casts as needed
        df = c.modify_df(df)

        # Forward explicit SQL type if provided
        if c.sql_type is not None:
            dtype_map[c.id] = c.sql_type

        # Treat as REF if it actually references another table (these will get FKs)
        if c.ref_table and c.id in df.columns:
            # Replace Grist's "no ref" marker with NULL
            df[c.id] = df[c.id].replace({0: pd.NA, "0": pd.NA})

            # Ensure pandas can carry NULLs (nullable Int64 if numeric-like)
            try:
                df[c.id] = pd.to_numeric(df[c.id], errors="ignore")
                if pd.api.types.is_integer_dtype(
                    df[c.id]
                ) or pd.api.types.is_float_dtype(df[c.id]):
                    df[c.id] = df[c.id].astype("Int64")
            except Exception:
                # Keep object dtype with NAs if conversion fails
                pass

    return df, dtype_map


def _table_regclass(connection: Connection, schema: str, table_name: str) -> Any:
    """
    Return regclass for schema.table_name, or None if it doesn't exist.
    """
    return connection.execute(
        sql_text("SELECT to_regclass(:qname)"),
        {"qname": f'{schema}."{table_name}"'},
    ).scalar()


def _prepare_main_table(
    connection: Connection, schema: str, table_name: str, if_exists: str
) -> str:
    """
    Decide the pandas.to_sql behavior for the main table and TRUNCATE when 'replace' is requested
    and the table already exists. Returns the effective if_exists ("append" or original).
    """
    main_reg = _table_regclass(connection, schema, table_name)
    effective = if_exists
    if if_exists == "replace" and main_reg is not None:
        connection.execute(
            sql_text(f"""
        TRUNCATE TABLE {schema}."{table_name}" RESTART IDENTITY CASCADE;
        """)
        )
        effective = "append"
    return effective


def _write_main_table(
    connection: Connection,
    schema: str,
    table_name: str,
    df: pd.DataFrame,
    dtype_map: Dict[str, Any],
    if_exists: str,
) -> None:
    """
    Write the main DataFrame to Postgres using pandas.to_sql with the provided dtype map.
    """
    kwargs: Dict[str, Any] = {
        "name": table_name,
        "con": connection,
        "schema": schema,
        "if_exists": if_exists,
        "index": False,
    }
    if dtype_map:
        kwargs["dtype"] = dtype_map
    df.to_sql(**kwargs)


def _pk_ddl(schema: str, table_name: str):
    """
    Compose an idempotent DO-block to create a PRIMARY KEY on (id) for the given table.
    """
    pk_name = _safe_cname(f"pk_{table_name}_id")
    return sql_text(f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
            WHERE nsp.nspname = '{schema}'
              AND rel.relname = '{table_name}'
              AND con.conname = '{pk_name}'
        ) THEN
            ALTER TABLE {schema}."{table_name}"
            ADD CONSTRAINT {pk_name} PRIMARY KEY ("id");
        END IF;
    END$$;
    """)


def _direct_fk_ddls(
    schema: str, main_table: str, column_types: List[GristTypes], prefix: str
) -> List[Any]:
    """
    For each REF column, compose an idempotent DO-block to create a FK from main_table(col) to ref_table(id).
    """
    ddls: List[Any] = []
    for c in column_types:
        if not c.ref_table:
            continue
        ref_base = sanitize_identifier(c.ref_table)
        ref_table = f"{prefix}_{ref_base}"
        fk_name = _safe_cname(f"fk_{main_table}_{c.id}")

        ddls.append(
            sql_text(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint con
                JOIN pg_class rel ON rel.oid = con.conrelid
                JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                WHERE nsp.nspname = '{schema}'
                  AND rel.relname = '{main_table}'
                  AND con.conname = '{fk_name}'
            ) THEN
                ALTER TABLE {schema}."{main_table}"
                ADD CONSTRAINT {fk_name}
                FOREIGN KEY ("{c.id}")
                REFERENCES {schema}."{ref_table}" ("id");
            END IF;
        END$$;
        """)
        )
    return ddls


def _build_exploded_df(df: pd.DataFrame, col_id: str) -> pd.DataFrame:
    """
    Given the main df and the column to explode, return a child df with:
        - 'source' = parent id
        - 'target_<col>' = exploded item from the refList / choiceList
    """
    target_col = f"target_{col_id}"
    if col_id in df.columns:
        tmp = df[["id", col_id]].copy()
        child = (
            tmp.explode(col_id)
            .rename(columns={"id": "source", col_id: target_col})
            .reset_index(drop=True)
        )
    else:
        child = pd.DataFrame(columns=["source", target_col])
    # Purge "no ref" markers (0/"0") before insert
    if target_col in child.columns:
        child[target_col] = child[target_col].replace({0: pd.NA, "0": pd.NA})
        child = child.dropna(subset=[target_col])
    return child


def _prepare_exploded_table(
    connection: Connection, schema: str, table_name: str, if_exists: str
) -> str:
    """
    Decide pandas.to_sql behavior for an exploded child table and TRUNCATE when 'replace'
    is requested and the table already exists. Returns effective if_exists.
    """
    reg = _table_regclass(connection, schema, table_name)
    effective = if_exists
    if if_exists == "replace" and reg is not None:
        connection.execute(
            sql_text(f"""
        TRUNCATE TABLE {schema}."{table_name}" RESTART IDENTITY;
        """)
        )
        effective = "append"
    return effective


def _write_exploded_table(
    connection: Connection,
    schema: str,
    table_name: str,
    df_exploded: pd.DataFrame,
    exploded_sql_type: Any | None,
    target_col: str,
    if_exists: str,
) -> None:
    """
    Write a child df (exploded) to Postgres using pandas.to_sql. Optionally pass dtype for target column.
    """
    dtype: Dict[str, Any] = {}
    if exploded_sql_type is not None:
        dtype[target_col] = exploded_sql_type
    kwargs: Dict[str, Any] = {
        "name": table_name,
        "con": connection,
        "schema": schema,
        "if_exists": if_exists,
        "index": False,
    }
    if dtype:
        kwargs["dtype"] = dtype
    df_exploded.to_sql(**kwargs)


def _exploded_fk_source(schema: str, exploded_table: str, main_table: str):
    """
    Compose idempotent DO-block for FK: exploded_table(source) -> main_table(id).
    """
    fk_src = _safe_cname(f"fk_{exploded_table}_source")
    return sql_text(f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
            WHERE nsp.nspname = '{schema}'
              AND rel.relname = '{exploded_table}'
              AND con.conname = '{fk_src}'
        ) THEN
            ALTER TABLE {schema}."{exploded_table}"
            ADD CONSTRAINT {fk_src}
            FOREIGN KEY ("source")
            REFERENCES {schema}."{main_table}" ("id");
        END IF;
    END$$;
    """)


def _exploded_fk_target(
    schema: str, exploded_table: str, target_col: str, ref_table: str
):
    """
    Compose idempotent DO-block for FK: exploded_table(target_<col>) -> ref_table(id).
    """
    fk_tgt = _safe_cname(
        f"fk_{exploded_table}_target_{target_col.replace('target_', '')}"
    )
    return sql_text(f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
            WHERE nsp.nspname = '{schema}'
              AND rel.relname = '{exploded_table}'
              AND con.conname = '{fk_tgt}'
        ) THEN
            ALTER TABLE {schema}."{exploded_table}"
            ADD CONSTRAINT {fk_tgt}
            FOREIGN KEY ("{target_col}")
            REFERENCES {schema}."{ref_table}" ("id");
        END IF;
    END$$;
    """)


# ==========================================
# === Public helper (single table + children)
# ==========================================


def _dump_grist_table_to_postgres(
    api: GristDocAPI,
    connection: Connection,
    grist_table_name: str,
    prefix: str,
    if_exists: str,
    schema: str,
    columns_to_explode: List[str] | None = None,
) -> Tuple[List[Any], List[Any]]:
    """
    Create / refresh one Grist table into Postgres (plus its exploded children), and
    RETURN the DDL statements to add PKs/FKs later in two phases.
    """
    if columns_to_explode is None:
        columns_to_explode = []

    pk_constraints: List[Any] = []
    fk_constraints: List[Any] = []

    # 1) Metadata and normalized names
    columns = _fetch_grist_columns(api, grist_table_name)
    base_table_name = sanitize_identifier(grist_table_name)
    main_table_name = f"{prefix}_{base_table_name}"

    # 2) Build column types (typing/refs/explode flags)
    column_types = _build_grist_types(columns, columns_to_explode)

    # 3) Fetch data frame with physical 'id'
    df = _fetch_table_records_df(api, grist_table_name)

    # 4) Apply transforms and collect dtype map
    df, dtype_map = _apply_column_transforms(df, column_types)

    # 5) Prepare and write main table (TRUNCATE+append when needed)
    main_if_exists = _prepare_main_table(connection, schema, main_table_name, if_exists)
    _write_main_table(
        connection, schema, main_table_name, df, dtype_map, main_if_exists
    )

    # 6) Stage PK DDL
    pk_constraints.append(_pk_ddl(schema, main_table_name))

    # 7) Stage direct FK DDLs
    fk_constraints.extend(
        _direct_fk_ddls(schema, main_table_name, column_types, prefix)
    )

    # 8) Explode refList columns and stage child FKs
    for c in column_types:
        if not c.explode:
            continue

        exploded_table_name = f"{prefix}_{base_table_name}_exploded_by_{c.id}"
        target_col = f"target_{c.id}"

        # Build exploded df
        df_exploded = _build_exploded_df(df, c.id)

        # Prepare and write exploded table
        exploded_if_exists = _prepare_exploded_table(
            connection, schema, exploded_table_name, if_exists
        )
        exploded_sql_type = getattr(c, "exploded_sql_type", None)
        _write_exploded_table(
            connection,
            schema,
            exploded_table_name,
            df_exploded,
            exploded_sql_type,
            target_col,
            exploded_if_exists,
        )

        # Stage child FKs: source->parent, optional target->referenced
        fk_constraints.append(
            _exploded_fk_source(schema, exploded_table_name, main_table_name)
        )
        if c.exploded_ref_table:
            ref_base = sanitize_identifier(c.exploded_ref_table)
            ref_table_name = f"{prefix}_{ref_base}"
            fk_constraints.append(
                _exploded_fk_target(
                    schema, exploded_table_name, target_col, ref_table_name
                )
            )

    print(f"Succesfully dump {grist_table_name}")
    return pk_constraints, fk_constraints


def drop_tables_with_prefix(connection, schema: str, prefix: str):
    """
    Drop all tables in a given schema whose names start with the given prefix.
    Uses PostgreSQL catalog to identify matching tables.
    """
    try:
        query = sql_text(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = :schema
            AND table_name LIKE :prefix
            """
        )

        result = connection.execute(query, {"schema": schema, "prefix": f"{prefix}%"})
        tables = [row[0] for row in result]

        if not tables:
            print(
                f"Aucune table à supprimer dans le schéma '{schema}' avec le préfixe '{prefix}'."
            )
            return

        for table in tables:
            fq_table_name = f'"{schema}"."{table}"'
            drop_query = sql_text(f"DROP TABLE IF EXISTS {fq_table_name} CASCADE")
            connection.execute(drop_query)
            print(f"Dropped table {fq_table_name}")
    except Exception as e:
        print(
            f"Erreur lors du drop des tables avec préfixe '{prefix}' dans le schéma '{schema}': {e}"
        )


# ==========================================
# === Dump ===
# ==========================================


def dump_document_to_postgres(
    api: GristDocAPI,
    engine: sqlalchemy.engine.Engine,
    prefix: str,
    tables=None,
    columns_to_explode: List[Tuple[str, str]] | None = None,
    schema: str = "grist",
    if_exists: str = "replace",
    include_metadata: bool = False,
    verify_constraints: bool = False,
) -> None:
    """
    Dump an entire Grist document to a postgres db.
    Args:
        api: the GristDocApi to use to get the document.
        engine: the sql engine to use to dump the databse
        database: the postgres namespace where the db will be created / updated
        prefix: string that will be appened to each table name
        tables: the list of table names to export. If not provided, all tables will be exported
        columns_to_explode: list of (table_name, column_name) that will be exploded.
                            Only a column of type reference list or choice list can be exploded.
                            When a column is exploded, a new auxiliary table will be created
                            to represent this one to many relationship.


    Technically, we use a two-pass orchestration with transactional commits:
      1) Create/refresh all physical tables (main + exploded) and COLLECT DDL (PKs, FKs).
      2) Execute ALL PK DDLs first (idempotent).
      3) Execute ALL FK DDLs after (idempotent).
    """
    # Normalize input
    if columns_to_explode is None:
        columns_to_explode = []

    # Map of table_id -> list[column_id] to explode
    explode_map: Dict[str, List[str]] = {}
    for t, c in columns_to_explode:
        explode_map.setdefault(t, []).append(c)

    with engine.begin() as connection:
        drop_tables_with_prefix(connection, schema=schema, prefix=prefix)

    all_pk_ddl: List[Any] = []
    all_fk_ddl: List[Any] = []

    if tables is None:
        grist_tables = list_grist_tables(api, include_metadata=include_metadata)
    else:
        grist_tables = tables

    with engine.begin() as connection:
        for table_id in grist_tables:
            cols_for_this_table = explode_map.get(table_id, [])
            pk_ddl, fk_ddl = _dump_grist_table_to_postgres(
                api=api,
                connection=connection,
                grist_table_name=table_id,
                prefix=prefix,
                if_exists=if_exists,
                schema=schema,
                columns_to_explode=cols_for_this_table,
            )
            all_pk_ddl.extend(pk_ddl)
            all_fk_ddl.extend(fk_ddl)

    with engine.begin() as connection:
        for ddl in all_pk_ddl:
            connection.execute(ddl)
        for ddl in all_fk_ddl:
            connection.execute(ddl)
